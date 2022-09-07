use crate::{
    broadcast::{Amendment, CompressedBatch, DeliveryShard},
    broker::{
        batch::{Batch, BatchStatus},
        Broker,
    },
    crypto::{statements::BatchDelivery, Certificate},
    warn, BrokerSettings, Membership,
};

use futures::{future::join_all, stream::FuturesUnordered, StreamExt};

use rand::{seq::SliceRandom, thread_rng};

use std::{collections::BTreeMap, sync::Arc};

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, Identity},
    net::SessionConnector,
    sync::fuse::Fuse,
};

use tokio::{
    sync::{
        oneshot::{self, Receiver as OneshotReceiver},
        watch::{self},
    },
    task,
    time::{self, timeout},
};

enum WitnessingRole {
    Verifier,
    Backup,
    Idle,
}

impl Broker {
    pub(in crate::broker::broker) async fn broadcast(
        batch: &mut Batch,
        compressed_batch: CompressedBatch,
        membership: Arc<Membership>,
        connector: Arc<SessionConnector>,
        settings: BrokerSettings,
    ) -> (u64, Certificate) {
        let witness_root = batch.entries.root();

        let mut servers = membership.servers().values().collect::<Vec<_>>();
        servers.shuffle(&mut thread_rng());

        let (witness_sender, witness_receiver) = watch::channel::<Option<Certificate>>(None);

        let compressed_batch = Arc::new(compressed_batch);

        let fuse = Fuse::new();

        let (verify_senders, receivers): (Vec<_>, Vec<_>) = servers
            .into_iter()
            .enumerate()
            .map(|(index, server)| {
                if index < membership.plurality() {
                    (server, WitnessingRole::Verifier)
                } else if index < membership.quorum() {
                    (server, WitnessingRole::Backup)
                } else {
                    (server, WitnessingRole::Idle)
                }
            })
            .map(|(server, role)| {
                let (verify_sender, verify_receiver) = watch::channel::<bool>(false);

                let (witness_shard_sender, witness_shard_receiver) =
                    oneshot::channel::<(Identity, MultiSignature)>();
                let (witness_shard_sender, witness_shard_receiver) = match role {
                    WitnessingRole::Verifier | WitnessingRole::Backup => {
                        (Some(witness_shard_sender), Some(witness_shard_receiver))
                    }
                    WitnessingRole::Idle => (None, None),
                };

                let (delivery_shard_sender, delivery_shard_receiver) =
                    oneshot::channel::<(Identity, DeliveryShard)>();

                let settings = settings.clone();
                let connector = connector.clone();
                let witness_receiver = witness_receiver.clone();
                let server = server.clone();
                let compressed_batch = compressed_batch.clone();

                let handle = fuse.spawn(async move {
                    Broker::submit(
                        &compressed_batch,
                        witness_root,
                        &server,
                        connector,
                        verify_receiver,
                        witness_shard_sender,
                        witness_receiver,
                        Some(delivery_shard_sender),
                        settings,
                    )
                    .await
                });

                (
                    (role, verify_sender),
                    (witness_shard_receiver, delivery_shard_receiver, handle),
                )
            })
            .unzip();

        let (receivers, handles): (Vec<_>, Vec<_>) =
            receivers.into_iter().map(|(a, b, c)| ((a, b), c)).unzip();

        let (witness_shard_receivers, delivery_shard_receivers): (Vec<_>, Vec<_>) =
            receivers.into_iter().unzip();

        let witness_shard_receivers = witness_shard_receivers
            .into_iter()
            .flat_map(|receiver| receiver)
            .collect::<Vec<_>>();

        let mut witness_collector =
            WitnessCollector::new(membership.clone(), witness_shard_receivers);

        verify_senders
            .iter()
            .for_each(|(role, verify_sender)| match role {
                WitnessingRole::Verifier => verify_sender.send(true).unwrap(),
                WitnessingRole::Idle => verify_sender.send(false).unwrap(),
                _ => (),
            });

        match time::timeout(settings.witnessing_timeout, witness_collector.progress()).await {
            Ok(_) => {
                verify_senders
                    .iter()
                    .for_each(|(role, verify_sender)| match role {
                        WitnessingRole::Backup => verify_sender.send(false).unwrap(),
                        _ => (),
                    });
            }
            Err(_) => {
                verify_senders
                    .iter()
                    .for_each(|(role, verify_sender)| match role {
                        WitnessingRole::Backup => verify_sender.send(true).unwrap(),
                        _ => (),
                    });

                witness_collector.progress().await
            }
        }

        let witness = witness_collector.finalize();
        let _ = witness_sender.send(Some(witness));

        let mut delivery_collector =
            DeliveryCollector::new(batch, membership.clone(), delivery_shard_receivers);

        delivery_collector.progress().await;

        let (batch_height, certificate) = delivery_collector.finalize();

        batch.status = BatchStatus::Delivered;

        task::spawn(async move {
            let _fuse = fuse;

            let submissions = join_all(handles.into_iter());
            match timeout(settings.totality_timeout, submissions).await {
                Ok(_) => (),
                Err(_) => warn!("Timeout! Could not finish submitting batch to all servers!"),
            }
        });

        (batch_height, certificate)
    }
}

struct WitnessCollector {
    membership: Arc<Membership>,
    shards: Vec<(Identity, MultiSignature)>,
    stream: FuturesUnordered<OneshotReceiver<(Identity, MultiSignature)>>,
}

impl WitnessCollector {
    fn new(
        membership: Arc<Membership>,
        receivers: Vec<OneshotReceiver<(Identity, MultiSignature)>>,
    ) -> Self {
        let stream = receivers.into_iter().collect::<FuturesUnordered<_>>();

        WitnessCollector {
            membership,
            shards: Vec::new(),
            stream,
        }
    }

    fn succeeded(&self) -> bool {
        self.shards.len() >= self.membership.plurality()
    }

    async fn progress(&mut self) {
        while !self.succeeded() {
            // A copy of `update_inlet` is held by `orchestrate`.
            // As a result, `update_outlet.recv()` cannot return `None`.
            match self.stream.next().await {
                Some(Ok(shard)) => self.shards.push(shard),
                Some(Err(_)) | None => unreachable!(), // Double check that this is indeed unreachable
            }
        }
    }

    pub fn finalize(self) -> Certificate {
        Certificate::aggregate_plurality(&self.membership, self.shards.into_iter())
    }
}

struct DeliveryCollector<'a> {
    batch: &'a mut Batch,
    batch_height: Option<u64>,
    membership: Arc<Membership>,
    new_shards: Vec<(Identity, DeliveryShard)>,
    good_shards: Vec<(Identity, DeliveryShard)>,
    bad_shards: Vec<(Identity, DeliveryShard)>,
    counts: BTreeMap<(Vec<Amendment>, u64), usize>,
    stream: FuturesUnordered<OneshotReceiver<(Identity, DeliveryShard)>>,
}

impl<'a> DeliveryCollector<'a> {
    fn new(
        batch: &'a mut Batch,
        membership: Arc<Membership>,
        receivers: Vec<OneshotReceiver<(Identity, DeliveryShard)>>,
    ) -> Self {
        let stream = receivers.into_iter().collect::<FuturesUnordered<_>>();

        DeliveryCollector {
            batch,
            batch_height: None,
            membership,
            new_shards: Vec::new(),
            good_shards: Vec::new(),
            bad_shards: Vec::new(),
            counts: BTreeMap::new(),
            stream,
        }
    }

    fn succeeded(&self) -> bool {
        self.good_shards.len() >= self.membership.quorum()
    }

    fn try_amend_batch(&mut self) {
        if self.batch_height.is_none() {
            if self.new_shards.len() >= self.membership.quorum() {
                let ((amendments, height), _) =
                    self.counts.iter().max_by_key(|&(_, count)| count).unwrap();

                for amendment in amendments.iter() {
                    match amendment {
                        Amendment::Nudge { id, sequence } => {
                            let index = self
                                .batch
                                .submissions
                                .binary_search_by(|probe| probe.entry.id.cmp(id))
                                .unwrap();
                            let mut entry = self.batch.entries.items()[index].clone().unwrap();
                            entry.sequence = *sequence;
                            self.batch.entries.set(index, Some(entry)).unwrap();
                        }
                        Amendment::Drop { id } => {
                            let index = self
                                .batch
                                .submissions
                                .binary_search_by(|probe| probe.entry.id.cmp(id))
                                .unwrap();
                            self.batch.entries.set(index, None).unwrap();
                        }
                    }
                }

                self.batch_height = Some(*height);
            }
        }
    }

    fn filter_new_shards(&mut self) {
        if let Some(height) = self.batch_height {
            while let Some((identity, shard)) = self.new_shards.pop() {
                let statement = BatchDelivery {
                    height: &height,
                    root: &self.batch.entries.root(),
                };

                let keycard = self.membership.servers().get(&identity).unwrap();

                match shard.multisignature.verify([keycard], &statement) {
                    Ok(_) => self.good_shards.push((identity, shard)),
                    Err(_) => self.bad_shards.push((identity, shard)),
                }
            }
        }
    }

    async fn progress(&mut self) {
        while !self.succeeded() {
            // A copy of `update_inlet` is held by `orchestrate`.
            // As a result, `update_outlet.recv()` cannot return `None`.
            match self.stream.next().await {
                Some(Ok((identity, shard))) => {
                    *self
                        .counts
                        .entry((shard.amendments.clone(), shard.height))
                        .or_insert(0) += 1;

                    self.new_shards.push((identity, shard));

                    self.try_amend_batch();
                    self.filter_new_shards();
                }
                Some(Err(_)) | None => unreachable!(), // Double check that this is indeed unreachable
            }
        }
    }

    pub fn finalize(self) -> (u64, Certificate) {
        (
            self.batch_height.unwrap(),
            Certificate::aggregate_plurality(
                &self.membership,
                self.good_shards
                    .into_iter()
                    .map(|(identity, shard)| (identity, shard.multisignature)),
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    use crate::{
        broadcast::Straggler,
        broker::submission::Submission,
        crypto::statements::BatchWitness,
        server::expanded_batch_entries,
        system::test::{fake_batch, generate_system},
    };

    use talk::{
        crypto::{primitives::hash::hash, KeyChain},
        net::{test::TestConnector, SessionConnector},
    };
    use varcram::VarCram;

    impl Clone for Straggler {
        fn clone(&self) -> Self {
            Self {
                id: self.id.clone(),
                sequence: self.sequence.clone(),
                signature: self.signature.clone(),
            }
        }
    }

    impl Clone for CompressedBatch {
        fn clone(&self) -> Self {
            let ids = VarCram::cram(self.ids.uncram().unwrap().clone().as_ref());
            Self {
                ids,
                messages: self.messages.clone(),
                raise: self.raise.clone(),
                multisignature: self.multisignature.clone(),
                stragglers: self.stragglers.clone(),
            }
        }
    }

    #[tokio::test]
    async fn broker_broadcast_0_faulty() {
        let (clients, _servers, membership, _, connector_map) = generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let session_connector = Arc::new(SessionConnector::new(connector));

        let batch_size = 1;
        let compressed_batch = fake_batch(&clients, batch_size);

        let entries = expanded_batch_entries(fake_batch(&clients, batch_size));
        let fake_signature = clients[0]
            .sign(&BatchWitness::new(hash(&0).unwrap()))
            .unwrap();
        let submissions = entries
            .items()
            .iter()
            .map(|item| {
                let address = "127.0.0.1:8000".parse().unwrap();
                Submission {
                    address,
                    entry: item.as_ref().unwrap().clone(),
                    signature: fake_signature.clone(),
                }
            })
            .collect::<Vec<_>>();
        let mut batch = Batch {
            status: BatchStatus::Submitting,
            submissions,
            raise: 0,
            entries,
        };

        let membership = Arc::new(membership);

        let (height, certificate) = Broker::broadcast(
            &mut batch,
            compressed_batch,
            membership.clone(),
            session_connector,
            BrokerSettings {
                totality_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .await;

        let statement = BatchDelivery {
            height: &height,
            root: &batch.entries.root(),
        };
        certificate.verify_quorum(&membership, &statement).unwrap();

        println!("Got height ({}) and certificate!!!", height);
        println!("Checking for hanging submissions...");

        time::sleep(Duration::from_millis(200)).await;

        println!("Done!");
    }

    #[tokio::test]
    async fn broker_broadcast_1_faulty_server() {
        let (clients, mut servers, membership, _, connector_map) = generate_system(1000, 4).await;

        let server = servers.pop().unwrap();
        drop(server);

        time::sleep(Duration::from_millis(100)).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let session_connector = Arc::new(SessionConnector::new(connector));

        let batch_size = 1;
        let compressed_batch = fake_batch(&clients, batch_size);

        let entries = expanded_batch_entries(fake_batch(&clients, batch_size));
        let fake_signature = clients[0]
            .sign(&BatchWitness::new(hash(&0).unwrap()))
            .unwrap();
        let submissions = entries
            .items()
            .iter()
            .map(|item| {
                let address = "127.0.0.1:8000".parse().unwrap();
                Submission {
                    address,
                    entry: item.as_ref().unwrap().clone(),
                    signature: fake_signature.clone(),
                }
            })
            .collect::<Vec<_>>();
        let mut batch = Batch {
            status: BatchStatus::Submitting,
            submissions,
            raise: 0,
            entries,
        };

        let membership = Arc::new(membership);

        let (height, certificate) = Broker::broadcast(
            &mut batch,
            compressed_batch,
            membership.clone(),
            session_connector,
            BrokerSettings {
                totality_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .await;

        let statement = BatchDelivery {
            height: &height,
            root: &batch.entries.root(),
        };
        certificate.verify_quorum(&membership, &statement).unwrap();

        println!("Got height ({}) and certificate!!!", height);
        println!("Checking for hanging submissions...");

        time::sleep(Duration::from_millis(200)).await;

        println!("Done!");
    }
}
