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
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, Identity},
    net::SessionConnector,
    sync::{fuse::Fuse, promise::Promise},
};
use tokio::{
    sync::{
        oneshot::{self, Receiver as OneshotReceiver},
        watch,
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
    pub(in crate::broker::broker) async fn broadcast_batch(
        batch: &mut Batch,
        compressed_batch: CompressedBatch,
        worker: Identity,
        sequence: u64,
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

        let mut backup_verifiers = Vec::with_capacity(membership.plurality() - 1);
        let mut witness_shard_receivers = Vec::with_capacity(membership.servers().len());
        let mut delivery_shard_receivers = Vec::with_capacity(membership.servers().len());
        let mut submit_handles = Vec::with_capacity(membership.servers().len());

        for (index, server) in servers.into_iter().enumerate() {
            let role = if index < membership.plurality() {
                WitnessingRole::Verifier
            } else if index < membership.quorum() {
                WitnessingRole::Backup
            } else {
                WitnessingRole::Idle
            };

            let promise = match role {
                WitnessingRole::Verifier => Promise::solved(true),
                WitnessingRole::Idle => Promise::solved(false),
                WitnessingRole::Backup => {
                    let (promise, solver) = Promise::pending();
                    backup_verifiers.push(solver);
                    promise
                }
            };

            let (witness_shard_sender, witness_shard_receiver) =
                oneshot::channel::<(Identity, MultiSignature)>();

            match role {
                WitnessingRole::Verifier | WitnessingRole::Backup => {
                    witness_shard_receivers.push(witness_shard_receiver);
                }
                _ => (),
            };

            let (delivery_shard_sender, delivery_shard_receiver) =
                oneshot::channel::<(Identity, DeliveryShard)>();

            delivery_shard_receivers.push(delivery_shard_receiver);

            let settings = settings.clone();
            let connector = connector.clone();
            let witness_receiver = witness_receiver.clone();
            let server = server.clone();
            let compressed_batch = compressed_batch.clone();

            let handle = fuse.spawn(async move {
                Broker::submit_batch(
                    worker,
                    sequence,
                    witness_root,
                    &compressed_batch,
                    &server,
                    connector,
                    promise,
                    Some(witness_shard_sender),
                    witness_receiver,
                    Some(delivery_shard_sender),
                    settings,
                )
                .await
            });

            submit_handles.push(handle);
        }

        let mut witness_collector =
            WitnessCollector::new(membership.clone(), witness_shard_receivers);

        match time::timeout(settings.witnessing_timeout, witness_collector.progress()).await {
            Ok(_) => {
                backup_verifiers
                    .into_iter()
                    .for_each(|solver| solver.solve(false));
            }
            Err(_) => {
                backup_verifiers
                    .into_iter()
                    .for_each(|solver| solver.solve(true));

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

            let submissions = join_all(submit_handles.into_iter());
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
    counts: HashMap<(Vec<Amendment>, u64), usize>,
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
            counts: HashMap::new(),
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
        broadcast::{test::null_batch, Straggler},
        broker::submission::Submission,
        crypto::statements::BatchWitness,
        server::expanded_batch_entries,
        system::test::generate_system,
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
        let (_servers, membership, _, connector_map, clients_keychains) =
            generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let session_connector = Arc::new(SessionConnector::new(connector));

        let (_, compressed_batch) = null_batch(&clients_keychains, 1);
        let entries = expanded_batch_entries(compressed_batch.clone());

        let fake_signature = clients_keychains[0]
            .sign(&BatchWitness {
                broker: &broker.keycard().identity(),
                sequence: &0,
                root: &hash(&0).unwrap(),
            })
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

        let (height, certificate) = Broker::broadcast_batch(
            &mut batch,
            compressed_batch,
            broker.keycard().identity(),
            0,
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

        time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn broker_broadcast_1_faulty_server() {
        let (mut servers, membership, _, connector_map, clients) = generate_system(1000, 4).await;

        let server = servers.pop().unwrap();
        drop(server);

        time::sleep(Duration::from_millis(100)).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let session_connector = Arc::new(SessionConnector::new(connector));

        let (_, compressed_batch) = null_batch(&clients, 1);
        let entries = expanded_batch_entries(compressed_batch.clone());

        let fake_signature = clients[0]
            .sign(&BatchWitness {
                broker: &broker.keycard().identity(),
                sequence: &0,
                root: &hash(&0).unwrap(),
            })
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

        let (height, certificate) = Broker::broadcast_batch(
            &mut batch,
            compressed_batch,
            broker.keycard().identity(),
            0,
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

        time::sleep(Duration::from_millis(200)).await;
    }
}
