use crate::{
    broadcast::{Amendment, CompressedBatch, DeliveryShard},
    broker::{
        batch::{Batch, BatchStatus},
        Broker,
    },
    crypto::{statements::BatchDelivery, Certificate},
    warn, BrokerSettings, Membership,
};
use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        Identity,
    },
    net::SessionConnector,
    sync::{board::Board, fuse::Fuse, promise::Promise},
};
use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver},
    task, time,
};

type MultiSignatureOutlet = MpscReceiver<(Identity, MultiSignature)>;

struct WitnessCollector<'a> {
    membership: &'a Membership,
    shards: Vec<(Identity, MultiSignature)>,
    outlet: MultiSignatureOutlet,
}

impl Broker {
    pub(in crate::broker::broker) async fn broadcast_batch(
        worker: Identity,
        sequence: u64,
        batch: &mut Batch,
        compressed_batch: CompressedBatch,
        membership: Arc<Membership>,
        connector: Arc<SessionConnector>,
        settings: BrokerSettings,
    ) -> (u64, Certificate) {
        // Preprocess arguments

        let compressed_batch = Arc::new(compressed_batch);

        // Shuffle servers

        let mut servers = membership.servers().values().collect::<Vec<_>>();
        servers.shuffle(&mut thread_rng());

        // Setup channels, `Board`s and and `Fuse`

        let (witness_shard_sender, witness_shard_receiver) =
            mpsc::channel::<(Identity, MultiSignature)>(membership.servers().len());

        let (delivery_shard_sender, mut delivery_shard_receiver) =
            mpsc::channel::<(Identity, DeliveryShard)>(membership.servers().len());

        let (witness_board, witness_poster) = Board::blank();

        let fuse = Fuse::new();

        // Spawn submissions

        let mut backup_verifiers = Vec::with_capacity(membership.plurality() - 1);
        let mut submit_tasks = Vec::with_capacity(membership.servers().len());

        for (index, server) in servers.into_iter().enumerate() {
            let verify_promise = if index < membership.plurality() {
                // Server witnessing role: Verifier
                Promise::solved(true)
            } else if index < membership.quorum() {
                // Server witnessing role: Backup verifier
                let (promise, solver) = Promise::pending();
                backup_verifiers.push(solver);
                promise
            } else {
                // Server witnessing role: Idle
                Promise::solved(false)
            };

            let settings = settings.clone();
            let connector = connector.clone();
            let server = server.clone();
            let compressed_batch = compressed_batch.clone();
            let witness_shard_sender = witness_shard_sender.clone();
            let delivery_shard_sender = delivery_shard_sender.clone();
            let root = batch.entries.root();

            let handle = fuse.spawn(Broker::submit_batch(
                worker,
                sequence,
                root,
                compressed_batch,
                server,
                connector,
                verify_promise,
                witness_shard_sender,
                witness_board.clone(),
                delivery_shard_sender,
                settings,
            ));

            submit_tasks.push(handle);
        }

        let mut witness_collector =
            WitnessCollector::new(membership.as_ref(), witness_shard_receiver);

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
        witness_poster.post(witness);

        let (batch_height, shards) =
            Self::collect_deliveries(batch, membership.as_ref(), &mut delivery_shard_receiver)
                .await;

        let certificate = Self::aggregate_deliveries(
            batch.entries.root(),
            batch_height,
            shards,
            membership.as_ref(),
            &mut delivery_shard_receiver,
        )
        .await;

        batch.status = BatchStatus::Delivered;

        task::spawn(async move {
            let _fuse = fuse;

            let submissions = join_all(submit_tasks.into_iter());
            match time::timeout(settings.totality_timeout, submissions).await {
                Ok(_) => (),
                Err(_) => warn!("Timeout! Could not finish submitting batch to all servers!"),
            }
        });

        (batch_height, certificate)
    }

    async fn collect_deliveries(
        batch: &mut Batch,
        membership: &Membership,
        receiver: &mut MpscReceiver<(Identity, DeliveryShard)>,
    ) -> (u64, Vec<(Identity, MultiSignature)>) {
        let mut shards = HashMap::new();

        loop {
            let (identity, shard) = receiver.recv().await.unwrap();

            let entry = shards
                .entry((shard.amendments.clone(), shard.height))
                .or_insert(Vec::new());

            entry.push((identity, shard.multisignature));

            if entry.len() >= membership.plurality() {
                let shards = entry.clone();

                for amendment in shard.amendments.iter() {
                    match amendment {
                        Amendment::Nudge { id, sequence } => {
                            let index = batch
                                .submissions
                                .binary_search_by(|probe| probe.entry.id.cmp(id))
                                .unwrap();

                            let mut entry = batch.entries.items()[index].clone().unwrap();
                            entry.sequence = *sequence;
                            batch.entries.set(index, Some(entry)).unwrap();
                        }
                        Amendment::Drop { id } => {
                            let index = batch
                                .submissions
                                .binary_search_by(|probe| probe.entry.id.cmp(id))
                                .unwrap();

                            batch.entries.set(index, None).unwrap();
                        }
                    }
                }

                let statement = BatchDelivery {
                    height: &shard.height,
                    root: &batch.entries.root(),
                };

                let shards = shards
                    .into_iter()
                    .flat_map(|(identity, signature)| {
                        let keycard = membership.servers().get(&identity).unwrap();
                        match shard.multisignature.verify([keycard], &statement) {
                            Ok(_) => Some((identity, signature)),
                            Err(_) => None,
                        }
                    })
                    .collect::<Vec<_>>();

                return (shard.height, shards);
            }
        }
    }

    async fn aggregate_deliveries(
        root: Hash,
        height: u64,
        mut shards: Vec<(Identity, MultiSignature)>,
        membership: &Membership,
        receiver: &mut MpscReceiver<(Identity, DeliveryShard)>,
    ) -> Certificate {
        let statement = BatchDelivery {
            height: &height,
            root: &root,
        };

        while shards.len() < membership.plurality() {
            if let Some((identity, shard)) = receiver.recv().await {
                let keycard = membership.servers().get(&identity).unwrap();

                if shard.multisignature.verify([keycard], &statement).is_ok() {
                    shards.push((identity, shard.multisignature));
                }
            }
        }

        Certificate::aggregate_plurality(&membership, shards)
    }
}

impl<'a> WitnessCollector<'a> {
    fn new(membership: &'a Membership, receiver: MpscReceiver<(Identity, MultiSignature)>) -> Self {
        WitnessCollector {
            membership,
            shards: Vec::new(),
            outlet: receiver,
        }
    }

    fn succeeded(&self) -> bool {
        self.shards.len() >= self.membership.plurality()
    }

    async fn progress(&mut self) {
        while !self.succeeded() {
            // A copy of `update_inlet` is held by `orchestrate`.
            // As a result, `update_outlet.recv()` cannot return `None`.
            match self.outlet.recv().await {
                Some(shard) => self.shards.push(shard),
                None => unreachable!(), // Double check that this is indeed unreachable
            }
        }
    }

    pub fn finalize(self) -> Certificate {
        Certificate::aggregate_plurality(&self.membership, self.shards.into_iter())
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
            broker.keycard().identity(),
            0,
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

        certificate
            .verify_plurality(&membership, &statement)
            .unwrap();

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
            broker.keycard().identity(),
            0,
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

        certificate
            .verify_plurality(&membership, &statement)
            .unwrap();

        time::sleep(Duration::from_millis(200)).await;
    }
}
