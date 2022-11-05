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
type DeliveryShardOutlet = MpscReceiver<(Identity, DeliveryShard)>;

struct WitnessCollector<'a> {
    membership: &'a Membership,
    witness_shards: Vec<(Identity, MultiSignature)>,
    witness_shard_outlet: MultiSignatureOutlet,
}

impl Broker {
    pub(in crate::broker::broker) async fn broadcast_batch(
        broker_identity: Identity,
        worker_index: u16,
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

        // Setup channels, `Board`s and `Fuse`

        let (witness_shard_inlet, witness_shard_outlet) = mpsc::channel(membership.servers().len());

        let (delivery_shard_inlet, mut delivery_shard_outlet) =
            mpsc::channel(membership.servers().len());

        let (witness_board, witness_poster) = Board::blank();

        let fuse = Fuse::new();

        // Spawn submissions:
        //  - (f + 1) verifiers get their `verify` argument immediately set to `true`.
        //  - (f) backup verifiers have their `verify` argument set later, depending
        //    on the responsiveness of the (f + 1) verifiers.
        //  - (f) idlers get their `verify` argument immediately set to `false`.

        let mut backup_verify_solvers = Vec::with_capacity(membership.plurality() - 1);
        let mut submit_tasks = Vec::with_capacity(membership.servers().len());

        for (index, server) in servers.into_iter().enumerate() {
            let verify = if index < membership.plurality() {
                Promise::solved(true) // Verifier
            } else if index < membership.quorum() {
                let (promise, solver) = Promise::pending(); // Backup verifier
                backup_verify_solvers.push(solver);
                promise
            } else {
                Promise::solved(false) // Idler
            };

            let root = batch.entries.root();

            let handle = fuse.spawn(Broker::submit_batch(
                broker_identity,
                worker_index,
                sequence,
                root,
                compressed_batch.clone(),
                server.clone(),
                connector.clone(),
                verify,
                witness_shard_inlet.clone(),
                witness_board.clone(),
                delivery_shard_inlet.clone(),
                settings.clone(),
            ));

            submit_tasks.push(handle);
        }

        // Collect and aggregate and post (f + 1) witness shards:
        //  - Collect witness shards from `witness_shard_outlet` until
        //    (f + 1) shards are collected or a timeout expires.
        //  - If the timeout expires, signal all backup verifiers to
        //    request a witness shard, then collect the missing
        //    witness shards from `witness_shard_outlet`.

        let mut witness_collector =
            WitnessCollector::new(membership.as_ref(), witness_shard_outlet);

        match time::timeout(settings.witnessing_timeout, witness_collector.progress()).await {
            Ok(_) => {
                backup_verify_solvers
                    .into_iter()
                    .for_each(|solver| solver.solve(false));
            }
            Err(_) => {
                backup_verify_solvers
                    .into_iter()
                    .for_each(|solver| solver.solve(true));

                witness_collector.progress().await
            }
        }

        let witness = witness_collector.finalize();
        witness_poster.post(witness);

        // Collect and aggregate (f + 1) `DeliveryShard`s:
        //  - Collect `DeliveryShard`s until the same set of `Amendment`s
        //    is received (f + 1) times: that is necessarily the correct set
        //    of amendments. Apply the correct set of `Amendment`s to `batch`.
        //  - Keep collecting `DeliveryShards` until (f + 1) `MultiSignature`s
        //    are collected for `batch`'s amended root.
        //
        //  Remark: because each delivery shard signs the root of `batch` after
        //  the corresponding `Amendment`s are applied, it is impossible to verify
        //  a `DeliverySnard`'s `MultiSignature` without applying its (possibly
        //  spurious) `Amendments` to `batch`. To avoid doing so (`batch`
        //  is large and would need to be cloned), `DeliveryShard`
        // `MultiSignature`s are verified only when the correct set of
        // `Amendment`s is determined and applied to `batch`. Because Byzantine
        //  processes could have spuriously signed the correctly amended root,
        //  additional `DeliveryShard`s might be required to assemble a delivery
        //  certificate for `batch`.

        let (batch_height, shards) =
            Self::collect_delivery_shards(batch, membership.as_ref(), &mut delivery_shard_outlet)
                .await;

        let certificate = Self::aggregate_delivery_shards(
            batch.entries.root(),
            batch_height,
            shards,
            membership.as_ref(),
            &mut delivery_shard_outlet,
        )
        .await;

        batch.status = BatchStatus::Delivered;

        // Move `fuse` to a long-lived task, submitting `batch`
        // to straggler servers until a timeout expires

        task::spawn(async move {
            let _fuse = fuse;

            let submissions = join_all(submit_tasks.into_iter());
            match time::timeout(settings.totality_timeout, submissions).await {
                Ok(_) => (),
                Err(_) => warn!("Timeout: could not finish submitting batch to all servers."),
            }
        });

        (batch_height, certificate)
    }

    async fn collect_delivery_shards(
        batch: &mut Batch,
        membership: &Membership,
        delivery_shard_outlet: &mut DeliveryShardOutlet,
    ) -> (u64, Vec<(Identity, MultiSignature)>) {
        let mut signature_shards = HashMap::new();

        loop {
            let (identity, delivery_shard) = delivery_shard_outlet.recv().await.unwrap();

            let entry = signature_shards
                .entry((delivery_shard.amendments.clone(), delivery_shard.height))
                .or_insert(Vec::new());

            entry.push((identity, delivery_shard.multisignature));

            if entry.len() >= membership.plurality() {
                // `delivery_shard`'s `Amendment`s and height have collected (f + 1)
                // (possibly incorrect) `MultiSignature`s: apply `delivery_sahrd.amendments`
                // to `batch` and return `delivery_shard.height` with the correct
                // `MultiSignature`s in `entry`.

                let signature_shards = entry.clone();

                // Apply `delivery_shard.amendments` to `batch`

                for amendment in delivery_shard.amendments.iter() {
                    match amendment {
                        Amendment::Nudge { id, sequence } => {
                            let index = batch
                                .submissions
                                .binary_search_by(|probe| probe.entry.id.cmp(id))
                                .unwrap();

                            // TODO: Streamline the following code when `Vector` supports in-place updates
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

                // Filter out invalid `signature_shards`

                let statement = BatchDelivery {
                    height: &delivery_shard.height,
                    root: &batch.entries.root(),
                };

                let signature_shards = signature_shards
                    .into_iter()
                    .flat_map(|(identity, signature)| {
                        let keycard = membership.servers().get(&identity).unwrap();

                        match delivery_shard.multisignature.verify([keycard], &statement) {
                            Ok(_) => Some((identity, signature)),
                            Err(_) => None,
                        }
                    })
                    .collect::<Vec<_>>();

                return (delivery_shard.height, signature_shards);
            }
        }
    }

    async fn aggregate_delivery_shards(
        root: Hash,
        height: u64,
        mut shards: Vec<(Identity, MultiSignature)>,
        membership: &Membership,
        delivery_shard_outlet: &mut DeliveryShardOutlet,
    ) -> Certificate {
        let statement = BatchDelivery {
            height: &height,
            root: &root,
        };

        while shards.len() < membership.plurality() {
            if let Some((identity, shard)) = delivery_shard_outlet.recv().await {
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
    fn new(membership: &'a Membership, witness_shard_outlet: MultiSignatureOutlet) -> Self {
        WitnessCollector {
            membership,
            witness_shards: Vec::new(),
            witness_shard_outlet,
        }
    }

    fn succeeded(&self) -> bool {
        self.witness_shards.len() >= self.membership.plurality()
    }

    async fn progress(&mut self) {
        while !self.succeeded() {
            // A copy of `delivery_shard_inlet` is held by `Broker::broadcast_batch`.
            // As a result, `delivery_shard_outlet.recv()` cannot return `None`.
            match self.witness_shard_outlet.recv().await {
                Some(shard) => self.witness_shards.push(shard),
                None => unreachable!(), // Double check that this is indeed unreachable
            }
        }
    }

    pub fn finalize(self) -> Certificate {
        Certificate::aggregate_plurality(&self.membership, self.witness_shards.into_iter())
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
        server::MerkleBatch,
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
        let entries = MerkleBatch::expanded_batch_entries(compressed_batch.clone());

        let fake_signature = clients_keychains[0]
            .sign(&BatchWitness {
                broker: &broker.keycard().identity(),
                worker: &0,
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
        let entries = MerkleBatch::expanded_batch_entries(compressed_batch.clone());

        let fake_signature = clients[0]
            .sign(&BatchWitness {
                broker: &broker.keycard().identity(),
                worker: &0,
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
