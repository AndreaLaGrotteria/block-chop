use crate::{
    broker::{LoadBatch, LoadBroker, LoadBrokerSettings},
    crypto::Certificate,
    warn, Membership,
};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::{seq::SliceRandom, thread_rng};
use std::{sync::Arc, time::Instant};
use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, Identity},
    net::PlexConnector,
    sync::{board::Board, fuse::Fuse, promise::Promise},
};
use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver},
    time,
};

type MultiSignatureOutlet = MpscReceiver<(Identity, MultiSignature)>;

struct WitnessCollector<'a> {
    membership: &'a Membership,
    witness_shards: Vec<(Identity, MultiSignature)>,
    witness_shard_outlet: MultiSignatureOutlet,
}

impl LoadBroker {
    pub(in crate::broker::load_broker) async fn broadcast_batch(
        broker_identity: Identity,
        worker_index: u16,
        sequence: u64,
        load_batch: LoadBatch,
        membership: Arc<Membership>,
        connector: Arc<PlexConnector>,
        settings: LoadBrokerSettings,
    ) {
        // Preprocess arguments

        let LoadBatch {
            root,
            raw_batch,
            affinities,
            mut lockstep,
            flow_index,
            batch_index,
        } = load_batch;

        let raw_batch = Arc::new(raw_batch);

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
            let verify = if index < membership.plurality() + settings.optimistic_margin {
                Promise::solved(true) // Verifier
            } else if index < membership.quorum() {
                let (promise, solver) = Promise::pending(); // Backup verifier
                backup_verify_solvers.push(solver);
                promise
            } else {
                Promise::solved(false) // Idler
            };

            let handle = fuse.spawn(LoadBroker::submit_batch(
                broker_identity,
                worker_index,
                sequence,
                root,
                raw_batch.clone(),
                server.clone(),
                connector.clone(),
                affinities.clone(),
                verify,
                witness_shard_inlet.clone(),
                witness_board.clone(),
                delivery_shard_inlet.clone(),
                settings.clone(),
                flow_index,
                batch_index,
            ));

            submit_tasks.push(handle);
        }

        // Collect and aggregate and post (f + 1) witness shards:
        //  - Collect witness shards from `witness_shard_outlet` until
        //    (f + 1) shards are collected or a timeout expires.
        //  - If the timeout expires, signal the missing amount of
        //    backup verifiers to request a witness shard, and try again.
        //  - If all backup verifiers have requested a witness shard
        //    wait until at least (f + 1) shards are collected (this
        //    is guaranteed to eventually succeed).
        //  - Aggregate the plurality of witness shards into a witness.

        let mut witness_collector =
            WitnessCollector::new(membership.as_ref(), witness_shard_outlet);

        let mut backup_verify_solvers = backup_verify_solvers.into_iter().peekable();

        while backup_verify_solvers.peek().is_some() {
            match time::timeout(settings.witnessing_timeout, witness_collector.progress()).await {
                Ok(_) => {
                    for solver in &mut backup_verify_solvers {
                        solver.solve(false);
                    }
                }
                Err(_) => {
                    warn!("Timeout: could not collect witness without backup verifiers.");

                    for solver in (&mut backup_verify_solvers).take(witness_collector.missing()) {
                        solver.solve(true);
                    }
                }
            }
        }

        witness_collector.progress().await;

        let witness = witness_collector.finalize();

        // Wait for `settings.dissemination_delay` to improve dissemination
        // to slower servers (this reduces the rate at which servers need to
        // use the `TotalityManager` in order to retrieve the batch)

        time::sleep(settings.dissemination_delay).await;

        // Wait on `lockstep` to ensure lockstepped submission (this reduces,
        // but does not remove, the rate at which servers observe duplicates)
        // Note: additionally waiting for `settings.lockstep_margin` reduces
        // the probability that two witnesses in independent `Plex`es will
        // overtake each other in-flight, thus causing causally-dependent
        // batches to be submitted to `Order` in the wrong order.

        lockstep.lock().await;
        time::sleep(settings.lockstep_margin).await;
        witness_poster.post((witness, Instant::now()));

        lockstep.free();

        // Wait until (f + 1) processes have replied with a delivery shard,
        // ensuring at least one correct process has delivered

        let mut counter = 0;

        while counter < membership.plurality() {
            let _ = delivery_shard_outlet.recv().await.unwrap();
            counter += 1;
        }

        // Wait until all `submit_tasks` are completed, or a timeout expires

        let server_count = membership.servers().len();
        let totality_condition = async move {
            let mut submissions = submit_tasks.into_iter().collect::<FuturesUnordered<_>>();
            let mut counter = 0;
            while counter < server_count - settings.garbage_collect_exclude {
                let _ = submissions.next().await;
                counter += 1;
            }
        };

        if time::timeout(settings.totality_timeout, totality_condition)
            .await
            .is_err()
        {
            warn!("Timeout: could not finish submitting batch to all servers.");
        }
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

    fn missing(&self) -> usize {
        self.membership.plurality() - self.witness_shards.len()
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
