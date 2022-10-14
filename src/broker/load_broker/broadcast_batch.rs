use crate::{
    broker::{LoadBroker, LoadBrokerSettings},
    crypto::Certificate,
    warn, Membership,
};
use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use std::sync::Arc;
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
        root: Hash,
        raw_batch: Vec<u8>,
        membership: Arc<Membership>,
        connector: Arc<SessionConnector>,
        settings: LoadBrokerSettings,
    ) {
        // Preprocess arguments

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
            let verify = if index < membership.plurality() {
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
                warn!("Timeout: could not collect witness without backup verifiers.");

                backup_verify_solvers
                    .into_iter()
                    .for_each(|solver| solver.solve(true));

                witness_collector.progress().await
            }
        }

        let witness = witness_collector.finalize();
        witness_poster.post(witness);

        // Wait until (f + 1) processes have replied with a delivery shard,
        // ensuring at least one correct process has delivered

        let mut counter = 0;

        while counter < membership.plurality() {
            let _ = delivery_shard_outlet.recv().await.unwrap();
            counter += 1;
        }

        // Wait until all `submit_tasks` are completed, or a timeout expires

        let submissions = join_all(submit_tasks.into_iter());

        if time::timeout(settings.totality_timeout, submissions)
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
