use std::{collections::VecDeque, f32::consts::PI};

use crate::server::Batch;
use talk::{crypto::primitives::hash::Hash, net::SessionConnector, sync::fuse::Fuse};
use tokio::sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender};

type EntryInlet = MpscSender<Entry>;
type EntryOutlet = MpscReceiver<Entry>;

type BatchInlet = MpscSender<Batch>;
type BatchOutlet = MpscReceiver<Batch>;

// TODO: Refactor constants into settings

const PIPELINE: usize = 8192;

pub(in crate::server) struct TotalityManager {
    run_inlet: EntryInlet,
    pull_outlet: BatchOutlet,
    _fuse: Fuse,
}

enum Entry {
    Hit(Vec<u8>, Batch),
    Miss(Hash),
}

impl TotalityManager {
    pub fn new(_connector: SessionConnector) -> Self {
        let (run_inlet, run_outlet) = mpsc::channel(PIPELINE);
        let (pull_inlet, pull_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        fuse.spawn(TotalityManager::run(run_outlet, pull_inlet));

        TotalityManager {
            run_inlet,
            pull_outlet,
            _fuse: fuse,
        }
    }

    pub async fn hit(&self, compressed_batch: Vec<u8>, batch: Batch) {
        let _ = self
            .run_inlet
            .send(Entry::Hit(compressed_batch, batch))
            .await;
    }

    pub async fn miss(&self, root: Hash) {
        let _ = self.run_inlet.send(Entry::Miss(root)).await;
    }

    pub async fn pull(&mut self) -> Batch {
        // The `Fuse` to `TotalityManager::run` is owned by
        // `self`, so `pull_inlet` cannot have been dropped
        self.pull_outlet.recv().await.unwrap()
    }

    async fn run(mut run_outlet: EntryOutlet, pull_inlet: BatchInlet) {
        let mut delivery_offset = 0;
        let mut delivery_queue = VecDeque::new();

        let mut totality_offset = 0;
        let mut totality_queue = VecDeque::new();

        loop {
            let entry = if let Some(entry) = run_outlet.recv().await {
                entry
            } else {
                // `TotalityManager` has dropped, shutdown
                return;
            };

            match entry {
                Entry::Hit(compressed_batch, batch) => {
                    delivery_queue.push_back(Some(batch));
                    totality_queue.push_back(Some(compressed_batch));
                }
                Entry::Miss(root) => {
                    todo!()
                }
            }

            while delivery_queue.front().is_some() && delivery_queue.front().unwrap().is_some() {
                let batch = delivery_queue.pop_front().unwrap().unwrap();
                let _ = pull_inlet.send(batch).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::test::random_unauthenticated_batch;
    use std::collections::HashMap;
    use talk::{crypto::KeyChain, net::test::TestConnector};

    #[tokio::test]
    async fn all_hits() {
        let connector =
            SessionConnector::new(TestConnector::new(KeyChain::random(), HashMap::new()));

        let mut totality_manager = TotalityManager::new(connector);

        for _ in 0..128 {
            let compressed_batch = random_unauthenticated_batch(128, 32);
            let serialized_compressed_batch = bincode::serialize(&compressed_batch).unwrap();

            let batch = Batch::expand_unverified(compressed_batch).unwrap();
            let root = batch.root();

            totality_manager
                .hit(serialized_compressed_batch, batch)
                .await;

            let batch = totality_manager.pull().await;

            assert_eq!(batch.root(), root);
        }
    }
}
