use crate::{server::Batch, system::Membership};
use std::{
    collections::{HashMap, VecDeque},
    sync::{atomic::AtomicU64, Arc, Mutex},
};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::SessionConnector,
    sync::fuse::Fuse,
};
use tokio::sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender};

type CallInlet = MpscSender<Call>;
type CallOutlet = MpscReceiver<Call>;

type BatchInlet = MpscSender<Batch>;
type BatchOutlet = MpscReceiver<Batch>;

type EntryInlet = MpscSender<(u64, Batch)>;
type EntryOutlet = MpscReceiver<(u64, Batch)>;

// TODO: Refactor constants into settings

const PIPELINE: usize = 8192;

pub(in crate::server) struct TotalityManager {
    run_call_inlet: CallInlet,
    pull_outlet: BatchOutlet,
    _fuse: Fuse,
}

enum Call {
    Hit(Vec<u8>, Batch),
    Miss(Hash),
}

struct DeliveryQueue {
    offset: u64,
    entries: VecDeque<Option<Batch>>,
}

struct TotalityQueue {
    offset: u64,
    entries: VecDeque<Option<Arc<Vec<u8>>>>,
}

impl TotalityManager {
    pub fn new(membership: Membership, _connector: SessionConnector) -> Self {
        let totality_queue = TotalityQueue {
            offset: 0,
            entries: VecDeque::new(),
        };

        let totality_queue = Arc::new(Mutex::new(totality_queue));

        let vector_clock = membership
            .servers()
            .keys()
            .copied()
            .map(|identity| (identity, AtomicU64::new(0)))
            .collect::<HashMap<_, _>>();

        let vector_clock = Arc::new(vector_clock);

        let (run_call_inlet, run_call_outlet) = mpsc::channel(PIPELINE);
        let (pull_inlet, pull_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        fuse.spawn(TotalityManager::run(
            totality_queue.clone(),
            vector_clock.clone(),
            run_call_outlet,
            pull_inlet,
        ));

        TotalityManager {
            run_call_inlet,
            pull_outlet,
            _fuse: fuse,
        }
    }

    pub async fn hit(&self, compressed_batch: Vec<u8>, batch: Batch) {
        let _ = self
            .run_call_inlet
            .send(Call::Hit(compressed_batch, batch))
            .await;
    }

    pub async fn miss(&self, root: Hash) {
        let _ = self.run_call_inlet.send(Call::Miss(root)).await;
    }

    pub async fn pull(&mut self) -> Batch {
        // The `Fuse` to `TotalityManager::run` is owned by
        // `self`, so `pull_inlet` cannot have been dropped
        self.pull_outlet.recv().await.unwrap()
    }

    async fn run(
        totality_queue: Arc<Mutex<TotalityQueue>>,
        vector_clock: Arc<HashMap<Identity, AtomicU64>>,
        mut run_call_outlet: CallOutlet,
        pull_inlet: BatchInlet,
    ) {
        let mut delivery_queue = DeliveryQueue {
            offset: 0,
            entries: VecDeque::new(),
        };

        let (run_entry_inlet, run_entry_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        loop {
            let call = if let Some(call) = run_call_outlet.recv().await {
                call
            } else {
                // `TotalityManager` has dropped, shutdown
                return;
            };

            match call {
                Call::Hit(compressed_batch, batch) => {
                    delivery_queue.entries.push_back(Some(batch));

                    totality_queue
                        .lock()
                        .unwrap()
                        .entries
                        .push_back(Some(Arc::new(compressed_batch)));
                }
                Call::Miss(root) => {
                    delivery_queue.entries.push_back(None);
                    totality_queue.lock().unwrap().entries.push_back(None);

                    let height =
                        delivery_queue.offset + ((delivery_queue.entries.len() - 1) as u64);

                    fuse.spawn(TotalityManager::retrieve(
                        height,
                        root,
                        run_entry_inlet.clone(),
                    ));
                }
            }

            while delivery_queue.entries.front().is_some()
                && delivery_queue.entries.front().unwrap().is_some()
            {
                let batch = delivery_queue.entries.pop_front().unwrap().unwrap();
                delivery_queue.offset += 1;

                let _ = pull_inlet.send(batch).await;
            }
        }
    }

    async fn retrieve(height: u64, root: Hash, run_entry_inlet: EntryInlet) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::test::random_unauthenticated_batch;
    use std::collections::HashMap;
    use talk::{crypto::KeyChain, net::test::TestConnector};

    #[tokio::test]
    async fn all_hits() {
        let membership = Membership::new([]);

        let connector =
            SessionConnector::new(TestConnector::new(KeyChain::random(), HashMap::new()));

        let mut totality_manager = TotalityManager::new(membership, connector);

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
