use crate::{crypto::Certificate, server::WitnessCacheSettings};
use std::collections::{HashSet, VecDeque};
use talk::crypto::{
    primitives::hash::{hash, Hash},
    Identity,
};

pub(in crate::server) struct WitnessCache {
    set: HashSet<Hash>,
    queue: VecDeque<Hash>,
    settings: WitnessCacheSettings,
}

impl WitnessCache {
    pub fn new(settings: WitnessCacheSettings) -> Self {
        WitnessCache {
            set: HashSet::new(),
            queue: VecDeque::new(),
            settings,
        }
    }

    pub fn store(
        &mut self,
        broker: &Identity,
        worker: &u16,
        sequence: &u64,
        root: &Hash,
        witness: &Certificate,
    ) {
        let hash = hash(&(broker, worker, sequence, root, witness)).unwrap();

        self.set.insert(hash);
        self.queue.push_back(hash);

        if self.queue.len() > self.settings.capacity {
            let stale = self.queue.pop_front().unwrap();
            self.set.remove(&stale);
        }
    }

    pub fn contains(
        &self,
        broker: &Identity,
        worker: &u16,
        sequence: &u64,
        root: &Hash,
        witness: &Certificate,
    ) -> bool {
        let hash = hash(&(broker, worker, sequence, root, witness)).unwrap();
        self.set.contains(&hash)
    }
}
