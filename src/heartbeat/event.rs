use talk::crypto::primitives::hash::Hash;

pub enum Event {
    BatchAnnounced {
        root: Hash,
    },
    BatchReceived {
        root: Hash,
    },
    BatchExpansionStarted {
        root: Hash,
        verify: bool,
    },
    BatchExpansionCompleted {
        root: Hash,
    },
    BatchWitnessed {
        root: Hash,
    },
    BatchOrdered {
        root: Hash,
    },
    BatchDelivered {
        root: Hash,
        entries: u32,
        duplicates: u32,
    },
    BatchServed {
        root: Hash,
    },
}
