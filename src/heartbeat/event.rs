use talk::crypto::primitives::hash::Hash;

pub enum Event {
    // Local `Server` received `Batch`'s metadata (root, worker, ..)
    BatchAnnounced {
        root: Hash,
    },

    // Local `Server` received (not yet deserialized) `CompressedBatch`
    BatchReceived {
        root: Hash,
    },

    // Local `Server` started expanding `CompressedBatch` into `Batch` (`verify`
    // indicates whether `expand_verified` or `expand_unverified` is called)
    BatchExpansionStarted {
        root: Hash,
        verify: bool,
    },

    // Local `Server` finished expanding `CompressedBatch` into `Batch`
    BatchExpansionCompleted {
        root: Hash,
    },

    // Local `Server` produced a witness shard for `Batch`
    BatchWitnessed {
        root: Hash,
    },

    // Local `Server` submitted `Batch` ('s root, witness certificate, ..) to
    // underlying Total-Order Broadcast (note that this event is logged when
    // `Batch` is *submitted*, not when it is delivered from underlying TOB)
    BatchOrdered {
        root: Hash,
    },

    // Local `Server` delivered (zero or more of) `Batch`'s `Entry`ies to the
    // application layer; `Batch` contains `entries` `Entry`ies, `duplicates`
    //  of which were omitted (i.e., not delivered to the application layer).
    // Note that: the event logs when `Entry`ies are delivered to, not processed
    // by, the application layer; the number of `Entry`ies effectively delivered
    // to the application layer is `entries - duplicates`.
    BatchDelivered {
        root: Hash,
        entries: u32,
        duplicates: u32,
    },

    // Local `Server` produced a `DeliveryShard` for `Batch`, concluding `serve`
    BatchServed {
        root: Hash,
    },
}
