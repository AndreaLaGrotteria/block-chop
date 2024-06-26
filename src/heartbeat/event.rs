use serde::{Deserialize, Serialize};
use talk::crypto::{primitives::hash::Hash, Identity};

use crate::Message;

#[derive(Clone, Serialize, Deserialize)]
pub enum Event {
    Server(ServerEvent),
    Broker(BrokerEvent),
    Client(ClientEvent),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ServerEvent {
    // Local `Server` with identity `Identity` booted (this event should
    // be logged only once per execution, and assumes that no two instances
    // of `Server` will be run on the same `chop_chop` process)
    Booted {
        identity: Identity,
    },

    // Local `Server` received `Batch`'s metadata (root, worker, ..)
    BatchAnnounced {
        root: Hash,
    },

    // Local `Server` received (not yet deserialized) `Batch`
    BatchReceived {
        root: Hash,
    },

    // Local `Server` deserialized `Batch` containing `entries`
    // entries, `stragglers` of which were stragglers (failed to reduce)
    BatchDeserialized {
        root: Hash,
        entries: u32,
        stragglers: u32,
    },

    // Local `Server` started expanding `Batch` into `Batch` (`verify`
    // indicates whether `expand_verified` or `expand_unverified` is called)
    BatchExpansionStarted {
        root: Hash,
        verify: bool,
    },

    // Local `Server` finished expanding `Batch` into `Batch`
    BatchExpansionCompleted {
        root: Hash,
    },

    // Local `Server` produced a witness shard for `Batch`
    BatchWitnessed {
        root: Hash,
    },

    // Local `Server` submitted `Batch` ('s root, witness certificate, ..) to
    // underlying instance of Total-Order Broadcast
    BatchSubmitted {
        root: Hash,
    },

    // Local `Server` delivered `Batch` ('s root, witness certificate, ..) from
    // underlying instance of Total-Order Broadcast
    BatchOrdered {
        root: Hash,
    },

    // Local `Server` delivered (zero or more of) `Batch`'s `Entry`ies to the
    // application layer. Reminder: `Batch` was previously logged by
    // `Event::BatchDeserialized` to contain `entries` `Entry`ies. `duplicates`
    // out of `entries` entries were omitted (i.e., not delivered to the
    // application layer). Note that: the event logs when `Entry`ies are delivered
    // to, not processed by, the application layer; the number of `Entry`ies
    // effectively delivered to the application layer is `entries - duplicates`.
    BatchDelivered {
        root: Hash,
        duplicates: u32,
    },

    // Local `Server` produced a `DeliveryShard` for `Batch`, concluding `serve`
    BatchServed {
        root: Hash,
    },

    // Local `Server` processed a burst of `payments` operations
    PaymentsBurstProcessed {
        size: u32,
    },

    // Local `Server` processed a burst of `autions` operations
    AuctionsBurstProcessed {
        size: u32,
    },

    // Local `Server` processed a burst of `pixel_war` operations
    PixelWarBurstProcessed {
        size: u32,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum BrokerEvent {
    // Local `Broker` with identity `Identity` booted (this event should
    // be logged only once per execution, and assumes that no two instances
    // of `Broker` will be run on the same `chop_chop` process)
    Booted {
        identity: Identity,
    },

    // Local `Broker` inserted the first message of a new pool
    PoolCreation,

    // Local `Broker` flushed the pool
    PoolFlush {
        worker_index: u16,
        sequence: u64,
    },

    // Local `Broker` has assembled the batch (merkle tree) from the pool
    BatchBuilt {
        root: Hash,
    },

    // Local `Broker` is waiting for reductions
    ReductionReceptionStarted {
        worker_index: u16,
        sequence: u64,
        root: Hash,
    },

    // Local `Broker` has finished
    ReductionReceptionEnded {
        root: Hash,
        timed_out: bool,
    },

    // Local `Broker` is waiting for reductions
    AggregateComputed {
        root: Hash,
    },

    // Local `Broker` is waiting for reductions
    ReductionEnded {
        first_root: Hash,
        second_root: Hash,
    },

    // Local `Broker` begun executing `Broker::try_submit_batch`
    SubmissionStarted {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` established a connection to submission `Server`
    ServerConnected {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` sent raw batch to submisison `Server`
    BatchSent {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` requested a witness shard from submission `Server`
    WitnessShardRequested {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` received a witness shard from submission `Server`
    WitnessShardReceived {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` successfully verified the witness shard received
    // from submission `Server`
    WitnessShardVerified {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` waived requesting a witness shard from submission
    // `Server` (i.e., `Server` is an idler)
    WitnessShardWaived {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` concluded witnessing submission batch (this event
    // is triggered both if the witness shard was requested and obtained,
    // or waived)
    WitnessShardConcluded {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` aggregated a complete witness `Certificate` from a
    // plurality of submission `Server`s, and is ready to be sent the
    // witness to the submission `Server`
    WitnessAcquired {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` sent witness `Certificate` to submission `Server`
    WitnessSent {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` received `DeliveryShard` from submission `Server`
    DeliveryShardReceived {
        root: Hash,
        server: Identity,
    },

    // Local `Broker` completed its submission to submission `Server`
    SubmissionCompleted {
        root: Hash,
        server: Identity,
    },

    DisseminatingDeliveries {
        second_root: Hash,
        third_root: Hash,
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ClientEvent {
    // Local `Client` with identity `Identity` booted (this event should
    // be logged only once per execution, and assumes that no two instances
    // of `Client` will be run on the same `chop_chop` process)
    Booted {
        identity: Identity,
    },

    // Local `Client` is starting a new message broadcast
    StartingBroadcast {
        message: Message,
        sequence: u64,
    },

    // Local `Client` is sending a message to a broker
    SendingMessage {
        sequence: u64,
        broker_index: usize,
    },

    // Local `Client` has received the inclusion from the broker
    ReceivedInclusion {
        message: Message,
        root: Hash,
    },

    // Local `Client` has approved and starts signing the reductions
    SigningReduction {
        message: Message,
        root: Hash,
        raise: u64,
    },

    // Local `Client` has started sending the reduction to the broker
    SendingReduction {
        message: Message,
        root: Hash,
    },

    // Local `Client` has received a delivery notification from a broker
    ReceivedDelivery {
        sequence: u64,
    },

    // Local `Client` has verified the delivery from a broker and
    // has completed the broadcast
    BroadcastComplete {
        sequence: u64,
    },
}

impl Event {
    pub fn is_boot(&self) -> bool {
        match self {
            Event::Server(event) => event.is_boot(),
            Event::Broker(event) => event.is_boot(),
            Event::Client(event) => event.is_boot(),
        }
    }
}

impl ServerEvent {
    pub fn is_boot(&self) -> bool {
        if let ServerEvent::Booted { .. } = self {
            true
        } else {
            false
        }
    }
}

impl BrokerEvent {
    pub fn is_boot(&self) -> bool {
        if let BrokerEvent::Booted { .. } = self {
            true
        } else {
            false
        }
    }
}

impl ClientEvent {
    pub fn is_boot(&self) -> bool {
        if let ClientEvent::Booted { .. } = self {
            true
        } else {
            false
        }
    }
}

impl From<ServerEvent> for Event {
    fn from(event: ServerEvent) -> Self {
        Event::Server(event)
    }
}

impl From<BrokerEvent> for Event {
    fn from(event: BrokerEvent) -> Self {
        Event::Broker(event)
    }
}

impl From<ClientEvent> for Event {
    fn from(event: ClientEvent) -> Self {
        Event::Client(event)
    }
}
