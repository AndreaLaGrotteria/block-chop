use zebra::vector::Vector;

use crate::{broadcast::Entry, broker::Submission};

pub(in crate::broker) enum BatchStatus {
    Reducing,
    Witnessing,
    Delivered,
}

pub(in crate::broker) struct Batch {
    pub status: BatchStatus,
    pub submissions: Vec<Submission>,
    pub raise: u64,
    pub entries: Vector<Option<Entry>>,
}
