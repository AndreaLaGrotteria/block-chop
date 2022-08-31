use crate::{
    broadcast::{Entry, PACKING},
    broker::Submission,
};

use zebra::vector::Vector;

pub(in crate::broker) enum BatchStatus {
    Reducing,
    Submitting,
    Delivered,
}

pub(in crate::broker) struct Batch {
    pub status: BatchStatus,
    pub submissions: Vec<Submission>,
    pub raise: u64,
    pub entries: Vector<Option<Entry>, PACKING>,
}
