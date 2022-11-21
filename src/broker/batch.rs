use crate::{
    broadcast::{Entry, PACKING},
    broker::Submission,
};
use zebra::vector::Vector;

pub(in crate::broker) struct Batch {
    pub submissions: Vec<Submission>,
    pub raise: u64,
    pub entries: Vector<Option<Entry>, PACKING>,
}
