use crate::broker::Lockstep;
use talk::crypto::primitives::hash::Hash;

pub(in crate::broker) struct LoadBatch {
    pub root: Hash,
    pub raw_batch: Vec<u8>,
    pub lockstep: Lockstep,
}
