use crate::broker::Lockstep;
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::MultiplexId,
};

pub(in crate::broker) struct LoadBatch {
    pub root: Hash,
    pub raw_batch: Vec<u8>,
    pub affinities: Arc<HashMap<Identity, MultiplexId>>,
    pub lockstep: Lockstep,
    pub flow_index: usize,
    pub batch_index: usize,
}
