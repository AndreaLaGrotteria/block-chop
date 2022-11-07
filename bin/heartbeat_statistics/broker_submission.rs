use std::time::SystemTime;
use talk::crypto::{primitives::hash::Hash, Identity};

#[allow(dead_code)]
pub(crate) struct BrokerSubmission {
    pub root: Hash,
    pub server: Identity,
    pub submission_started: SystemTime,
    pub server_connected: Option<SystemTime>,
    pub batch_sent: Option<SystemTime>,
    pub witness_shard_requested: Option<SystemTime>,
    pub witness_shard_received: Option<SystemTime>,
    pub witness_shard_verified: Option<SystemTime>,
    pub witness_shard_waived: Option<SystemTime>,
    pub witness_shard_concluded: Option<SystemTime>,
    pub witness_acquired: Option<SystemTime>,
    pub witness_sent: Option<SystemTime>,
    pub delivery_shard_received: Option<SystemTime>,
    pub submission_completed: Option<SystemTime>,
}
