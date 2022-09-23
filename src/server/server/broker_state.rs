use crate::server::Batch;
use std::sync::Arc;
use talk::crypto::primitives::multi::Signature as MultiSignature;
use tokio::sync::watch::{self, Sender as WatchSender};

pub(in crate::server::server) struct BrokerState {
    pub next_sequence: u64,
    pub expected_batch: Option<(Vec<u8>, Batch)>,
    pub last_delivery_shard: Arc<WatchSender<Option<(u64, MultiSignature)>>>,
}

impl Default for BrokerState {
    fn default() -> Self {
        let (sender, _) = watch::channel(None);
        Self {
            next_sequence: 0,
            expected_batch: None,
            last_delivery_shard: Arc::new(sender),
        }
    }
}
