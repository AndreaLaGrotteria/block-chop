use crate::{broadcast::DeliveryShard, server::Batch};
use std::sync::Arc;
use tokio::sync::watch::{self, Sender as WatchSender};

type DeliveryShardInlet = WatchSender<Option<(u64, DeliveryShard)>>;

pub(in crate::server) struct BrokerSlot {
    pub next_sequence: u64,
    pub expected_batch: Option<(Vec<u8>, Batch)>,
    pub last_delivery_shard: Arc<DeliveryShardInlet>,
}

impl Default for BrokerSlot {
    fn default() -> Self {
        let (sender, _) = watch::channel(None);

        Self {
            next_sequence: 0,
            expected_batch: None,
            last_delivery_shard: Arc::new(sender),
        }
    }
}
