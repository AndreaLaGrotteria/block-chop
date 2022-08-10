use crate::{
    broadcast::CompressedBatch,
    broker::{Batch, Broker, Reduction},
    system::Directory,
};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

type ReductionOutlet = BroadcastReceiver<Reduction>;

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        _directory: &Directory,
        _batch: &mut Batch,
        _reduction_outlet: ReductionOutlet,
    ) -> CompressedBatch {
        todo!()
    }
}
