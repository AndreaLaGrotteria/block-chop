use std::time::{Duration, Instant};

use crate::{
    broker::{Batch, Broker, Reduction},
    system::Directory,
};

use tokio::{sync::broadcast::Receiver as BroadcastReceiver, time};

type ReductionOutlet = BroadcastReceiver<Reduction>;

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        _directory: &Directory,
        batch: &mut Batch,
        mut reduction_outlet: ReductionOutlet,
    ) {
        let start = Instant::now();

        loop {
            if let Some(reduction) = tokio::select! {
                reduction = reduction_outlet.recv() => reduction.ok(), // If the channel is closed, the task will soon shutdown anyway, and unlikely lags are not a problem
                _ = time::sleep(Duration::from_millis(10)) => None, // TODO: Add settings
            } {
                if reduction.root != batch.entries.root() {
                    if let Ok(index) = batch
                        .submissions
                        .binary_search_by_key(&reduction.id, |submission| submission.entry.id)
                    {
                        batch.submissions.get_mut(index).unwrap().reduction =
                            Some(reduction.multisignature);
                    }
                }
            }

            // TODO: Add settings
            if start.elapsed() > Duration::from_secs(1) {
                break;
            }
        }
    }
}
