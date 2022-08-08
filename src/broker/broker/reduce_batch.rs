use crate::{
    broadcast::{Entry, Straggler},
    broker::{Batch, Broker, Reduction},
    crypto::statements::Reduction as ReductionStatement,
    system::Directory,
};

use std::time::{Duration, Instant};

use talk::crypto::{
    primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature},
    KeyCard,
};

use tokio::{sync::broadcast::Receiver as BroadcastReceiver, time};

type ReductionOutlet = BroadcastReceiver<Reduction>;

struct Reducer<'a> {
    entry: &'a Entry,
    keycard: &'a KeyCard,
    signature: &'a Signature,
    multisignature: &'a MultiSignature,
}

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        directory: &Directory,
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

        let mut stragglers: Vec<Straggler> = Vec::new();
        let mut reducers: Vec<Reducer> = Vec::new();

        for submission in batch.submissions.iter() {
            if let Some(multisignature) = &submission.reduction {
                reducers.push(Reducer {
                    entry: &submission.entry,
                    keycard: directory.get(submission.entry.id).unwrap(),
                    signature: &submission.signature,
                    multisignature,
                });
            } else {
                stragglers.push(Straggler {
                    id: submission.entry.id,
                    sequence: submission.entry.sequence,
                    signature: submission.signature,
                });
            }
        }

        let (aggregation, mut byzantine) =
            Broker::aggregate_reductions(batch.entries.root(), reducers.as_slice());

        stragglers.append(&mut byzantine);

        // TODO: Update `entries`, build `CompressedBatch`
    }

    fn aggregate_reductions(
        root: Hash,
        reducers: &[Reducer],
    ) -> (Option<MultiSignature>, Vec<Straggler>) {
        let aggregation = MultiSignature::aggregate(
            reducers
                .iter()
                .map(|reducer| reducer.multisignature)
                .cloned(),
        )
        .ok();

        let statement = ReductionStatement { root: &root };

        let aggregation = aggregation.filter(|aggregation| {
            aggregation
                .verify(reducers.iter().map(|reducer| reducer.keycard), &statement)
                .is_ok()
        });

        if let Some(aggregation) = aggregation {
            (Some(aggregation), Vec::new())
        } else {
            if reducers.len() == 1 {
                let reducer = reducers.first().unwrap();

                let straggler = Straggler {
                    id: reducer.entry.id,
                    sequence: reducer.entry.sequence,
                    signature: *reducer.signature,
                };

                (None, vec![straggler])
            } else {
                let mid = reducers.len() / 2;
                let (left, right) = reducers.split_at(mid);

                let (left_aggregation, mut left_stragglers) =
                    Broker::aggregate_reductions(root, left);

                let (right_aggregation, mut right_stragglers) =
                    Broker::aggregate_reductions(root, right);

                let aggregation = match (left_aggregation, right_aggregation) {
                    (Some(left_aggregation), Some(right_aggregation)) => {
                        MultiSignature::aggregate([left_aggregation, right_aggregation]).ok()
                    }
                    (Some(left_aggregation), None) => Some(left_aggregation),
                    (None, Some(right_aggregation)) => Some(right_aggregation),
                    (None, None) => None,
                };

                left_stragglers.append(&mut right_stragglers);
                let stragglers = left_stragglers;

                (aggregation, stragglers)
            }
        }
    }
}
