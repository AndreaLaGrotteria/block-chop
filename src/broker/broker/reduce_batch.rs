use crate::{
    broadcast::{CompressedBatch, Entry, Straggler},
    broker::{Batch, BatchStatus, Broker, Reduction},
    crypto::statements::Reduction as ReductionStatement,
    system::Directory,
};

use log::{debug, info};

use std::time::{Duration, Instant};

use talk::crypto::{
    primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature},
    KeyCard,
};

use tokio::{sync::broadcast::Receiver as BroadcastReceiver, time};

use varcram::VarCram;

type ReductionOutlet = BroadcastReceiver<Reduction>;

struct Reducer<'a> {
    entry: &'a Entry,
    keycard: &'a KeyCard,
    signature: &'a Signature,
    multisignature: &'a MultiSignature,
}

struct AggregationNode {
    multisignature: Option<MultiSignature>,
    children: Option<(Box<AggregationNode>, Box<AggregationNode>)>,
}

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        directory: &Directory,
        batch: &mut Batch,
        mut reduction_outlet: ReductionOutlet,
    ) -> CompressedBatch {
        info!("Reducing batch..");

        // Receive `Reduction`s until timeout

        let start = Instant::now();
        let mut reduced = 0;

        loop {
            if let Some(reduction) = tokio::select! {
                reduction = reduction_outlet.recv() => {
                    if reduction.is_ok() {
                        reduction.ok()
                    } else {
                        // `Broker` has dropped, which means that this task will soon
                        // be shut down by the `handle_requests`' `Fuse`. Just wait
                        // indefinitely for that to happen. Note: `return`ing is not
                        // feasible here: the function would have to return an `Option`,
                        // which would force a counter-intuitive `unwrap()` on the caller
                        // side, making code more bloated and less understandable.

                        loop {
                            time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = time::sleep(Duration::from_millis(10)) => None, // TODO: Add settings
            } {
                // Acquire `reduction` only if relevant to an existing `Submission` in `batch`

                if reduction.root == batch.entries.root() {
                    if let Ok(index) = batch
                        .submissions
                        .binary_search_by_key(&reduction.id, |submission| submission.entry.id)
                    {
                        debug!("Received reduction for id {}.", reduction.id);

                        let submission = batch.submissions.get_mut(index).unwrap();

                        if submission.reduction.is_none() {
                            submission.reduction = Some(reduction.multisignature);
                            reduced += 1;
                        }
                    }
                }
            }

            // TODO: Add settings
            if reduced == batch.submissions.len() || start.elapsed() > Duration::from_secs(1) {
                break;
            }
        }

        info!(
            "Reductions collected ({} / {}).",
            reduced,
            batch.submissions.len()
        );

        // Separate reducers from stragglers

        let mut stragglers: Vec<Straggler> = Vec::new();
        let mut reducers: Vec<Reducer> = Vec::new();

        for submission in batch.submissions.iter() {
            if let Some(multisignature) = &submission.reduction {
                reducers.push(Reducer {
                    entry: &submission.entry,
                    keycard: directory.get(submission.entry.id).unwrap(), // This will change when `Directory` is subject to churn
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

        // Aggregate correct reductions into `batch`-wide `MultiSignature`, add
        // to `stragglers` those Byzantine clients that reduced incorrectly.
        // (Accountability measures will be put in place).

        let aggregation_root = Broker::aggregation_tree(reducers.as_slice(), 32); // TODO: Add settings

        let (multisignature, mut byzantine) = if reducers.len() > 0 {
            Broker::aggregate_reductions(
                batch.entries.root(),
                reducers.as_slice(),
                Some(aggregation_root),
            )
        } else {
            (None, Vec::new())
        };

        info!(
            "Reductions aggregated ({} Byzantine clients detected).",
            byzantine.len()
        );

        stragglers.append(&mut byzantine);

        // Update `batch.status` and `batch.entries`

        batch.status = BatchStatus::Witnessing;

        stragglers.sort_unstable_by_key(|straggler| straggler.id);

        for straggler in stragglers.iter() {
            let index = batch
                .submissions
                .binary_search_by_key(&straggler.id, |submission| submission.entry.id)
                .unwrap();

            let submission = batch.submissions.get(index).unwrap();

            batch
                .entries
                .set(index, Some(submission.entry.clone()))
                .unwrap();
        }

        // Return `CompressedBatch`

        info!(
            "Compressing batch ({} total stragglers)..",
            stragglers.len()
        );

        let mut ids = Vec::with_capacity(batch.submissions.len());
        let mut messages = Vec::with_capacity(batch.submissions.len());

        for submission in batch.submissions.iter() {
            ids.push(submission.entry.id);
            messages.push(submission.entry.message.clone());
        }

        let ids = VarCram::cram(ids.as_slice());
        let raise = batch.raise;

        CompressedBatch {
            ids,
            messages,
            raise,
            multisignature,
            stragglers,
        }
    }

    fn aggregation_tree(reducers: &[Reducer], chunk: usize) -> AggregationNode {
        if reducers.len() > chunk {
            let mid = reducers.len() / 2;
            let (left, right) = reducers.split_at(mid);

            let (left_node, right_node) = rayon::join(
                || Broker::aggregation_tree(left, chunk),
                || Broker::aggregation_tree(right, chunk),
            );

            let multisignature = match (&left_node.multisignature, &right_node.multisignature) {
                (Some(left_multisignature), Some(right_multisignature)) => {
                    MultiSignature::aggregate([*left_multisignature, *right_multisignature]).ok()
                }
                _ => None,
            };

            AggregationNode {
                multisignature,
                children: Some((Box::new(left_node), Box::new(right_node))),
            }
        } else {
            AggregationNode {
                multisignature: MultiSignature::aggregate(
                    reducers
                        .iter()
                        .map(|reducer| reducer.multisignature)
                        .copied(),
                )
                .ok(),
                children: None,
            }
        }
    }

    fn aggregate_reductions(
        root: Hash,
        reducers: &[Reducer],
        aggregation_node: Option<AggregationNode>,
    ) -> (Option<MultiSignature>, Vec<Straggler>) {
        // Aggregate `MultiSignature`s

        let multisignatures = reducers
            .iter()
            .map(|reducer| reducer.multisignature)
            .copied();

        let (aggregation, aggregation_children) = match aggregation_node {
            Some(aggregation_node) => (aggregation_node.multisignature, aggregation_node.children),
            None => (MultiSignature::aggregate(multisignatures).ok(), None), // If `aggregate` fails, at least one `multisignature` is malformed
        };

        // Verify `aggregation` against `ReductionStatement`

        let statement = ReductionStatement { root: &root };
        let keycards = reducers.iter().map(|reducer| reducer.keycard);

        let aggregation =
            aggregation.filter(|aggregation| aggregation.verify(keycards, &statement).is_ok());

        // Return `aggregation` or recurd

        if aggregation.is_some() {
            // Successfully verified and aggregated: no Byzantine in `reducers`
            (aggregation, Vec::new())
        } else {
            // Failed to verify or aggregate: at least one Byzantine in `reducers`
            Broker::split_reductions(root, reducers, aggregation_children)
        }
    }

    fn split_reductions(
        root: Hash,
        reducers: &[Reducer],
        aggregation_children: Option<(Box<AggregationNode>, Box<AggregationNode>)>,
    ) -> (Option<MultiSignature>, Vec<Straggler>) {
        if reducers.len() == 1 {
            // Terminating case: the only `Reducer` is necessarily Byzantine

            let reducer = reducers.first().unwrap();

            let straggler = Straggler {
                id: reducer.entry.id,
                sequence: reducer.entry.sequence,
                signature: *reducer.signature,
            };

            (None, vec![straggler])
        } else {
            // Recurring case: find Byzantine(s) by splitting `reducers` in half

            let mid = reducers.len() / 2; // Guaranteed: `reducers.len() >= 2` ..
            let (left, right) = reducers.split_at(mid); // .. so both `left` and `right` are non-empty

            // Recur on `left` and `right`

            let (left_child, right_child) = match aggregation_children {
                Some((left_child, right_child)) => (Some(*left_child), Some(*right_child)),
                None => (None, None),
            };

            let (left_aggregation, mut left_stragglers) =
                Broker::aggregate_reductions(root, left, left_child);

            let (right_aggregation, mut right_stragglers) =
                Broker::aggregate_reductions(root, right, right_child);

            // Aggregate `left_aggregation` and `right_aggregation`

            let aggregation = match (left_aggregation, right_aggregation) {
                (Some(left_aggregation), Some(right_aggregation)) => {
                    // This is guaranteed to work
                    MultiSignature::aggregate([left_aggregation, right_aggregation]).ok()
                }
                (Some(left_aggregation), None) => Some(left_aggregation),
                (None, Some(right_aggregation)) => Some(right_aggregation),
                (None, None) => None,
            };

            // Merge `left_stragglers` and `right_stragglers`

            let mut stragglers = Vec::new();
            stragglers.append(&mut left_stragglers);
            stragglers.append(&mut right_stragglers);

            (aggregation, stragglers)
        }
    }
}
