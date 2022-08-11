use crate::{
    broadcast::{CompressedBatch, Straggler},
    broker::{Batch, BatchStatus, Broker, Reduction},
    crypto::statements::Reduction as ReductionStatement,
    system::Directory,
};

use std::{
    collections::HashSet,
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use rayon::slice::ParallelSliceMut;
use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        procedures as crypto_procedures,
    },
    sync::fuse::Fuse,
};

use tokio::{
    sync::{
        broadcast::{error::RecvError as BroadcastRecvError, Receiver as BroadcastReceiver},
        mpsc::{self, UnboundedReceiver as UnboundedMpscReceiver},
    },
    time,
};
use varcram::VarCram;

type ReductionOutlet = BroadcastReceiver<Reduction>;
type StreamReductionRequestOutlet = UnboundedMpscReceiver<StreamReductionRequest>;

enum StreamReductionRequest {
    Aggregate(Vec<(u64, MultiSignature)>),
    Terminate,
}

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        directory: Arc<Directory>,
        batch: &mut Batch,
        mut reduction_outlet: ReductionOutlet,
    ) -> CompressedBatch {
        let (stream_reduction_inlet, stream_reduction_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        let stream_reduction_task = fuse.spawn(Broker::stream_reduction(
            directory.clone(),
            batch.entries.root(),
            stream_reduction_outlet,
        ));

        let mut pending_reducers = batch
            .submissions
            .iter()
            .map(|submission| submission.entry.id)
            .collect::<HashSet<_>>();

        let mut stream_buffer = Vec::new();

        let start = Instant::now();

        loop {
            if let Some(reduction) = tokio::select! {
                reduction = reduction_outlet.recv() => {
                    match reduction {
                        Ok(reduction) => Some(reduction),
                        Err(BroadcastRecvError::Lagged(_)) => None,
                        Err(BroadcastRecvError::Closed) => {
                            // `Broker` has dropped. Idle waiting for task to be cancelled.
                            // (Note that `return`ing something meaningful is not possible)

                            loop {
                                time::sleep(Duration::from_secs(1)).await;
                            }
                        },
                    }
                },
                _ = time::sleep(Duration::from_millis(10)) => None, // TODO: Add settings
            } {
                if reduction.root != batch.entries.root() {
                    continue;
                }

                if pending_reducers.remove(&reduction.id) {
                    stream_buffer.push((reduction.id, reduction.multisignature));

                    if stream_buffer.len() >= 512 {
                        let _ = stream_reduction_inlet.send(StreamReductionRequest::Aggregate(
                            mem::take(&mut stream_buffer),
                        ));
                    }
                }
            };

            // TODO: Add settings
            if pending_reducers.is_empty() || start.elapsed() >= Duration::from_secs(1) {
                break;
            }
        }

        if !stream_buffer.is_empty() {
            let _ = stream_reduction_inlet.send(StreamReductionRequest::Aggregate(stream_buffer));
            let _ = stream_reduction_inlet.send(StreamReductionRequest::Terminate);
        }

        let (multisignature, byzantines) = stream_reduction_task.await.unwrap().unwrap();

        let mut straggler_ids = pending_reducers.into_iter().collect::<Vec<_>>();
        straggler_ids.extend(byzantines);

        straggler_ids.par_sort_unstable();

        let mut straggler_ids = straggler_ids.into_iter().peekable();

        let mut ids = Vec::with_capacity(batch.submissions.len());
        let mut messages = Vec::with_capacity(batch.submissions.len());
        let mut stragglers = Vec::with_capacity(straggler_ids.len());

        for (index, submission) in batch.submissions.iter().enumerate() {
            ids.push(submission.entry.id);
            messages.push(submission.entry.message.clone());

            if straggler_ids.peek() == Some(&submission.entry.id) {
                straggler_ids.next().unwrap();

                stragglers.push(Straggler {
                    id: submission.entry.id,
                    sequence: submission.entry.sequence,
                    signature: submission.signature,
                });

                batch
                    .entries
                    .set(index, Some(submission.entry.clone()))
                    .unwrap();
            }
        }

        let ids = VarCram::cram(ids.as_slice());
        let raise = batch.raise;

        batch.status = BatchStatus::Submitting;

        CompressedBatch {
            ids,
            messages,
            raise,
            multisignature,
            stragglers,
        }
    }

    async fn stream_reduction(
        directory: Arc<Directory>,
        root: Hash,
        mut stream_reduction_outlet: StreamReductionRequestOutlet,
    ) -> (Option<MultiSignature>, Vec<u64>) {
        let mut aggregation = None;
        let mut exceptions = Vec::new();

        loop {
            let request = if let Some(request) = stream_reduction_outlet.recv().await {
                request
            } else {
                // `Broker` has dropped, shutdown
                return (aggregation, exceptions);
            };

            match request {
                StreamReductionRequest::Aggregate(reductions) => {
                    let statement = ReductionStatement { root: &root };

                    let entries = reductions.iter().map(|(id, multisignature)| {
                        let keycard = directory.get(*id).unwrap();
                        (keycard, multisignature)
                    });

                    let (burst_aggregation, burst_exceptions) =
                        crypto_procedures::filter_aggregate(&statement, entries);

                    aggregation = match (aggregation, burst_aggregation) {
                        (Some(aggregation), Some(burst_aggregation)) => Some(
                            MultiSignature::aggregate([aggregation, burst_aggregation]).unwrap(),
                        ),
                        (aggregation, burst_aggregation) => aggregation.or(burst_aggregation),
                    };

                    let burst_exceptions = burst_exceptions
                        .into_iter()
                        .map(|exception| reductions.get(exception).unwrap().0);

                    exceptions.extend(burst_exceptions);
                }
                StreamReductionRequest::Terminate => {
                    return (aggregation, exceptions);
                }
            }
        }
    }
}
