use crate::{
    broadcast::{CompressedBatch, Straggler},
    broker::{Batch, BatchStatus, Broker, Reduction},
    crypto::statements::Reduction as ReductionStatement,
    system::Directory,
};

use log::{debug, info};

use rayon::slice::ParallelSliceMut;

use std::{
    collections::HashSet,
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

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
type StreamReductionCommandOutlet = UnboundedMpscReceiver<StreamReductionCommand>;

enum StreamReductionCommand {
    Aggregate(Vec<(u64, MultiSignature)>),
    Terminate,
}

impl Broker {
    pub(in crate::broker::broker) async fn reduce_batch(
        directory: Arc<Directory>,
        batch: &mut Batch,
        mut reduction_outlet: ReductionOutlet,
    ) -> CompressedBatch {
        // Spawn the stream reduction task

        let (command_inlet, command_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        let stream_reduction_task = fuse.spawn(Broker::stream_reduction(
            directory.clone(),
            batch.entries.root(),
            command_outlet,
        ));

        // Listen for reductions until all reductions are collected or the timeout expires:
        //  - Organize reductions in bursts to send to `Broker::stream_reduction`.
        //  - Send only one reduction per id.
        //  - When all reductions are collected or the timeout expires, collect the
        //    aggregation and list of Byzantines from `Broker::stream_reduction`.

        info!(
            "Collecting reductions for root {:#?}..",
            batch.entries.root(),
        );

        let reduction_deadline = Instant::now() + Duration::from_secs(1); // TODO: Add settings

        let mut pending_reducers = batch
            .submissions
            .iter()
            .map(|submission| submission.entry.id)
            .collect::<HashSet<_>>();

        let mut burst_buffer = Vec::with_capacity(512); // TODO: Add settings

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
                // Filter out reductions for other batches

                if reduction.root != batch.entries.root() {
                    continue;
                }

                // If this is the first reduction from this id, batch it in `burst_buffer`

                if pending_reducers.remove(&reduction.id) {
                    debug!(
                        "Received reduction for {:#?} from id {}.",
                        batch.entries.root(),
                        reduction.id,
                    );

                    burst_buffer.push((reduction.id, reduction.multisignature));

                    // If `burst_buffer` is large enough, flush it to `Broker::stream_reduction`

                    // TODO: Add settings
                    if burst_buffer.len() >= 512 {
                        // TODO: Add settings
                        let mut burst = Vec::with_capacity(512); // Better performance than `mem:take`ing
                        mem::swap(&mut burst, &mut burst_buffer);

                        let _ = command_inlet.send(StreamReductionCommand::Aggregate(burst));
                    }
                }
            };

            if pending_reducers.is_empty() || Instant::now() >= reduction_deadline {
                break;
            }
        }

        // Flush reductions lingering in `burst_buffer` to `Broker::stream_reduction`,
        // join `Broker::stream_reduction` to obtain aggregate multisignature and
        // a list of (Byzantine) clients that reduced incorrectly.

        if !burst_buffer.is_empty() {
            let _ = command_inlet.send(StreamReductionCommand::Aggregate(burst_buffer));
        }

        let _ = command_inlet.send(StreamReductionCommand::Terminate);

        let (multisignature, byzantines) = stream_reduction_task.await.unwrap().unwrap();

        // Collect and sort straggler ids (both late and Byzantine)

        info!("Building compressed batch..");

        let mut straggler_ids = pending_reducers.into_iter().collect::<Vec<_>>();
        straggler_ids.extend(byzantines);

        straggler_ids.par_sort_unstable();

        // Build `CompressedBatch` fields and update `batch.entries` to account for stragglers

        let mut straggler_ids = straggler_ids.into_iter().peekable();

        let mut ids = Vec::with_capacity(batch.submissions.len());
        let mut messages = Vec::with_capacity(batch.submissions.len());
        let mut stragglers = Vec::with_capacity(straggler_ids.len());

        for (index, submission) in batch.submissions.iter().enumerate() {
            ids.push(submission.entry.id);
            messages.push(submission.entry.message.clone());

            // Note that both `straggler_ids` and `batch.subimssions[..].entry.id` are sorted
            if straggler_ids.peek() == Some(&submission.entry.id) {
                straggler_ids.next().unwrap();

                // If `submission` is from a straggler, add appropriate `Straggler`
                // to `stragglers` and update `batch.entries` with the originally
                // submitted sequence number.

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

        // Assemble and return `CompressedBatch`

        info!(
            "Built compressed batch (root {:#?}, {} messages, {} stragglers: {:?}).",
            batch.entries.root(),
            messages.len(),
            stragglers.len(),
            stragglers
                .iter()
                .map(|straggler| straggler.id)
                .collect::<Vec<_>>()
        );

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
        mut command_outlet: StreamReductionCommandOutlet,
    ) -> (Option<MultiSignature>, Vec<u64>) {
        let statement = ReductionStatement { root: &root };

        let mut aggregation = None;
        let mut exceptions = Vec::new();

        loop {
            // Receive next `StreamReductionCommand`

            let command = if let Some(command) = command_outlet.recv().await {
                command
            } else {
                // `Broker` has dropped, shutdown
                return (aggregation, exceptions); // Note: the returned values will never be read
            };

            match command {
                StreamReductionCommand::Aggregate(reductions) => {
                    // Map ids to `KeyCard`s

                    let entries = reductions.iter().map(|(id, multisignature)| {
                        let keycard = directory.get(*id).unwrap();
                        (keycard, multisignature)
                    });

                    // Aggregate `entries`

                    let (burst_aggregation, burst_exceptions) =
                        crypto_procedures::filter_aggregate(&statement, entries);

                    // Aggregate `burst_aggregation` into the `aggregation` accumulator (note
                    // that both are `Option<MultiSignature>`, so unless both are `Some` the
                    // operation reduces to an `Option::or`).

                    aggregation = match (aggregation, burst_aggregation) {
                        (Some(aggregation), Some(burst_aggregation)) => Some(
                            MultiSignature::aggregate([aggregation, burst_aggregation]).unwrap(),
                        ),
                        (aggregation, burst_aggregation) => aggregation.or(burst_aggregation),
                    };

                    // Map positional exceptions back to id exceptions

                    let burst_exceptions = burst_exceptions
                        .into_iter()
                        .map(|exception| reductions.get(exception).unwrap().0);

                    // Add `burst_exceptions` to `exceptions`
                    // (Note: `exceptions` is unsorted but not duplicated.)

                    exceptions.extend(burst_exceptions);
                }
                StreamReductionCommand::Terminate => {
                    return (aggregation, exceptions);
                }
            }
        }
    }
}
