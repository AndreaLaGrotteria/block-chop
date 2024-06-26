use crate::{
    broadcast::{Batch as BroadcastBatch, Straggler},
    broker::{Batch as BrokerBatch, Broker, BrokerSettings, Reduction},
    crypto::statements::Reduction as ReductionStatement,
    debug,
    heartbeat::{self, BrokerEvent},
    info,
    system::Directory,
};
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
        broker_batch: &mut BrokerBatch,
        mut reduction_outlet: ReductionOutlet,
        settings: &BrokerSettings,
    ) -> BroadcastBatch {
        // Spawn the stream reduction task

        let (command_inlet, command_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        let stream_reduction_task = fuse.spawn(Broker::stream_reduction(
            directory.clone(),
            broker_batch.entries.root(),
            command_outlet,
        ));

        // Listen for reductions until all reductions are collected or the timeout expires:
        //  - Organize reductions in bursts to send to `Broker::stream_reduction`.
        //  - Send only one reduction per id.
        //  - When all reductions are collected or the timeout expires, collect the
        //    aggregation and list of Byzantines from `Broker::stream_reduction`.

        info!(
            "Collecting reductions for root {:#?}..",
            broker_batch.entries.root(),
        );

        let reduction_deadline = Instant::now() + settings.reduction_timeout;

        let mut pending_reducers = broker_batch
            .submissions
            .iter()
            .map(|submission| submission.entry.id)
            .collect::<HashSet<_>>();

        let mut burst_buffer = Vec::with_capacity(settings.reduction_burst_size);

        while !pending_reducers.is_empty() && Instant::now() < reduction_deadline {
            if let Some(reduction) = tokio::select! {
                reduction = reduction_outlet.recv() => {
                    match reduction {
                        Ok(reduction) => Some(reduction),
                        Err(BroadcastRecvError::Lagged(_)) => {
                            info!(
                                "Broadcast channel lagged",
                            );

                            None
                        },
                        Err(BroadcastRecvError::Closed) => {
                            // `Broker` has dropped. Idle waiting for task to be cancelled.
                            // (Note that `return`ing something meaningful is not possible)
                            info!(
                                "Broadcast channel closed",
                            );
                            loop {
                                time::sleep(Duration::from_secs(1)).await;
                            }
                        },
                    }
                },
                _ = time::sleep(settings.reduction_interval) => None,
            } {
                // Filter out reductions for other batches
                if reduction.root != broker_batch.entries.root() {
                    continue;
                }

                // If this is the first reduction from this id, batch it in `burst_buffer`

                if pending_reducers.remove(&reduction.id) {
                    debug!(
                        "Received reduction for {:#?} from id {}.",
                        broker_batch.entries.root(),
                        reduction.id,
                    );

                    burst_buffer.push((reduction.id, reduction.multisignature));

                    // If `burst_buffer` is large enough, flush it to `Broker::stream_reduction`

                    if burst_buffer.len() >= settings.reduction_burst_size {
                        let mut burst = Vec::with_capacity(settings.reduction_burst_size); // Better performance than `mem:take`ing
                        mem::swap(&mut burst, &mut burst_buffer);

                        let _ = command_inlet.send(StreamReductionCommand::Aggregate(burst));
                    }
                }
            }
        }

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::ReductionReceptionEnded {
            root: broker_batch.entries.root(),
            timed_out: Instant::now() >= reduction_deadline,
        });

        // Flush reductions lingering in `burst_buffer` to `Broker::stream_reduction`,
        // join `Broker::stream_reduction` to obtain aggregate multisignature and
        // a list of (Byzantine) clients that reduced incorrectly.

        if !burst_buffer.is_empty() {
            let _ = command_inlet.send(StreamReductionCommand::Aggregate(burst_buffer));
        }

        let _ = command_inlet.send(StreamReductionCommand::Terminate);

        let (multisignature, byzantines) = stream_reduction_task.await.unwrap().unwrap();

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::AggregateComputed {
            root: broker_batch.entries.root(),
        });

        // Collect and sort straggler ids (both late and Byzantine)

        info!("Building broadcast batch..");

        let mut straggler_ids = pending_reducers.into_iter().collect::<Vec<_>>();
        straggler_ids.extend(byzantines);

        straggler_ids.par_sort_unstable();

        // Build `BroadcastBatch` fields and update `batch.entries` to account for stragglers

        let mut straggler_ids = straggler_ids.into_iter().peekable();

        let mut ids = Vec::with_capacity(broker_batch.submissions.len());
        let mut messages = Vec::with_capacity(broker_batch.submissions.len());
        let mut stragglers = Vec::with_capacity(straggler_ids.len());

        for (index, submission) in broker_batch.submissions.iter().enumerate() {
            ids.push(submission.entry.id);
            messages.push(submission.entry.message.clone());

            // Note that both `straggler_ids` and `batch.submissions[..].entry.id` are sorted
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

                broker_batch
                    .entries
                    .set(index, Some(submission.entry.clone()))
                    .unwrap();
            }
        }

        let ids = VarCram::cram(ids.as_slice());
        let raise = broker_batch.raise;

        // Assemble and return `BroadcastBatch`

        info!(
            "Built broadcast batch (root {:#?}, {} messages, {} stragglers).",
            broker_batch.entries.root(),
            messages.len(),
            stragglers.len(),
        );

        BroadcastBatch {
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
                    // Map ids to `MultiPublicKey`s

                    let entries = reductions.iter().map(|(id, multisignature)| {
                        let multi_public_key = directory.get_multi_public_key(*id).unwrap();
                        (multi_public_key, multisignature)
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
