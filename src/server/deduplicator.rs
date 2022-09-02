use crate::{
    broadcast::{Amendment, Message},
    server::Batch,
};

use oh_snap::Snap;

use std::{
    iter,
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use talk::sync::fuse::Fuse;

use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
    task, time,
};

type BatchInlet = MpscSender<Batch>;
type BatchOutlet = MpscReceiver<Batch>;

type AmendedBatchInlet = MpscSender<(Batch, Vec<Amendment>)>;
type AmendedBatchOutlet = MpscReceiver<(Batch, Vec<Amendment>)>;

type BatchBurstInlet = MpscSender<Arc<Vec<Batch>>>;
type BatchBurstOutlet = MpscReceiver<Arc<Vec<Batch>>>;

type AmendmentsBurstInlet = MpscSender<Vec<Vec<Amendment>>>;
type AmendmentsBurstOutlet = MpscReceiver<Vec<Vec<Amendment>>>;

// TODO: Turn the following constants into settings

const TASKS: usize = 4;
const PIPELINE: usize = 1024;
const RUN_DURATION: Duration = Duration::from_secs(3600);

const BURST_SIZE: usize = 16;
const BURST_TIMEOUT: Duration = Duration::from_millis(100);
const BURST_INTERVAL: Duration = Duration::from_millis(10);

pub(in crate::server) struct Deduplicator {
    dispatch_inlet: BatchInlet,
    pop_outlet: AmendedBatchOutlet,
    _fuse: Fuse,
}

#[derive(Clone)]
struct Log {
    last_sequence: u64,
    last_message: Message,
}

impl Deduplicator {
    pub fn with_capacity(capacity: usize) -> Self {
        let logs = vec![None; capacity];

        let (dispatch_inlet, dispatch_outlet) = mpsc::channel(PIPELINE);
        let (pop_inlet, pop_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        fuse.spawn(Deduplicator::run(logs, dispatch_outlet, pop_inlet));

        Deduplicator {
            dispatch_inlet,
            pop_outlet,
            _fuse: fuse,
        }
    }

    async fn run(
        logs: Vec<Option<Log>>,
        mut dispatch_outlet: BatchOutlet,
        mut pop_inlet: AmendedBatchInlet,
    ) {
        let mut logs = Snap::new(logs);

        let fuse = Fuse::new();

        loop {
            (logs, dispatch_outlet, pop_inlet) = {
                // Partition `logs` indices in `TASKS` chunks

                let cuts = (0..TASKS)
                    .map(|task| task * (logs.len() / TASKS))
                    .chain([logs.len()])
                    .map(|cut| cut as u64)
                    .collect::<Vec<_>>();

                let mut ranges = cuts.windows(2).map(|window| window[0]..window[1]);

                // Partition `logs` in `TASKS` `Snap`s

                let mut snaps = Vec::new();

                for task in 0..(TASKS - 1) {
                    let cut = (task + 1) * (logs.len() / TASKS);

                    logs = {
                        let (snap, tail) = logs.snap(cut);
                        snaps.push(snap);
                        tail
                    };
                }

                snaps.push(logs);

                let mut snaps = snaps.into_iter();

                // Initialize channels and `Fuse`

                let (process_inlets, process_outlets): (Vec<_>, Vec<_>) =
                    iter::repeat_with(|| mpsc::channel(PIPELINE))
                        .take(TASKS + 1)
                        .unzip();

                let (join_batch_burst_inlet, join_batch_burst_outlet) = mpsc::channel(PIPELINE);

                let (join_amendments_burst_inlets, join_amendments_burst_outlets): (
                    Vec<_>,
                    Vec<_>,
                ) = iter::repeat_with(|| mpsc::channel(PIPELINE))
                    .take(TASKS + 1)
                    .unzip();

                let fuse = Fuse::new();

                // Start tasks

                let dispatch_task = fuse.spawn(Deduplicator::dispatch(
                    dispatch_outlet,
                    process_inlets,
                    join_batch_burst_inlet,
                ));

                let mut process_snap_tasks = Vec::new();
                let mut process_tail_task = None; // This will hold the handle to the `process_tail` task..

                for (index, (process_outlet, join_amendments_burst_inlet)) in process_outlets
                    .into_iter()
                    .zip(join_amendments_burst_inlets)
                    .enumerate()
                {
                    if index < TASKS {
                        let snap = snaps.next().unwrap();
                        let range = ranges.next().unwrap();

                        process_snap_tasks.push(task::spawn_blocking(move || {
                            Deduplicator::process_snap(
                                snap,
                                range,
                                process_outlet,
                                join_amendments_burst_inlet,
                            )
                        }));
                    } else {
                        let offset = *cuts.last().unwrap();

                        // .. which is always set here (although the compiler cannot tell)..
                        process_tail_task = Some(task::spawn_blocking(move || {
                            Deduplicator::process_tail(
                                offset,
                                process_outlet,
                                join_amendments_burst_inlet,
                            )
                        }));
                    }
                }

                // .. so it is safe to `unwrap()` here
                let process_tail_task = process_tail_task.unwrap();

                let join_task = fuse.spawn(Deduplicator::join(
                    join_batch_burst_outlet,
                    join_amendments_burst_outlets,
                    pop_inlet,
                ));

                // Wait for tasks to complete, retrieve channels and `Snap`s

                // The `join()` task owns the `Fuse` on which all tasks are `spawn()`ed,
                // hence all tasks can be joined safely

                let dispatch_outlet = dispatch_task.await.unwrap().unwrap();

                let mut snaps = Vec::new();

                for process_snap_task in process_snap_tasks {
                    snaps.push(process_snap_task.await.unwrap());
                }

                let mut tail = process_tail_task.await.unwrap();
                let pop_inlet = join_task.await.unwrap().unwrap();

                // Merge all `snaps`, unwrap, concatenate `tail`, `Snap` again

                let logs = snaps.into_iter().reduce(Snap::merge).unwrap();

                let mut logs = match logs.try_unwrap() {
                    Ok(logs) => logs,
                    Err(_) => unreachable!(),
                };

                logs.append(&mut tail);

                let logs = Snap::new(logs);

                (logs, dispatch_outlet, pop_inlet)
            };
        }
    }

    async fn dispatch(
        mut dispatch_outlet: BatchOutlet,
        process_inlets: Vec<BatchBurstInlet>,
        join_batch_burst_inlet: BatchBurstInlet,
    ) -> BatchOutlet {
        let task_start = Instant::now();

        while task_start.elapsed() < RUN_DURATION {
            let mut burst = Vec::with_capacity(BURST_SIZE);

            let burst_start = Instant::now();

            while burst.len() < BURST_SIZE && burst_start.elapsed() < BURST_INTERVAL {
                tokio::select! {
                    batch = dispatch_outlet.recv() => {
                        if let Some(batch) = batch {
                            burst.push(batch);
                        } else {
                            // `Deduplicator` has dropped. Idle waiting for task to be cancelled.
                            // (Note that `return`ing something meaningful is not possible)
                            loop {
                                time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    },
                    _ = time::sleep(BURST_INTERVAL) => {}
                }
            }

            if !burst.is_empty() {
                let burst = Arc::new(burst);

                let _ = join_batch_burst_inlet.send(burst.clone()).await;

                for process_inlet in process_inlets.iter() {
                    let _ = process_inlet.send(burst.clone()).await;
                }
            }
        }

        dispatch_outlet
    }

    fn process_snap(
        mut snap: Snap<Option<Log>>,
        range: Range<u64>,
        mut process_snap_outlet: BatchBurstOutlet,
        join_amendments_burst_inlet: AmendmentsBurstInlet,
    ) -> Snap<Option<Log>> {
        loop {
            let batch_burst = if let Some(burst) = process_snap_outlet.blocking_recv() {
                burst
            } else {
                break;
            };

            let amendments_burst = batch_burst
                .iter()
                .map(|batch| {
                    let entries = batch.entries.items();

                    let locate = |id| match entries
                        .binary_search_by_key(&id, |entry| entry.as_ref().unwrap().id)
                    {
                        Ok(index) => index,
                        Err(index) => index,
                    };

                    let chunk = &entries[locate(range.start)..locate(range.end)];

                    chunk
                        .iter()
                        .filter_map(|entry| {
                            let entry = entry.as_ref().unwrap();

                            match snap.get_mut((entry.id - range.start) as usize).unwrap() {
                                Some(log) => {
                                    if entry.message == log.last_message {
                                        if entry.sequence != log.last_sequence {
                                            Some(Amendment::Nudge {
                                                id: entry.id,
                                                sequence: log.last_sequence,
                                            })
                                        } else {
                                            None
                                        }
                                    } else {
                                        if entry.sequence > log.last_sequence {
                                            log.last_sequence = entry.sequence;
                                            log.last_message = entry.message;

                                            None
                                        } else {
                                            Some(Amendment::Drop { id: entry.id })
                                        }
                                    }
                                }
                                log => {
                                    *log = Some(Log {
                                        last_sequence: entry.sequence,
                                        last_message: entry.message,
                                    });

                                    None
                                }
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let _ = join_amendments_burst_inlet.blocking_send(amendments_burst);
        }

        snap
    }

    fn process_tail(
        offset: u64,
        process_tail_outlet: BatchBurstOutlet,
        join_amendments_burst_inlet: AmendmentsBurstInlet,
    ) -> Vec<Option<Log>> {
        Vec::new()
    }

    async fn join(
        join_batch_burst_outlet: BatchBurstOutlet,
        join_amendments_burst_outlets: Vec<AmendmentsBurstOutlet>,
        pop_inlet: AmendedBatchInlet,
    ) -> AmendedBatchInlet {
        pop_inlet
    }
}
