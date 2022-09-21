use crate::{
    broadcast::{Entry, Message},
    server::{Batch, DeduplicatorSettings, Duplicate},
};
use futures::{stream::FuturesOrdered, StreamExt};
use oh_snap::Snap;
use std::{
    cmp, iter,
    ops::{Bound, Range, RangeBounds},
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

type DeduplicatedBatchInlet = MpscSender<(Batch, Vec<Duplicate>)>;
type DeduplicatedBatchOutlet = MpscReceiver<(Batch, Vec<Duplicate>)>;

type BatchBurstInlet = MpscSender<Arc<Vec<Batch>>>;
type BatchBurstOutlet = MpscReceiver<Arc<Vec<Batch>>>;

type DuplicatesBurstInlet = MpscSender<Vec<Vec<Duplicate>>>;
type DuplicatesBurstOutlet = MpscReceiver<Vec<Vec<Duplicate>>>;

pub(in crate::server) struct Deduplicator {
    dispatch_inlet: BatchInlet,
    pop_outlet: DeduplicatedBatchOutlet,
    _fuse: Fuse,
}

#[derive(Clone)]
struct Log {
    last_sequence: u64,
    last_message: Message,
}

impl Deduplicator {
    pub fn with_capacity(capacity: usize, settings: DeduplicatorSettings) -> Self {
        let capacity = cmp::max(capacity, settings.tasks);
        let logs = vec![None; capacity];

        let (dispatch_inlet, dispatch_outlet) = mpsc::channel(settings.pipeline);
        let (pop_inlet, pop_outlet) = mpsc::channel(settings.pipeline);

        let fuse = Fuse::new();

        fuse.spawn(Deduplicator::run(
            logs,
            dispatch_outlet,
            pop_inlet,
            settings,
        ));

        Deduplicator {
            dispatch_inlet,
            pop_outlet,
            _fuse: fuse,
        }
    }

    pub async fn push(&self, batch: Batch) {
        // `self` holds the `Fuse` to all tasks,
        // so this is guaranteed to succeed.
        let _ = self.dispatch_inlet.send(batch).await;
    }

    pub async fn pop(&mut self) -> (Batch, Vec<Duplicate>) {
        // `self` holds the `Fuse` to all tasks,
        // so this is guaranteed to succeed.
        self.pop_outlet.recv().await.unwrap()
    }

    async fn run(
        logs: Vec<Option<Log>>,
        mut dispatch_outlet: BatchOutlet,
        mut pop_inlet: DeduplicatedBatchInlet,
        settings: DeduplicatorSettings,
    ) {
        let mut logs = Snap::new(logs);

        let fuse = Fuse::new();

        loop {
            (logs, dispatch_outlet, pop_inlet) = {
                // Partition `logs` in `settings.tasks` chunks

                let capacity = logs.len();

                // Each chunk has `ceil(capacity / settings.tasks)` elements
                // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
                let chunk_size = (capacity + settings.tasks - 1) / settings.tasks;

                let mut snaps = logs.chunks(chunk_size).into_iter();

                // Initialize channels

                let (process_inlets, process_outlets): (Vec<_>, Vec<_>) =
                    iter::repeat_with(|| mpsc::channel(settings.pipeline))
                        .take(settings.tasks + 1) // `settings.tasks` channels for `process_snap`, one channel for `process_tail`
                        .unzip();

                let (join_batch_burst_inlet, join_batch_burst_outlet) =
                    mpsc::channel(settings.pipeline);

                let (join_duplicates_burst_inlets, join_duplicates_burst_outlets): (
                    Vec<_>,
                    Vec<_>,
                ) = iter::repeat_with(|| mpsc::channel(settings.pipeline))
                    .take(settings.tasks + 1) // `settings.tasks` channels for `process_snap`, one channel for `process_tail`
                    .unzip();

                // Start tasks

                let dispatch_task = fuse.spawn(Deduplicator::dispatch(
                    dispatch_outlet,
                    join_batch_burst_inlet,
                    process_inlets,
                    settings.clone(),
                ));

                let mut process_snap_tasks = FuturesOrdered::new();
                let mut process_tail_task = None; // This will hold the handle to the `process_tail` task..

                for (index, (process_outlet, join_duplicates_burst_inlet)) in process_outlets
                    .into_iter()
                    .zip(join_duplicates_burst_inlets)
                    .enumerate()
                {
                    if index < settings.tasks {
                        let snap = snaps.next().unwrap();

                        process_snap_tasks.push_back(task::spawn_blocking(move || {
                            Deduplicator::process_snap(
                                snap,
                                process_outlet,
                                join_duplicates_burst_inlet,
                            )
                        }));
                    } else {
                        // .. which is always set here (although the compiler cannot tell)..
                        process_tail_task = Some(task::spawn_blocking(move || {
                            Deduplicator::process_tail(
                                capacity as u64,
                                process_outlet,
                                join_duplicates_burst_inlet,
                            )
                        }));
                    }
                }

                // .. so it is safe to `unwrap()` here
                let process_tail_task = process_tail_task.unwrap();

                let join_task = fuse.spawn(Deduplicator::join(
                    join_batch_burst_outlet,
                    join_duplicates_burst_outlets,
                    pop_inlet,
                    settings.clone(),
                ));

                // Wait for tasks to complete, retrieve channels and `Snap`s

                // The `run` task owns the `Fuse` on which all tasks are `spawn()`ed,
                // hence all tasks can be joined safely

                let dispatch_outlet = dispatch_task.await.unwrap().unwrap();

                let snaps = process_snap_tasks
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
                    .await;

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
        join_batch_burst_inlet: BatchBurstInlet,
        process_inlets: Vec<BatchBurstInlet>,
        settings: DeduplicatorSettings,
    ) -> BatchOutlet {
        let task_start = Instant::now();

        // Run task for `settings.run_duration`

        while task_start.elapsed() < settings.run_duration {
            // Collect burst of `Batch`es

            let mut burst = Vec::with_capacity(settings.burst_size);

            let burst_start = Instant::now();

            while burst.len() < settings.burst_size
                && burst_start.elapsed() < settings.burst_timeout
            {
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
                    _ = time::sleep(settings.burst_interval) => {}
                }
            }

            // Send `burst` to `join` and `process_*` tasks

            if !burst.is_empty() {
                let burst = Arc::new(burst);

                for process_inlet in process_inlets.iter() {
                    let _ = process_inlet.send(burst.clone()).await;
                }

                let _ = join_batch_burst_inlet.send(burst).await;
            }
        }

        dispatch_outlet
    }

    fn process_snap(
        mut snap: Snap<Option<Log>>,
        mut process_snap_outlet: BatchBurstOutlet,
        join_duplicates_burst_inlet: DuplicatesBurstInlet,
    ) -> Snap<Option<Log>> {
        loop {
            let batch_burst = if let Some(burst) = process_snap_outlet.blocking_recv() {
                burst
            } else {
                // Either the current iteration of `run` concluded, or
                // `Deduplicator` has dropped: shutdown in both cases.
                break;
            };

            let duplicates_burst = batch_burst
                .iter()
                .map(|batch| {
                    let range: &Range<_> = snap.range(); // Enforce `Range` type to prevent silent changes in `Snap`'s interface

                    let range = Range {
                        start: range.start as u64,
                        end: range.end as u64,
                    };

                    let crop = Deduplicator::crop(batch.entries.items(), range.clone());

                    crop.iter()
                        .filter_map(|entry| {
                            let entry = entry.as_ref().unwrap();
                            let log = snap.get_mut((entry.id - range.start) as usize).unwrap();

                            Deduplicator::deduplicate(log, entry)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let _ = join_duplicates_burst_inlet.blocking_send(duplicates_burst);
        }

        snap
    }

    fn process_tail(
        offset: u64,
        mut process_tail_outlet: BatchBurstOutlet,
        join_duplicates_burst_inlet: DuplicatesBurstInlet,
    ) -> Vec<Option<Log>> {
        let mut tail = Vec::new();

        loop {
            let batch_burst = if let Some(burst) = process_tail_outlet.blocking_recv() {
                burst
            } else {
                // Either the current iteration of `run` concluded, or
                // `Deduplicator` has dropped: shutdown in both cases.
                break;
            };

            let duplicates_burst = batch_burst
                .iter()
                .map(|batch| {
                    let top_id = batch.entries.items().last().unwrap().as_ref().unwrap().id;

                    let capacity_required = if top_id >= offset {
                        ((top_id - offset) as usize) + 1 // `tail.get_mut(top_id - offset)` is invoked later
                    } else {
                        0 // No `Entry` in `batch` pertains to `tail`s range
                    };

                    if capacity_required > tail.len() {
                        tail.resize(capacity_required, None);
                    }

                    let crop = Deduplicator::crop(batch.entries.items(), offset..);

                    crop.iter()
                        .filter_map(|entry| {
                            let entry = entry.as_ref().unwrap();
                            let log = tail.get_mut((entry.id - offset) as usize).unwrap();

                            Deduplicator::deduplicate(log, entry)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let _ = join_duplicates_burst_inlet.blocking_send(duplicates_burst);
        }

        tail
    }

    async fn join(
        mut join_batch_burst_outlet: BatchBurstOutlet,
        mut join_duplicates_burst_outlets: Vec<DuplicatesBurstOutlet>,
        pop_inlet: DeduplicatedBatchInlet,
        settings: DeduplicatorSettings,
    ) -> DeduplicatedBatchInlet {
        loop {
            // Receive next burst of `Batch`es from `dispatch`

            let mut batch_burst = if let Some(burst) = join_batch_burst_outlet.recv().await {
                // Either the current iteration of `run` concluded, or
                // `Deduplicator` has dropped: shutdown in both cases.
                burst
            } else {
                break;
            };

            // Receive one burst of `Vec<Duplicate>`s from each `process_*`

            let mut duplicates_bursts = Vec::with_capacity(settings.tasks + 1);

            for join_duplicates_burst_outlet in join_duplicates_burst_outlets.iter_mut() {
                let duplicates_burst =
                    if let Some(burst) = join_duplicates_burst_outlet.recv().await {
                        burst
                    } else {
                        loop {
                            // `Deduplicator` has dropped. Idle waiting for task to be cancelled.
                            // (Note that `return`ing something meaningful is not possible)
                            loop {
                                time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                    };

                duplicates_bursts.push(duplicates_burst.into_iter());
            }

            // Extract inner `Vec<Batch>` from `batch_burst`

            // Note that:
            // - `batch_burst` was received from `dispatch`, so `dispatch` owns
            //   no copy of `batch_burst`'s `Arc`.
            // - A `duplicates_burst` was received from every `process_snap` and
            //   `process_tail`, and both drop their copy of `batch_burst`'s `Arc`
            //   after sending their `duplicates_burst`.
            // Therefore, `batch_burst` is eventually owned only by `join`.
            let batch_burst = loop {
                // This loop will (nearly) always exit on its first iteration.
                // Further iterations happen only in the off-chance `batch_burst`
                // is not decreffed in a timely fashion by `process_*`.
                batch_burst = match Arc::try_unwrap(batch_burst) {
                    Ok(batch_burst) => break batch_burst,
                    Err(batch_burst) => batch_burst,
                };
            };

            // For each `Batch` in `batch_burst`, concatenate the corresponding
            // `Vec<Duplicate>`s from each element of `duplicates_burst`. Send
            // each resulting deduplicated batch to `pop`

            for batch in batch_burst {
                let mut duplicates = Vec::new();

                for duplicates_burst in duplicates_bursts.iter_mut() {
                    duplicates.extend(duplicates_burst.next().unwrap());
                }

                let _ = pop_inlet.send((batch, duplicates)).await;
            }
        }

        pop_inlet
    }

    fn crop<R>(entries: &[Option<Entry>], range: R) -> &[Option<Entry>]
    where
        R: RangeBounds<u64>,
    {
        // Provided with an `id`, returns the index at which `id` appears
        // in `entries` (if such an entry exists), or the index at which
        // an entry with id `id` could be inserted (otherwise)

        let fit = |id| match entries.binary_search_by_key(&id, |entry| entry.as_ref().unwrap().id) {
            Ok(index) => index,
            Err(index) => index,
        };

        // Map `range` (on ids) onto the bounds of a `Range` (on indices) (start included, end excluded)

        let start = match range.start_bound() {
            Bound::Included(start) => fit(*start), // Include `fit(start)`
            Bound::Excluded(start) => fit(*start + 1), // Include `fit(start) + 1`
            Bound::Unbounded => 0,                 // Include everything
        };

        let end = match range.end_bound() {
            Bound::Included(end) => fit(*end + 1), // Exclude `fit(end)`
            Bound::Excluded(end) => fit(*end),     // Exclude `fit(end)`
            Bound::Unbounded => entries.len(),     // Include everything
        };

        &entries[start..end]
    }

    fn deduplicate(log: &mut Option<Log>, entry: &Entry) -> Option<Duplicate> {
        match log {
            Some(log) => {
                if entry.message == log.last_message {
                    if entry.sequence != log.last_sequence {
                        // The same message was previously delivered with a different
                        // sequence number: acknowledge with old sequence number to
                        // avoid duplicated delivery certificates, do not deliver.

                        Some(Duplicate::Nudge {
                            id: entry.id,
                            sequence: log.last_sequence,
                        })
                    } else {
                        // The same message was previously delivered with the same sequence
                        // number: acknowledge again, but do not deliver.

                        Some(Duplicate::Ignore { id: entry.id })
                    }
                } else {
                    if entry.sequence > log.last_sequence {
                        // The message is new and its sequence number is the highest observed
                        // at delivery time: acknowledge and deliver.
                        log.last_sequence = entry.sequence;
                        log.last_message = entry.message;

                        None
                    } else {
                        // The message is different from the last observer but its sequence
                        // number is low: the message cannot be delivered or acknowledged.
                        Some(Duplicate::Drop { id: entry.id })
                    }
                }
            }
            log => {
                // No message was previously delivered from `log`: initialize `log`,
                // deliver, and acknowledge the message.

                *log = Some(Log {
                    last_sequence: entry.sequence,
                    last_message: entry.message,
                });

                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::seq::index;

    use zebra::vector::Vector;

    #[test]
    fn crop_manual() {
        let entries = [3, 6, 9, 10, 13, 14, 18, 200]
            .into_iter()
            .map(|id| {
                Some(Entry {
                    id,
                    sequence: Default::default(),
                    message: Default::default(),
                })
            })
            .collect::<Vec<_>>();

        let ids = |slice: &[Option<Entry>]| {
            slice
                .into_iter()
                .map(|entry| entry.as_ref().unwrap().id)
                .collect::<Vec<_>>()
        };

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 10..13)),
            vec![10]
        );

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 10..14)),
            vec![10, 13]
        );

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 10..=13)),
            vec![10, 13]
        );

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 10..=15)),
            vec![10, 13, 14]
        );

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 10..)),
            vec![10, 13, 14, 18, 200]
        );

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), 6..10)),
            vec![6, 9]
        );

        assert_eq!(ids(Deduplicator::crop(entries.as_slice(), 7..10)), vec![9]);

        assert_eq!(
            ids(Deduplicator::crop(entries.as_slice(), ..10)),
            vec![3, 6, 9]
        );
    }

    #[test]
    fn crop_exhaustive() {
        let entry_ids = vec![3, 6, 9, 10, 13, 14, 18];

        let entries = entry_ids
            .iter()
            .copied()
            .map(|id| {
                Some(Entry {
                    id,
                    sequence: Default::default(),
                    message: Default::default(),
                })
            })
            .collect::<Vec<_>>();

        let ids = |slice: &[Option<Entry>]| {
            slice
                .into_iter()
                .map(|entry| entry.as_ref().unwrap().id)
                .collect::<Vec<_>>()
        };

        for start in 0u64..20 {
            for end in start..20 {
                let inclusive_exclusive = ids(Deduplicator::crop(entries.as_slice(), start..end));

                for id in entry_ids.iter().copied() {
                    let should = id >= start && id < end;
                    let is = inclusive_exclusive.contains(&id);

                    assert_eq!(should, is);
                }

                let inclusive_inclusive = ids(Deduplicator::crop(entries.as_slice(), start..=end));

                for id in entry_ids.iter().copied() {
                    let should = id >= start && id <= end;
                    let is = inclusive_inclusive.contains(&id);

                    assert_eq!(should, is);
                }

                let inclusive_open = ids(Deduplicator::crop(entries.as_slice(), start..));

                for id in entry_ids.iter().copied() {
                    let should = id >= start;
                    let is = inclusive_open.contains(&id);

                    assert_eq!(should, is);
                }

                let open_exclusive = ids(Deduplicator::crop(entries.as_slice(), ..end));

                for id in entry_ids.iter().copied() {
                    let should = id < end;
                    let is = open_exclusive.contains(&id);

                    assert_eq!(should, is);
                }

                let open_inclusive = ids(Deduplicator::crop(entries.as_slice(), ..=end));

                for id in entry_ids.iter().copied() {
                    let should = id <= end;
                    let is = open_inclusive.contains(&id);

                    assert_eq!(should, is);
                }
            }
        }

        let open_open = ids(Deduplicator::crop(entries.as_slice(), ..));

        for id in entry_ids.iter().copied() {
            let should = true;
            let is = open_open.contains(&id);

            assert_eq!(should, is);
        }
    }

    fn build_entry(id: u64, sequence: u64, message: u64) -> Entry {
        let mut buffer = Message::default();
        buffer[0..8].copy_from_slice(&message.to_be_bytes()[..]);

        Entry {
            id,
            sequence,
            message: buffer,
        }
    }

    fn build_entries<E>(entries: E) -> Vec<Option<Entry>>
    where
        E: IntoIterator<Item = (u64, u64, u64)>,
    {
        entries
            .into_iter()
            .map(|(id, sequence, message)| Some(build_entry(id, sequence, message)))
            .collect::<Vec<_>>()
    }

    fn build_batch<E>(entries: E) -> Batch
    where
        E: IntoIterator<Item = (u64, u64, u64)>,
    {
        Batch {
            entries: Vector::new(build_entries(entries)).unwrap(),
        }
    }

    #[tokio::test]
    async fn single_log() {
        let mut deduplicator = Deduplicator::with_capacity(1, Default::default());

        {
            let entries = [(0, 0, 0)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }

        {
            let entries = [(0, 1, 1)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }

        {
            let entries = [(0, 1, 1)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert_eq!(duplicates, vec![Duplicate::Ignore { id: 0 }]);
        }

        {
            let entries = [(0, 2, 1)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert_eq!(duplicates, vec![Duplicate::Nudge { id: 0, sequence: 1 }]);
        }

        {
            let entries = [(0, 1, 2)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert_eq!(duplicates, vec![Duplicate::Drop { id: 0 }]);
        }

        {
            let entries = [(0, 0, 0)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert_eq!(duplicates, vec![Duplicate::Drop { id: 0 }]);
        }

        {
            let entries = [(0, 3, 3)];

            deduplicator.push(build_batch(entries)).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }
    }

    #[tokio::test]
    async fn single_log_burst() {
        let mut deduplicator = Deduplicator::with_capacity(1, Default::default());

        let entries_0 = [(0, 0, 0)];
        deduplicator.push(build_batch(entries_0)).await;

        let entries_1 = [(0, 1, 1)];
        deduplicator.push(build_batch(entries_1)).await;

        let entries_2 = [(0, 1, 1)];
        deduplicator.push(build_batch(entries_2)).await;

        let entries_3 = [(0, 2, 1)];
        deduplicator.push(build_batch(entries_3)).await;

        let entries_4 = [(0, 1, 2)];
        deduplicator.push(build_batch(entries_4)).await;

        let entries_5 = [(0, 0, 0)];
        deduplicator.push(build_batch(entries_5)).await;

        let entries_6 = [(0, 3, 3)];
        deduplicator.push(build_batch(entries_6)).await;

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_0));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_1));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_2));
        assert_eq!(duplicates, vec![Duplicate::Ignore { id: 0 }]);

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_3));
        assert_eq!(duplicates, vec![Duplicate::Nudge { id: 0, sequence: 1 }]);

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_4));
        assert_eq!(duplicates, vec![Duplicate::Drop { id: 0 }]);

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_5));
        assert_eq!(duplicates, vec![Duplicate::Drop { id: 0 }]);

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_6));
        assert!(duplicates.is_empty());
    }

    #[tokio::test]
    async fn multiple_logs() {
        let mut deduplicator = Deduplicator::with_capacity(128, Default::default());

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id, 1024 * id))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));

            assert_eq!(
                duplicates,
                (0..128)
                    .map(|id| Duplicate::Ignore { id })
                    .collect::<Vec<_>>()
            );
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 2, 1024 * id + 1))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));

            assert_eq!(
                duplicates,
                (0..128)
                    .map(|id| Duplicate::Nudge {
                        id,
                        sequence: 1024 * id + 1
                    })
                    .collect::<Vec<_>>()
            );
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 1, 1024 * id + 2))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));

            assert_eq!(
                duplicates,
                (0..128)
                    .map(|id| Duplicate::Drop { id })
                    .collect::<Vec<_>>()
            );
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 0, 1024 * id + 0))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));

            assert_eq!(
                duplicates,
                (0..128)
                    .map(|id| Duplicate::Drop { id })
                    .collect::<Vec<_>>()
            );
        }

        {
            let entries = (0..128)
                .map(|id| (id, 1024 * id + 3, 1024 * id + 3))
                .collect::<Vec<_>>();

            deduplicator.push(build_batch(entries.clone())).await;
            let (batch, duplicates) = deduplicator.pop().await;

            assert_eq!(batch.entries.items(), &build_entries(entries));
            assert!(duplicates.is_empty());
        }
    }

    #[tokio::test]
    async fn multiple_logs_burst() {
        let mut deduplicator = Deduplicator::with_capacity(128, Default::default());

        let entries_0 = (0..128)
            .map(|id| (id, 1024 * id, 1024 * id))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_0.clone())).await;

        let entries_1 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_1.clone())).await;

        let entries_2 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_2.clone())).await;

        let entries_3 = (0..128)
            .map(|id| (id, 1024 * id + 2, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_3.clone())).await;

        let entries_4 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 2))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_4.clone())).await;

        let entries_5 = (0..128)
            .map(|id| (id, 1024 * id + 0, 1024 * id + 0))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_5.clone())).await;

        let entries_6 = (0..128)
            .map(|id| (id, 1024 * id + 3, 1024 * id + 3))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_6.clone())).await;

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_0));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_1));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_2));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Ignore { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_3));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Nudge {
                    id,
                    sequence: 1024 * id + 1
                })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_4));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_5));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_6));
        assert!(duplicates.is_empty());
    }

    #[tokio::test]
    async fn tail() {
        let mut deduplicator = Deduplicator::with_capacity(0, Default::default());

        let entries_0 = (0..64)
            .map(|id| (id, 1024 * id, 1024 * id))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_0.clone())).await;

        let entries_1 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_1.clone())).await;

        let entries_2 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_2.clone())).await;

        let entries_3 = (0..128)
            .map(|id| (id, 1024 * id + 2, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_3.clone())).await;

        let entries_4 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 2))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_4.clone())).await;

        let entries_5 = (0..128)
            .map(|id| (id, 1024 * id + 0, 1024 * id + 0))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_5.clone())).await;

        let entries_6 = (0..256)
            .map(|id| (id, 1024 * id + 3, 1024 * id + 3))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_6.clone())).await;

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_0));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_1));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_2));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Ignore { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_3));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Nudge {
                    id,
                    sequence: 1024 * id + 1
                })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_4));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_5));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_6));
        assert!(duplicates.is_empty());
    }

    #[tokio::test]
    async fn merge_tail() {
        let mut deduplicator = Deduplicator::with_capacity(
            0,
            DeduplicatorSettings {
                run_duration: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let entries_0 = (0..64)
            .map(|id| (id, 1024 * id, 1024 * id))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_0.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_1 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_1.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_2 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_2.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_3 = (0..128)
            .map(|id| (id, 1024 * id + 2, 1024 * id + 1))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_3.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_4 = (0..128)
            .map(|id| (id, 1024 * id + 1, 1024 * id + 2))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_4.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_5 = (0..128)
            .map(|id| (id, 1024 * id + 0, 1024 * id + 0))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_5.clone())).await;

        time::sleep(Duration::from_millis(200)).await;

        let entries_6 = (0..256)
            .map(|id| (id, 1024 * id + 3, 1024 * id + 3))
            .collect::<Vec<_>>();

        deduplicator.push(build_batch(entries_6.clone())).await;

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_0));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_1));
        assert!(duplicates.is_empty());

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_2));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Ignore { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_3));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Nudge {
                    id,
                    sequence: 1024 * id + 1
                })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_4));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_5));

        assert_eq!(
            duplicates,
            (0..128)
                .map(|id| Duplicate::Drop { id })
                .collect::<Vec<_>>()
        );

        let (batch, duplicates) = deduplicator.pop().await;
        assert_eq!(batch.entries.items(), &build_entries(entries_6));
        assert!(duplicates.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn stress() {
        let mut ticks = vec![0; 1048576];

        let deduplicated_batches = (0..128)
            .map(|step| {
                let mut logs =
                    index::sample(&mut rand::thread_rng(), 131072 + step, 65536).into_vec();

                logs.sort_unstable();

                let (entries, duplicates): (Vec<_>, Vec<_>) = logs
                    .into_iter()
                    .map(|log| {
                        let tick = ticks.get_mut(log).unwrap();
                        let id = log as u64;

                        let action = if *tick > 0 {
                            rand::random::<u8>() % 4
                        } else {
                            0
                        };

                        match action {
                            0 => {
                                // (Ok): next tick, correct message and sequence
                                *tick += 1;
                                ((id, 1024 * *tick, *tick), None)
                            }
                            1 => {
                                // (Ignore): same tick, correct message and sequence
                                ((id, 1024 * *tick, *tick), Some(Duplicate::Ignore { id }))
                            }
                            2 => {
                                // (Nudge): same tick, correct message, increase sequence by random % 1024
                                (
                                    (id, 1024 * *tick + (1 + rand::random::<u64>() % 1023), *tick),
                                    Some(Duplicate::Nudge {
                                        id,
                                        sequence: 1024 * *tick,
                                    }),
                                )
                            }
                            3 => {
                                // (Drop): same tick, correct sequence, incorrect message
                                (
                                    (id, 1024 * *tick, u64::MAX - *tick),
                                    Some(Duplicate::Drop { id }),
                                )
                            }
                            4.. => unreachable!(),
                        }
                    })
                    .unzip();

                let batch = build_batch(entries);

                let duplicates = duplicates
                    .into_iter()
                    .filter_map(|duplicate| duplicate)
                    .collect::<Vec<_>>();

                (batch, duplicates)
            })
            .collect::<Vec<_>>();

        let mut deduplicator = Deduplicator::with_capacity(
            0,
            DeduplicatorSettings {
                burst_size: 1,
                run_duration: Duration::from_millis(100),
                ..Default::default()
            },
        );

        for (batch, expected_duplicates) in deduplicated_batches {
            deduplicator.push(batch).await;
            let (_, duplicates) = deduplicator.pop().await;
            assert_eq!(duplicates, expected_duplicates);
        }
    }
}
