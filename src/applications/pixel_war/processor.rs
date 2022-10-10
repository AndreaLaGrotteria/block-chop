use crate::{
    applications::pixel_war::{Color, Paint, ProcessorSettings, CANVAS_EDGE},
    broadcast::Entry,
};
use futures::{stream::FuturesOrdered, StreamExt};
use std::{
    ops::{Bound, RangeBounds},
    sync::{atomic::Ordering, Arc},
};
use talk::sync::fuse::Fuse;
use tokio::{
    sync::{
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::{self, Sender as OneShotSender},
    },
    task::{self},
};

type BatchInlet = MpscSender<Vec<Paint>>;
type BatchOutlet = MpscReceiver<Vec<Paint>>;

type FilterTaskOutlet = MpscReceiver<FilterTask>;
type EmptyInlet = OneShotSender<()>;

type BurstOutlet = MpscReceiver<Arc<Vec<Vec<Paint>>>>;

pub struct Processor {
    process_inlet: BatchInlet,
    _fuse: Fuse,
}

struct FilterTask {
    batch_burst: Arc<Vec<Vec<Paint>>>,
    return_inlet: EmptyInlet,
}

impl Processor {
    pub fn new(accounts: u64, cooldown_period: u64, settings: ProcessorSettings) -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(settings.pipeline);
        let fuse = Fuse::new();

        fuse.spawn(Processor::process(
            accounts,
            cooldown_period,
            process_outlet,
            settings,
        ));

        Processor {
            process_inlet,
            _fuse: fuse,
        }
    }

    pub async fn push<I>(&self, batch: I)
    where
        I: IntoIterator<Item = Entry>,
    {
        let batch = batch.into_iter().map(Paint::from_entry).collect::<Vec<_>>();

        // A copy of `process_outlet` is held by `Processor::process`,
        // whose `Fuse` is held by `self`, so `process_inlet.send()`
        // cannot return an `Err`.
        self.process_inlet.send(batch).await.unwrap();
    }

    async fn process(
        accounts: u64,
        cooldown_period: u64,
        mut process_outlet: BatchOutlet,
        settings: ProcessorSettings,
    ) {
        // Spawn filter tasks

        // Each filter is relevant to `ceil(accounts / settings.shards)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let filter_span = (accounts + (settings.shards as u64) - 1) / (settings.shards as u64);

        let filter_inlets = (0..settings.shards)
            .map(|filter| {
                let (filter_inlet, filter_outlet) = mpsc::channel(1);

                task::spawn_blocking(move || {
                    Processor::filter(filter as u64, filter_span, cooldown_period, filter_outlet);
                });

                filter_inlet
            })
            .collect::<Vec<_>>();

        // Spawn apply task

        let (apply_inlet, apply_outlet) =
            mpsc::channel(settings.pipeline / settings.batch_burst_size);

        task::spawn_blocking(move || {
            Processor::apply(apply_outlet);
        });

        loop {
            // Collect next batch burst

            let mut batch_burst = Vec::with_capacity(settings.batch_burst_size);

            for _ in 0..settings.batch_burst_size {
                let batch = if let Some(batch) = process_outlet.recv().await {
                    batch
                } else {
                    // `Processor` has dropped, shutdown
                    return;
                };

                batch_burst.push(batch);
            }

            let batch_burst = Arc::new(batch_burst);

            // Filter out `Paint`s that need to be throttled

            let return_outlets = filter_inlets
                .iter()
                .map(|filter_inlet| {
                    let (return_inlet, return_outlet) = oneshot::channel();

                    let filter_task = FilterTask {
                        batch_burst: batch_burst.clone(),
                        return_inlet,
                    };

                    // No more than one task is ever fed to `task_inlet` at the same time,
                    // and `shard` does not shutdown before `task_inlet` is dropped
                    let _ = filter_inlet.try_send(filter_task);

                    return_outlet
                })
                .collect::<FuturesOrdered<_>>();

            return_outlets
                .map(|return_outlet| return_outlet.unwrap())
                .collect::<()>()
                .await;

            // Apply `Paint`s to the canvas

            apply_inlet.send(batch_burst).await.unwrap();
        }
    }

    fn filter(
        filter: u64,
        filter_span: u64,
        cooldown_period: u64,
        mut filter_outlet: FilterTaskOutlet,
    ) {
        let offset = filter * filter_span;

        let mut current_time = cooldown_period;
        let mut last_paint_shard = vec![0; filter_span as usize];

        loop {
            let task = if let Some(task) = filter_outlet.blocking_recv() {
                task
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            for batch in task.batch_burst.iter() {
                let paints = Processor::crop(batch.as_slice(), offset..(offset + filter_span));

                for paint in paints {
                    let last_paint = last_paint_shard
                        .get_mut((paint.painter - offset) as usize)
                        .unwrap();

                    if *last_paint <= current_time - cooldown_period {
                        *last_paint = current_time;
                    } else {
                        paint.throttle.store(true, Ordering::Relaxed);
                    }
                }
            }

            current_time += 1;

            let _ = task.return_inlet.send(());
        }
    }

    fn apply(mut apply_outlet: BurstOutlet) {
        let mut canvas = [[Color::default(); CANVAS_EDGE]; CANVAS_EDGE];

        loop {
            let burst = if let Some(burst) = apply_outlet.blocking_recv() {
                burst
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            let paints = burst
                .iter()
                .flatten()
                .filter(|paint| !paint.throttle.load(Ordering::Relaxed));

            for paint in paints {
                canvas[paint.coordinates.x as usize][paint.coordinates.y as usize] = paint.color;
            }
        }
    }

    fn crop<R>(paints: &[Paint], range: R) -> &[Paint]
    where
        R: RangeBounds<u64>,
    {
        // Provided with an `id`, returns the index at which `id` appears
        // as `painter` in `paints` (if such a `Paint` exists), or the index
        // at which a `Paint` from `id` could be inserted (otherwise)

        let fit = |id| match paints.binary_search_by_key(&id, |paint| paint.painter) {
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
            Bound::Unbounded => paints.len(),      // Include everything
        };

        &paints[start..end]
    }
}
