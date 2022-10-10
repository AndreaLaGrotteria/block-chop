use crate::{
    applications::pixel_war::{Color, Coordinates, Paint, ProcessorSettings, CANVAS_EDGE},
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

type BurstOutlet = MpscReceiver<Arc<Vec<Vec<Paint>>>>;

type BatchInlet = MpscSender<Vec<Paint>>;
type BatchOutlet = MpscReceiver<Vec<Paint>>;

type TaskOutlet = MpscReceiver<FilterTask>;
type EmptyInlet = OneShotSender<()>;

struct FilterTask {
    batch_burst: Arc<Vec<Vec<Paint>>>,
    return_inlet: EmptyInlet,
}

pub struct Processor {
    process_inlet: BatchInlet,
    _fuse: Fuse,
}

impl Processor {
    pub fn new(accounts: u64, throttle_period: u64, settings: ProcessorSettings) -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(settings.pipeline);
        let fuse = Fuse::new();

        fuse.spawn(Processor::process(
            accounts,
            throttle_period,
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
        throttle_period: u64,
        mut process_outlet: BatchOutlet,
        settings: ProcessorSettings,
    ) {
        let (burst_inlet, burst_outlet) = mpsc::channel(settings.pipeline);

        let _task = task::spawn_blocking(move || {
            Processor::apply(burst_outlet);
        });
        // Spawn shard tasks

        // Each shard is relevant to `ceil(accounts / settings.shards)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let shard_span = (accounts + (settings.shards as u64) - 1) / (settings.shards as u64);

        let task_inlets = (0..settings.shards)
            .map(|shard| {
                let (task_inlet, task_outlet) = mpsc::channel(1);

                task::spawn_blocking(move || {
                    Processor::shard(shard as u64, shard_span, throttle_period, task_outlet);
                });

                task_inlet
            })
            .collect::<Vec<_>>();

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

            // Perform paints to obtain filtered ones

            let return_outlets = task_inlets
                .iter()
                .map(|task_inlet| {
                    let (return_inlet, return_outlet) = oneshot::channel();

                    let task = FilterTask {
                        batch_burst: batch_burst.clone(),
                        return_inlet,
                    };

                    // No more than one task is ever fed to `task_inlet` at the same time,
                    // and `shard` does not shutdown before `task_inlet` is dropped
                    let _ = task_inlet.try_send(task);

                    return_outlet
                })
                .collect::<FuturesOrdered<_>>();

            let _filtered_paints = return_outlets
                .map(|return_outlet| return_outlet.unwrap())
                .collect::<Vec<_>>()
                .await;

            burst_inlet.send(batch_burst).await.unwrap();
        }
    }

    fn apply(mut receiver: BurstOutlet) {
        let mut canvas = [[Color::default(); CANVAS_EDGE as usize]; CANVAS_EDGE as usize];

        loop {
            if let Some(burst) = receiver.blocking_recv() {
                for Paint {
                    coordinates: Coordinates { x, y },
                    color,
                    ..
                } in burst
                    .iter()
                    .flatten()
                    .filter(|paint| !paint.throttle.load(Ordering::Relaxed))
                {
                    canvas[*x as usize][*y as usize] = *color;
                }
            } else {
                // `Processor` has dropped, shutdown
                return;
            }
        }
    }

    fn shard(shard: u64, shard_span: u64, throttle_period: u64, mut task_outlet: TaskOutlet) {
        let mut current_time = throttle_period;
        let mut last_change_shard = vec![0; shard_span as usize];

        loop {
            if let Some(FilterTask {
                batch_burst,
                return_inlet,
            }) = task_outlet.blocking_recv()
            {
                Processor::filter(
                    batch_burst.as_ref(),
                    throttle_period,
                    current_time,
                    shard,
                    shard_span,
                    &mut last_change_shard,
                );

                current_time += 1;

                let _ = return_inlet.send(());
            } else {
                // `Processor` has dropped, shutdown
                return;
            };
        }
    }

    fn filter(
        batch_burst: &Vec<Vec<Paint>>,
        throttle_period: u64,
        current_time: u64,
        shard: u64,
        shard_span: u64,
        last_change_shard: &mut Vec<u64>,
    ) {
        let offset = shard * shard_span;

        for batch in batch_burst.iter() {
            let paints = Processor::crop(batch.as_slice(), offset..(offset + shard_span));

            for paint in paints {
                let last_change = last_change_shard
                    .get_mut((paint.painter - offset) as usize)
                    .unwrap();

                if *last_change > current_time - throttle_period {
                    *last_change = current_time;
                } else {
                    paint.throttle.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    fn crop<R>(payments: &[Paint], range: R) -> &[Paint]
    where
        R: RangeBounds<u64>,
    {
        // Provided with an `id`, returns the index at which `id` appears
        // as `from` in `payments` (if such a payment exists), or the index
        // at which a payment from `id` could be inserted (otherwise)

        let fit = |id| match payments.binary_search_by_key(&id, |payment| payment.painter) {
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
            Bound::Unbounded => payments.len(),    // Include everything
        };

        &payments[start..end]
    }
}
