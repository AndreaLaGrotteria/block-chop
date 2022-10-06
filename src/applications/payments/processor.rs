use crate::{
    applications::payments::{Deposit, Payment, ProcessorSettings},
    broadcast::Entry,
};
use futures::{stream::FuturesOrdered, StreamExt};
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};
use talk::sync::fuse::Fuse;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task,
};

type BatchInlet = Sender<Vec<Payment>>;
type BatchOutlet = Receiver<Vec<Payment>>;

pub struct Processor {
    process_inlet: BatchInlet,
    _fuse: Fuse,
}

impl Processor {
    pub fn new(accounts: u64, initial_balance: u64, settings: ProcessorSettings) -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(settings.process_channel_capacity);
        let fuse = Fuse::new();

        fuse.spawn(Processor::process(
            accounts,
            initial_balance,
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
        I: Iterator<Item = Entry>,
    {
        let batch = batch.map(Payment::from_entry).collect::<Vec<_>>();

        // A copy of `process_outlet` is held by `Processor::process`,
        // whose `Fuse` is held by `self`, so `process_inlet.send()`
        // cannot return an `Err`.
        self.process_inlet.send(batch).await.unwrap();
    }

    async fn process(
        accounts: u64,
        initial_balance: u64,
        mut process_outlet: BatchOutlet,
        settings: ProcessorSettings,
    ) {
        // Initialize balance shards

        // Each shard is relevant to `ceil(accounts / settings.shards)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let shard_span = (accounts + (settings.shards as u64) - 1) / (settings.shards as u64);

        let mut balance_shards = vec![vec![initial_balance; shard_span as usize]; settings.shards];

        loop {
            // Receive next batch

            let batch = if let Some(batch) = process_outlet.recv().await {
                batch
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            // Apply `batch` to `balance_shards`

            let batch = Arc::new(batch);

            balance_shards = {
                // Spawn and join withdraw tasks

                let withdraw_tasks = balance_shards
                    .into_iter()
                    .enumerate()
                    .map(|(shard, balance_shard)| {
                        let batch = batch.clone();
                        let settings = settings.clone();

                        task::spawn_blocking(move || {
                            Processor::withdraw(
                                batch,
                                shard as u64,
                                shard_span,
                                balance_shard,
                                settings,
                            )
                        })
                    })
                    .collect::<FuturesOrdered<_>>();

                let withdraw_results = withdraw_tasks
                    .map(|task| task.unwrap())
                    .collect::<Vec<_>>()
                    .await;

                // Rebuild balance shards, transpose deposit matrix

                let mut balance_shards = Vec::with_capacity(settings.shards);
                let mut deposit_columns =
                    vec![Vec::with_capacity(settings.shards); settings.shards];

                for (balance_shard, deposit_row) in withdraw_results {
                    balance_shards.push(balance_shard);

                    for (deposit_column, bucket) in deposit_columns.iter_mut().zip(deposit_row) {
                        deposit_column.push(bucket);
                    }
                }

                // Spawn and join deposit tasks

                let deposit_tasks = deposit_columns
                    .into_iter()
                    .zip(balance_shards)
                    .enumerate()
                    .map(|(shard, (deposit_column, balance_shard))| {
                        task::spawn_blocking(move || {
                            Processor::deposit(
                                deposit_column,
                                shard as u64,
                                shard_span,
                                balance_shard,
                            )
                        })
                    })
                    .collect::<FuturesOrdered<_>>();

                let balance_shards = deposit_tasks
                    .map(|task| task.unwrap())
                    .collect::<Vec<_>>()
                    .await;

                balance_shards
            };
        }
    }

    fn withdraw(
        batch: Arc<Vec<Payment>>,
        shard: u64,
        shard_span: u64,
        mut balance_shard: Vec<u64>,
        settings: ProcessorSettings,
    ) -> (Vec<u64>, Vec<Vec<Deposit>>) {
        let offset = shard * shard_span;

        let payments = Processor::crop(batch.as_slice(), offset..(offset + shard_span));
        let mut deposit_row = vec![Vec::with_capacity(payments.len()); settings.shards];

        for payment in payments {
            let balance = balance_shard
                .get_mut((payment.from - offset) as usize)
                .unwrap();

            if *balance >= payment.amount {
                *balance -= payment.amount;
                deposit_row[(payment.to / shard_span) as usize].push(payment.deposit());
            }
        }

        (balance_shard, deposit_row)
    }

    fn deposit(
        deposit_column: Vec<Vec<Deposit>>,
        shard: u64,
        shard_span: u64,
        mut balance_shard: Vec<u64>,
    ) -> Vec<u64> {
        let offset = shard * shard_span;

        for bucket in deposit_column {
            for deposit in bucket {
                balance_shard[(deposit.to - offset) as usize] += deposit.amount;
            }
        }

        balance_shard
    }

    fn crop<R>(payments: &[Payment], range: R) -> &[Payment]
    where
        R: RangeBounds<u64>,
    {
        // Provided with an `id`, returns the index at which `id` appears
        // as `from` in `payments` (if such a payment exists), or the index
        // at which a payment from `id` could be inserted (otherwise)

        let fit = |id| match payments.binary_search_by_key(&id, |payment| payment.from) {
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
