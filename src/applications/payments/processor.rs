use crate::{applications::payments::Payment, broadcast::Entry};
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

// TODO: Refactor `const`s into settings
const SHARDS: usize = 8;

const ACCOUNTS: u64 = 258000000;
const INITIAL_BALANCE: u64 = 1000000000;

const PROCESS_CHANNEL_CAPACITY: usize = 8192;

pub struct Processor {
    process_inlet: BatchInlet,
    _fuse: Fuse,
}

#[derive(Clone)]
struct Deposit {
    to: u64,
    amount: u64,
}

impl Processor {
    pub fn new() -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(PROCESS_CHANNEL_CAPACITY);
        let fuse = Fuse::new();

        fuse.spawn(Processor::process(process_outlet));

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

    async fn process(mut process_outlet: BatchOutlet) {
        // Each shard is relevant to `ceil(ACCOUNTS / SHARDS)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let shard_span = (ACCOUNTS + (SHARDS as u64) - 1) / (SHARDS as u64);

        let mut balance_shards = vec![vec![INITIAL_BALANCE; shard_span as usize]; SHARDS];

        loop {
            let batch = if let Some(batch) = process_outlet.recv().await {
                batch
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            let batch = Arc::new(batch);

            balance_shards = {
                // Spawn and join withdraw tasks

                let withdraw_tasks = balance_shards
                    .into_iter()
                    .enumerate()
                    .map(|(shard, balance_shard)| {
                        let batch = batch.clone();

                        task::spawn_blocking(move || {
                            Processor::withdraw(batch, shard as u64, shard_span, balance_shard)
                        })
                    })
                    .collect::<FuturesOrdered<_>>();

                let withdraw_results = withdraw_tasks
                    .map(|task| task.unwrap())
                    .collect::<Vec<_>>()
                    .await;

                // Rebuild balance shards, transpose deposit matrix

                let mut balance_shards = Vec::with_capacity(SHARDS);
                let mut deposit_columns = vec![Vec::with_capacity(SHARDS); SHARDS];

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
    ) -> (Vec<u64>, Vec<Vec<Deposit>>) {
        let offset = shard * shard_span;
        let range = offset..(offset + shard_span);

        let payments = Processor::crop(batch.as_slice(), range);

        let mut deposit_row = vec![Vec::with_capacity(payments.len()); SHARDS as usize];

        for payment in payments {
            let balance = balance_shard
                .get_mut((payment.from - offset) as usize)
                .unwrap();

            if *balance >= payment.amount {
                *balance -= payment.amount;

                deposit_row[(payment.to / shard_span) as usize].push(Deposit {
                    to: payment.to,
                    amount: payment.amount,
                });
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
