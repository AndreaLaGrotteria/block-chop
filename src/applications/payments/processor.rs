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
const PROCESS_CHANNEL_CAPACITY: usize = 8192;
const ACCOUNTS: u64 = 258000000;
const INITIAL_BALANCE: u64 = 1000000000;
const SHARDS: usize = 8;

pub struct Processor {
    process_inlet: BatchInlet,
    _fuse: Fuse,
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
        let batch = batch
            .into_iter()
            .map(Payment::from_entry)
            .collect::<Vec<_>>();

        // A copy of `process_outlet` is held by `Processor::process`,
        // whose `Fuse` is held by `self`, so `process_inlet.send()`
        // cannot return an `Err`.
        self.process_inlet.send(batch).await.unwrap();
    }

    async fn process(mut process_outlet: BatchOutlet) {
        // Each shard has `ceil(ACCOUNTS / TASKS)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let shard_size = (ACCOUNTS + (SHARDS as u64) - 1) / (SHARDS as u64);

        let mut balances = vec![vec![INITIAL_BALANCE; shard_size as usize]; SHARDS];

        loop {
            let batch = if let Some(batch) = process_outlet.recv().await {
                batch
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            let batch = Arc::new(batch);

            balances = {
                let withdraw_tasks = balances
                    .into_iter()
                    .enumerate()
                    .map(|(shard, balances)| {
                        let batch = batch.clone();

                        task::spawn_blocking(move || {
                            Processor::withdraw(batch, shard as u64, shard_size, balances)
                        })
                    })
                    .collect::<FuturesOrdered<_>>();

                let results = withdraw_tasks
                    .map(|task| task.unwrap())
                    .collect::<Vec<_>>()
                    .await;

                let mut balances = Vec::with_capacity(SHARDS);
                let mut deposits = vec![Vec::with_capacity(batch.len()); SHARDS];

                for (balance, deposit) in results {
                    balances.push(balance);

                    for (rd, dd) in deposit.into_iter().zip(deposits.iter_mut()) {
                        dd.extend(rd);
                    }
                }

                let deposit_tasks = deposits
                    .into_iter()
                    .zip(balances)
                    .enumerate()
                    .map(|(shard, (deposits, balances))| {
                        task::spawn_blocking(move || {
                            Processor::deposit(deposits, shard as u64, shard_size, balances)
                        })
                    })
                    .collect::<FuturesOrdered<_>>();

                let balances = deposit_tasks
                    .map(|task| task.unwrap())
                    .collect::<Vec<_>>()
                    .await;

                balances
            };
        }
    }

    fn withdraw(
        batch: Arc<Vec<Payment>>,
        shard: u64,
        shard_size: u64,
        mut balances: Vec<u64>,
    ) -> (Vec<u64>, Vec<Vec<Payment>>) {
        let offset = shard * shard_size;
        let range = offset..(offset + shard_size);

        let payments = Processor::crop(batch.as_slice(), range);
        let mut deposits =
            vec![Vec::<Payment>::with_capacity(shard_size as usize); SHARDS as usize];

        for payment in payments {
            let balance = balances.get_mut((payment.from - offset) as usize).unwrap();

            if *balance >= payment.amount {
                *balance -= payment.amount;
                deposits[(payment.to / shard_size) as usize].push(payment.clone());
            }
        }

        (balances, deposits)
    }

    fn deposit(
        deposits: Vec<Payment>,
        shard: u64,
        shard_size: u64,
        mut balances: Vec<u64>,
    ) -> Vec<u64> {
        let offset = shard * shard_size;

        for deposit in deposits {
            balances[(deposit.to - offset) as usize] += deposit.amount;
        }

        balances
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
