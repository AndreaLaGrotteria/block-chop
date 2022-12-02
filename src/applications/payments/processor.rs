use crate::{
    applications::payments::{Deposit, Payment, ProcessorSettings},
    broadcast::Entry,
};
use futures::{stream::FuturesOrdered, StreamExt};
use std::{
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use talk::sync::fuse::Fuse;
use tokio::{
    sync::{
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::{self, Sender as OneShotSender},
    },
    task,
};

type BatchInlet = MpscSender<Vec<Payment>>;
type BatchOutlet = MpscReceiver<Vec<Payment>>;

type TaskOutlet = MpscReceiver<Task>;

type DepositRowInlet = OneShotSender<Vec<Vec<Deposit>>>;
type EmptyInlet = OneShotSender<()>;

pub struct Processor {
    process_inlet: BatchInlet,
    operations_processed: Arc<AtomicU64>,
    _fuse: Fuse,
}

enum Task {
    Withdraw {
        batch_burst: Arc<Vec<Vec<Payment>>>,
        return_inlet: DepositRowInlet,
    },
    Deposit {
        deposit_column: Vec<Vec<Deposit>>,
        return_inlet: EmptyInlet,
    },
}

impl Processor {
    pub fn new(accounts: u64, initial_balance: u64, settings: ProcessorSettings) -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(settings.pipeline);
        let fuse = Fuse::new();

        let operations_processed = Arc::new(AtomicU64::new(0));

        fuse.spawn(Processor::process(
            accounts,
            initial_balance,
            process_outlet,
            operations_processed.clone(),
            settings,
        ));

        Processor {
            process_inlet,
            operations_processed,
            _fuse: fuse,
        }
    }

    pub fn operations_processed(&self) -> u64 {
        self.operations_processed.load(Ordering::Relaxed)
    }

    pub async fn push<I>(&self, batch: I)
    where
        I: IntoIterator<Item = Entry>,
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

    async fn process(
        accounts: u64,
        initial_balance: u64,
        mut process_outlet: BatchOutlet,
        operations_processed: Arc<AtomicU64>,
        settings: ProcessorSettings,
    ) {
        // Spawn shard tasks

        // Each shard is relevant to `ceil(accounts / settings.shards)` accounts
        // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
        let shard_span = (accounts + (settings.shards as u64) - 1) / (settings.shards as u64);

        let task_inlets = (0..settings.shards)
            .map(|shard| {
                let settings = settings.clone();

                let (task_inlet, task_outlet) = mpsc::channel(1);

                task::spawn_blocking(move || {
                    Processor::shard(
                        shard as u64,
                        shard_span,
                        initial_balance,
                        task_outlet,
                        settings,
                    );
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
            let burst_operation_count = batch_burst.iter().map(Vec::len).sum::<usize>() as u64;

            // Perform withdraws to obtain deposit rows

            let return_outlets = task_inlets
                .iter()
                .map(|task_inlet| {
                    let (return_inlet, return_outlet) = oneshot::channel();

                    let task = Task::Withdraw {
                        batch_burst: batch_burst.clone(),
                        return_inlet,
                    };

                    // No more than one task is ever fed to `task_inlet` at the same time,
                    // and `shard` does not shutdown before `task_inlet` is dropped
                    let _ = task_inlet.try_send(task);

                    return_outlet
                })
                .collect::<FuturesOrdered<_>>();

            let deposit_rows = return_outlets
                .map(|return_outlet| return_outlet.unwrap())
                .collect::<Vec<_>>()
                .await;

            // Transpose deposit matrix

            let mut deposit_columns = vec![Vec::with_capacity(settings.shards); settings.shards];

            for deposit_row in deposit_rows {
                for (deposit_column, bucket) in deposit_columns.iter_mut().zip(deposit_row) {
                    deposit_column.push(bucket);
                }
            }

            // Perform deposits on deposit columns

            let return_outlets = task_inlets
                .iter()
                .zip(deposit_columns)
                .map(|(task_inlet, deposit_column)| {
                    let (return_inlet, return_outlet) = oneshot::channel();

                    let task = Task::Deposit {
                        deposit_column,
                        return_inlet,
                    };

                    // No more than one task is ever fed to `task_inlet` at the same time,
                    // and `shard` does not shutdown before `task_inlet` is dropped
                    let _ = task_inlet.try_send(task);

                    return_outlet
                })
                .collect::<FuturesOrdered<_>>();

            return_outlets
                .map(|return_outlet| return_outlet.unwrap())
                .collect::<()>()
                .await;

            // Increment `operations_processed`

            operations_processed.fetch_add(burst_operation_count, Ordering::Relaxed);
        }
    }

    fn shard(
        shard: u64,
        shard_span: u64,
        initial_balance: u64,
        mut task_outlet: TaskOutlet,
        settings: ProcessorSettings,
    ) {
        let mut balance_shard = vec![initial_balance; shard_span as usize];

        loop {
            let task = if let Some(task) = task_outlet.blocking_recv() {
                task
            } else {
                // `Processor` has dropped, shutdown
                return;
            };

            match task {
                Task::Withdraw {
                    batch_burst,
                    return_inlet,
                } => {
                    let deposit_row = Processor::withdraw(
                        batch_burst.as_ref(),
                        shard,
                        shard_span,
                        &mut balance_shard,
                        &settings,
                    );

                    let _ = return_inlet.send(deposit_row);
                }
                Task::Deposit {
                    deposit_column,
                    return_inlet,
                } => {
                    Processor::deposit(deposit_column, shard, shard_span, &mut balance_shard);
                    let _ = return_inlet.send(());
                }
            }
        }
    }

    fn withdraw(
        batch_burst: &Vec<Vec<Payment>>,
        shard: u64,
        shard_span: u64,
        balance_shard: &mut Vec<u64>,
        settings: &ProcessorSettings,
    ) -> Vec<Vec<Deposit>> {
        let offset = shard * shard_span;

        let mut deposit_row =
            vec![Vec::with_capacity(settings.deposit_bucket_capacity); settings.shards];

        for batch in batch_burst.iter() {
            let payments = Processor::crop(batch.as_slice(), offset..(offset + shard_span));

            for payment in payments {
                let balance = balance_shard
                    .get_mut((payment.from - offset) as usize)
                    .unwrap();

                if *balance >= payment.amount {
                    *balance -= payment.amount;
                    deposit_row[(payment.to / shard_span) as usize].push(payment.deposit());
                }
            }
        }

        deposit_row
    }

    fn deposit(
        deposit_column: Vec<Vec<Deposit>>,
        shard: u64,
        shard_span: u64,
        balance_shard: &mut Vec<u64>,
    ) {
        let offset = shard * shard_span;

        for bucket in deposit_column {
            for deposit in bucket {
                balance_shard[(deposit.to - offset) as usize] += deposit.amount;
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::Entry;
    use rayon::prelude::{IntoParallelIterator, ParallelIterator};
    use std::time::Duration;
    use tokio::time;

    #[tokio::test]
    async fn payments_stress() {
        let accounts = 257_000_000;

        println!("Generating messages..");

        let messages = (0..3000)
            .map(|batch| {
                (0..65536)
                    .into_par_iter()
                    .map(|offset| {
                        let id = batch * 65536 + offset;
                        let payment = Payment::generate(id, accounts, 10);
                        Entry {
                            id,
                            sequence: 0,
                            message: payment.to_message().1,
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let processor = Arc::new(Processor::new(accounts, 100_000, Default::default()));

        println!("Starting processor..");

        {
            let processor = processor.clone();

            tokio::spawn(async move {
                for batch in messages {
                    processor.push(batch).await
                }
            });
        }

        let mut old_count = 0;
        for _ in 0..20 {
            let new_count = processor.operations_processed();
            println!("Payments processed per second: {}", new_count - old_count);
            old_count = new_count;
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}
