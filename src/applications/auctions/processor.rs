use crate::{
    applications::auctions::{Bid, ProcessorSettings, Request, Token},
    broadcast::Entry,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
    task::{self},
};

type BatchInlet = MpscSender<Vec<Request>>;
type BatchOutlet = MpscReceiver<Vec<Request>>;

pub struct Processor {
    process_inlet: BatchInlet,
    operations_processed: Arc<AtomicU64>,
}

impl Processor {
    pub fn new(
        accounts: u64,
        initial_balance: u64,
        token_owners: Vec<u64>,
        settings: ProcessorSettings,
    ) -> Self {
        let (process_inlet, process_outlet) = mpsc::channel(settings.process_channel_capacity);
        let operations_processed = Arc::new(AtomicU64::new(0));

        {
            let operations_processed = operations_processed.clone();

            task::spawn_blocking(move || {
                Processor::process(
                    accounts,
                    initial_balance,
                    token_owners,
                    process_outlet,
                    operations_processed,
                )
            });
        }

        Processor {
            process_inlet,
            operations_processed,
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
            .map(Request::from_entry)
            .collect::<Vec<_>>();

        // A copy of `process_outlet` is held by `Processor::process`,
        // whose `Fuse` is held by `self`, so `process_inlet.send()`
        // cannot return an `Err`.
        self.process_inlet.send(batch).await.unwrap();
    }

    fn process(
        accounts: u64,
        initial_balance: u64,
        token_owners: Vec<u64>,
        mut process_outlet: BatchOutlet,
        operations_processed: Arc<AtomicU64>,
    ) {
        let mut balances = vec![initial_balance; accounts as usize];

        let mut tokens = token_owners
            .into_iter()
            .map(|owner| Token {
                owner,
                best_bid: None,
            })
            .collect::<Vec<_>>();

        loop {
            // Receive next batch

            let batch = if let Some(batch) = process_outlet.blocking_recv() {
                batch
            } else {
                // `Processor` dropped, shutdown
                return;
            };

            let batch_operation_count = batch.len() as u64;

            // Fulfill `batch`'s `Request`s

            for request in batch {
                match request {
                    Request::Bid {
                        bidder,
                        token,
                        offer,
                    } => {
                        let bidder_balance = balances.get_mut(bidder as usize).unwrap();

                        if *bidder_balance >= offer {
                            *bidder_balance -= offer;
                        } else {
                            // Not enough balance to place the bid
                            continue;
                        }

                        let bid = Bid { bidder, offer };

                        let token = if let Some(token) = tokens.get_mut(token as usize) {
                            token
                        } else {
                            // `Token` does not exist
                            continue;
                        };

                        let refund_bid = if bid.outbids(&token.best_bid) {
                            // `bid` outbids the old `Bid`: refund the old `Bid`
                            token.best_bid.replace(bid)
                        } else {
                            // `bid` does not outbid the old `Bid`: refund `bid`
                            Some(bid)
                        };

                        if let Some(refund_bid) = refund_bid {
                            balances[refund_bid.bidder as usize] += refund_bid.offer;
                        }
                    }
                    Request::Take { taker, token } => {
                        let token = if let Some(token) = tokens.get_mut(token as usize) {
                            token
                        } else {
                            // `Token` does not exist
                            continue;
                        };

                        if taker != token.owner {
                            // `taker` is not the owner of `token`
                            continue;
                        }

                        if let Some(best_bid) = token.best_bid.take() {
                            balances[token.owner as usize] += best_bid.offer;
                            token.owner = best_bid.bidder;
                        }
                    }
                }
            }

            // // Increment `operations_processed`

            operations_processed.fetch_add(batch_operation_count, Ordering::Relaxed);
        }
    }
}
