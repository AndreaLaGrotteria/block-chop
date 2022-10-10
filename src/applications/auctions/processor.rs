use crate::applications::auctions::Request;
use std::sync::{atomic::AtomicU64, Arc};
use talk::sync::fuse::Fuse;
use tokio::{
    sync::{
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
        oneshot::{self, Sender as OneShotSender},
    },
    task::{self},
};

type BatchInlet = MpscSender<Vec<Request>>;
type BatchOutlet = MpscReceiver<Vec<Request>>;

pub struct Processor {
    process_inlet: BatchInlet,
    operations_processed: Arc<AtomicU64>,
    _fuse: Fuse,
}

impl Processor {}
