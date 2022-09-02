use crate::{
    broadcast::{Amendment, Message},
    server::Batch,
};

use talk::sync::fuse::Fuse;

use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

type BatchInlet = MpscSender<Batch>;
type AmendedBatchOutlet = MpscReceiver<(Batch, Vec<Amendment>)>;

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

impl Deduplicator {}
