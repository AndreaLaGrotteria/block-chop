use crate::{
    broadcast::{Amendment, Message},
    server::Batch,
};

use talk::sync::fuse::Fuse;

use tokio::sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender};

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

impl Deduplicator {
    pub fn new() -> Self {
        let (dispatch_inlet, _) = mpsc::channel(1);
        let (_, pop_outlet) = mpsc::channel(1);
        let _fuse = Fuse::new();

        Self {
            dispatch_inlet,
            pop_outlet,
            _fuse,
        }
    }
}
