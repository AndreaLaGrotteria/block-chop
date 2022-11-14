use crate::heartbeat::Entry;
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex, MutexGuard,
};

type EntryInlet = Sender<Entry>;
type EntryOutlet = Receiver<Entry>;

pub(in crate::heartbeat) struct Channel {
    inlet: Mutex<EntryInlet>,
    outlet: Mutex<EntryOutlet>,
}

impl Channel {
    pub fn new() -> Self {
        let (inlet, outlet) = mpsc::channel();

        let inlet = Mutex::new(inlet);
        let outlet = Mutex::new(outlet);

        Channel { inlet, outlet }
    }

    pub fn clone_inlet(&self) -> EntryInlet {
        self.inlet.lock().unwrap().clone()
    }

    pub fn lock_outlet(&self) -> MutexGuard<EntryOutlet> {
        self.outlet.lock().unwrap()
    }
}
