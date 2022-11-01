use crate::heartbeat::Entry;
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Arc, Mutex,
};

type EntryInlet = Sender<Entry>;
type EntryOutlet = Receiver<Entry>;

pub(in crate::heartbeat) struct Holder {
    channel: Mutex<Channel>,
}

struct Channel {
    inlet: EntryInlet,
    outlet: Arc<Mutex<EntryOutlet>>,
}

impl Holder {
    pub fn new() -> Self {
        let (inlet, outlet) = mpsc::channel();
        let outlet = Arc::new(Mutex::new(outlet));

        let channel = Channel { inlet, outlet };
        let channel = Mutex::new(channel);

        Holder { channel }
    }

    pub fn get_inlet(&self) -> EntryInlet {
        let channel = self.channel.lock().unwrap();
        channel.inlet.clone()
    }

    pub fn get_outlet(&self) -> Arc<Mutex<EntryOutlet>> {
        let channel = self.channel.lock().unwrap();
        channel.outlet.clone()
    }
}
