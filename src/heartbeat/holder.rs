use crate::heartbeat::Event;
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Arc, Mutex,
};

type EventInlet = Sender<Event>;
type EventOutlet = Receiver<Event>;

pub(in crate::heartbeat) struct Holder {
    channel: Mutex<Option<Channel>>,
}

struct Channel {
    inlet: EventInlet,
    outlet: Arc<Mutex<EventOutlet>>,
}

impl Holder {
    pub const fn new() -> Self {
        Holder {
            channel: Mutex::new(None),
        }
    }

    pub fn get_inlet(&self) -> EventInlet {
        let mut channel = self.channel.lock().unwrap();
        Holder::fill(&mut channel);
        channel.as_ref().unwrap().inlet.clone()
    }

    pub fn get_outlet(&self) -> Arc<Mutex<EventOutlet>> {
        let mut channel = self.channel.lock().unwrap();
        Holder::fill(&mut channel);
        channel.as_ref().unwrap().outlet.clone()
    }

    fn fill(channel: &mut Option<Channel>) {
        if channel.is_none() {
            let (inlet, outlet) = mpsc::channel();
            let outlet = Arc::new(Mutex::new(outlet));

            *channel = Some(Channel { inlet, outlet })
        }
    }
}
