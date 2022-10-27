use crate::heartbeat::Event;
use std::time::SystemTime;

pub struct Entry {
    pub time: SystemTime,
    pub event: Event,
}

impl Entry {
    pub fn now(event: Event) -> Self {
        Entry {
            time: SystemTime::now(),
            event,
        }
    }
}
