use crate::heartbeat::Event;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Clone, Serialize, Deserialize)]
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
