use crate::heartbeat::{Entry, Event, CHANNEL};
use std::sync::mpsc::Sender;

type EntryInlet = Sender<Entry>;

thread_local! {
    static ENTRY_INLET: EntryInlet = CHANNEL.clone_inlet();
}

pub(crate) fn log(event: Event) {
    ENTRY_INLET.with(|entry_inlet| entry_inlet.send(Entry::now(event)).unwrap());
}

pub fn flush() -> Vec<Entry> {
    let outlet = CHANNEL.lock_outlet();

    let mut entries = Vec::new();

    while let Ok(entry) = outlet.try_recv() {
        entries.push(entry);
    }

    entries
}
