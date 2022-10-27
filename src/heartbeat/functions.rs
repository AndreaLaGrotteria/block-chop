use crate::heartbeat::{Event, HOLDER};
use std::sync::mpsc::Sender;

type EventInlet = Sender<Event>;

thread_local! {
    static EVENT_INLET: EventInlet = HOLDER.get_inlet();
}

pub(crate) fn log(event: Event) {
    EVENT_INLET.with(|event_inlet| event_inlet.send(event).unwrap());
}

pub fn flush() -> Vec<Event> {
    let outlet = HOLDER.get_outlet();
    let outlet = outlet.lock().unwrap();

    let mut events = Vec::new();

    while let Ok(event) = outlet.try_recv() {
        events.push(event);
    }

    events
}
