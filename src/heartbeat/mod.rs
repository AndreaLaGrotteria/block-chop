mod channel;
mod entry;
mod event;
mod functions;

use channel::Channel;

pub(crate) use functions::log;

pub use entry::Entry;
pub use event::{BrokerEvent, Event, ServerEvent, ClientEvent};
pub use functions::flush;

lazy_static::lazy_static! {
    static ref CHANNEL: Channel = Channel::new();
}
