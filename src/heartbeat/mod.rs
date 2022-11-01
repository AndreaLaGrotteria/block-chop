mod entry;
mod event;
mod functions;
mod holder;

use holder::Holder;

pub(crate) use functions::log;

pub use entry::Entry;
pub use event::Event;
pub use functions::flush;

lazy_static::lazy_static! {
    static ref HOLDER: Holder = Holder::new();
}
