mod event;
mod functions;
mod holder;

use holder::Holder;

pub(crate) use functions::log;

pub use event::Event;
pub use functions::flush;

static HOLDER: Holder = Holder::new();
