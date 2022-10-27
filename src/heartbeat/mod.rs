mod event;
mod holder;

use holder::Holder;

pub use event::Event;

static HOLDER: Holder = Holder::new();
