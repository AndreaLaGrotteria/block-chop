#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "benchmark")]
pub mod applications;
pub mod client;
#[cfg(feature = "benchmark")]
pub mod heartbeat;

mod broadcast;
mod broker;
mod crypto;
mod order;
mod server;
mod system;

#[cfg(not(test))]
pub(crate) use log::{debug, info, warn};

#[cfg(test)]
pub(crate) use std::{println as info, println as debug, println as warn};

pub use broadcast::{CompressedBatch, Entry, Message, Straggler, MESSAGE_SIZE};
pub use broker::{Broker, BrokerSettings};
pub use client::Client;
pub use crypto::{DeliveryRecord, DeliveryRecordError};
pub use order::{BftSmart, HotStuff, LoopBack, Order};
pub use server::{Server, ServerSettings};
pub use system::{Directory, Membership};

#[cfg(feature = "benchmark")]
pub use broker::{LoadBroker, LoadBrokerSettings};
#[cfg(feature = "benchmark")]
pub use system::Passepartout;
