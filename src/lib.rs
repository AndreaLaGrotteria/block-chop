pub mod client;

mod broadcast;
mod broker;
mod crypto;
mod server;
mod system;
mod total_order;

#[cfg(not(test))]
pub(crate) use log::{debug, info, warn};
#[cfg(test)]
pub(crate) use std::{println as info, println as debug, println as warn};

pub use broadcast::{Entry, Message, MESSAGE_SIZE};
pub use broker::{Broker, BrokerSettings};
pub use client::Client;
pub use crypto::{DeliveryRecord, DeliveryRecordError};
pub use server::Server;
pub use system::{Directory, Membership};
pub use total_order::{Broadcast, LoopBack};

#[cfg(feature = "benchmark")]
pub use system::Passepartout;
