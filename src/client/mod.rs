#[cfg(feature = "benchmark")]
mod load;

mod client;

pub use client::Client;

#[cfg(feature = "benchmark")]
pub use load::load;
