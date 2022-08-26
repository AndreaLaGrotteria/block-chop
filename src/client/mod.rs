#[cfg(feature = "benchmark")]
pub mod load;

mod client;

pub use client::Client;
