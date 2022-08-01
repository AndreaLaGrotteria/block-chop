use crate::{broadcast::Message, crypto::Header};

use serde::Serialize;

use talk::crypto::Statement;

#[derive(Serialize)]
pub(crate) struct Broadcast {
    pub sequence: u64,
    pub message: Message,
}

impl Statement for Broadcast {
    type Header = Header;
    const HEADER: Self::Header = Header::Broadcast;
}
