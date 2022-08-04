use crate::{broadcast::Message, crypto::Header};

use serde::Serialize;

use talk::crypto::Statement;

#[derive(Serialize)]
pub(crate) struct Broadcast<'a> {
    pub sequence: &'a u64,
    pub message: &'a Message,
}

impl<'a> Statement for Broadcast<'a> {
    type Header = Header;
    const HEADER: Self::Header = Header::Broadcast;
}
