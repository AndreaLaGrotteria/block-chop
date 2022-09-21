use crate::crypto::{records::Height as HeightRecord, Header};
use serde::Serialize;
use talk::crypto::Statement;

#[derive(Serialize)]
pub(crate) struct BroadcastAuthentication<'a> {
    pub height_record: &'a HeightRecord,
}

impl<'a> Statement for BroadcastAuthentication<'a> {
    type Header = Header;
    const HEADER: Self::Header = Header::BroadcastAuthentication;
}
