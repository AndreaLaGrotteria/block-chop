use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct BatchDelivery<'a> {
    pub height: &'a u64,
    pub root: &'a Hash,
}

impl<'a> Statement for BatchDelivery<'a> {
    type Header = Header;
    const HEADER: Self::Header = Header::BatchDelivery;
}
