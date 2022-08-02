use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct BatchDelivery {
    pub height: u64,
    pub root: Hash,
}

impl Statement for BatchDelivery {
    type Header = Header;
    const HEADER: Self::Header = Header::BatchDelivery;
}
