use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct Delivery {
    pub height: u64,
    pub root: Hash,
}

impl Statement for Delivery {
    type Header = Header;
    const HEADER: Self::Header = Header::Delivery;
}
