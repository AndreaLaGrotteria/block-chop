use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct Reduction {
    pub root: Hash,
}

impl Statement for Reduction {
    type Header = Header;
    const HEADER: Self::Header = Header::Reduction;
}
