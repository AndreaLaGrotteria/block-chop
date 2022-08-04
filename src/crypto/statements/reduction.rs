use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct Reduction<'a> {
    pub root: &'a Hash,
}

impl<'a> Statement for Reduction<'a> {
    type Header = Header;
    const HEADER: Self::Header = Header::Reduction;
}
