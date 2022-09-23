use crate::crypto::Header;
use serde::Serialize;
use talk::crypto::{primitives::hash::Hash, Identity, Statement};

#[derive(Serialize)]
pub(crate) struct BatchWitness<'a> {
    pub broker: &'a Identity,
    pub root: &'a Hash,
    pub sequence: &'a u64,
}

impl<'a> Statement for BatchWitness<'a> {
    type Header = Header;
    const HEADER: Header = Header::BatchWitness;
}
