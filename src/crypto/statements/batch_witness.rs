use crate::crypto::Header;
use serde::Serialize;
use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct BatchWitness<'a> {
    pub root: &'a Hash,
}

impl<'a> Statement for BatchWitness<'a> {
    type Header = Header;
    const HEADER: Header = Header::BatchWitness;
}
