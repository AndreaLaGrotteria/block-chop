use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{primitives::hash::Hash, Statement};

#[derive(Serialize)]
pub(crate) struct BatchWitness {
    root: Hash,
}

impl BatchWitness {
    pub fn new(root: Hash) -> Self {
        BatchWitness { root }
    }
}

impl Statement for BatchWitness {
    type Header = Header;
    const HEADER: Header = Header::BatchWitness;
}
