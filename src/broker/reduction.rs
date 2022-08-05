use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature};

#[derive(Clone)]
pub(in crate::broker) struct Reduction {
    pub root: Hash,
    pub id: u64,
    pub multisignature: MultiSignature,
}
