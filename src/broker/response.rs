use crate::crypto::{records::Height as HeightRecord, Certificate};
use serde::{Deserialize, Serialize};
use talk::crypto::primitives::hash::Hash;
use zebra::vector::Proof;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Response {
    Inclusion {
        id: u64,
        root: Hash,
        proof: Proof,
        raise: u64,
        top_record: Option<HeightRecord>,
    },
    Delivery {
        height: u64,
        root: Hash,
        certificate: Certificate,
        sequence: u64,
        proof: Proof,
    },
}
