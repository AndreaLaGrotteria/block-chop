use crate::crypto::{records::Height as HeightRecord, Certificate};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::Hash;

use zebra::vector::Proof;

#[derive(Serialize, Deserialize)]
pub(crate) enum Response {
    Inclusion {
        id: u64,
        root: Hash,
        proof: Proof,
        raise: u64,
        height_record: HeightRecord,
    },
    Delivery {
        height: u64,
        root: Hash,
        certificate: Certificate,
        sequence: u64,
        proof: Proof,
    },
}
