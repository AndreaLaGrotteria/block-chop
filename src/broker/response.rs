use crate::crypto::records::Height as HeightRecord;

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
}
