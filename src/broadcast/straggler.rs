use serde::{Deserialize, Serialize};

use talk::crypto::primitives::sign::Signature;

#[derive(Serialize, Deserialize)]
pub(crate) struct Straggler {
    pub id: u64,
    pub sequence: u64,
    pub signature: Signature,
}
