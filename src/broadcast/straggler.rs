use serde::{Deserialize, Serialize};
use talk::crypto::primitives::sign::Signature;

#[derive(Clone, Serialize, Deserialize)]
pub struct Straggler {
    pub id: u64,
    pub sequence: u64,
    pub signature: Signature,
}
