use serde::{Deserialize, Serialize};
use talk::crypto::primitives::multi::Signature as MultiSignature;

use crate::broadcast::Amendment;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DeliveryShard {
    pub height: u64,
    pub amendments: Vec<Amendment>,
    pub multisignature: MultiSignature,
}
