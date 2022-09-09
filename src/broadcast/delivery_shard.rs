use crate::broadcast::Amendment;

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::multi::Signature as MultiSignature;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DeliveryShard {
    pub height: u64,
    pub amendments: Vec<Amendment>,
    pub multisignature: MultiSignature,
}
