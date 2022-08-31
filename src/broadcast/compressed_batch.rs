use crate::broadcast::{Message, Straggler};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::multi::Signature as MultiSignature;

use varcram::VarCram;

#[derive(Serialize, Deserialize)]
pub(crate) struct CompressedBatch {
    pub ids: VarCram,
    pub messages: Vec<Message>,
    pub raise: u64,
    pub multisignature: Option<MultiSignature>,
    pub stragglers: Vec<Straggler>,
}
