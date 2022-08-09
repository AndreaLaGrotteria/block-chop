use crate::broadcast::{Message, Straggler};

use talk::crypto::primitives::multi::Signature as MultiSignature;

use varcram::VarCram;

pub(crate) struct CompressedBatch {
    pub ids: VarCram,
    pub messages: Vec<Message>,
    pub raise: u64,
    pub multisignature: MultiSignature,
    pub stragglers: Vec<Straggler>,
}
