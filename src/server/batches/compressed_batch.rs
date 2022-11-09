use crate::broadcast::{Batch as BroadcastBatch, Message};
use serde::{Deserialize, Serialize};
use talk::crypto::primitives::hash::Hash;
use varcram::VarCram;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CompressedBatch {
    #[serde(skip)]
    pub(in crate::server::batches) root: Option<Hash>,
    pub(in crate::server::batches) ids: VarCram,
    pub(in crate::server::batches) sequence_mode: u64,
    pub(in crate::server::batches) messages: Vec<Message>,
    pub(in crate::server::batches) deltas: Vec<Delta>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(in crate::server::batches) struct Delta {
    pub(in crate::server::batches) id: u64,
    pub(in crate::server::batches) sequence: u64,
}

impl CompressedBatch {
    pub(in crate::server) fn from_broadcast(root: Hash, broadcast_batch: BroadcastBatch) -> Self {
        let deltas = broadcast_batch
            .stragglers
            .into_iter()
            .map(|straggler| Delta {
                id: straggler.id,
                sequence: straggler.sequence,
            })
            .collect::<Vec<_>>();

        CompressedBatch {
            root: Some(root),
            ids: broadcast_batch.ids,
            sequence_mode: broadcast_batch.raise,
            messages: broadcast_batch.messages,
            deltas,
        }
    }

    pub fn root(&self) -> Option<Hash> {
        self.root
    }
}
