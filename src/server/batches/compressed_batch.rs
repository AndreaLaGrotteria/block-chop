use crate::{
    broadcast::{Entry, Message},
    server::batches::PlainBatch,
};
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
pub(in crate::server::batches) enum Delta {
    Nudge { index: usize, sequence: u64 },
    Drop { index: usize },
}

impl CompressedBatch {
    pub fn root(&self) -> Option<Hash> {
        self.root
    }

    pub fn from_plain(plain_batch: &PlainBatch) -> Self {
        let root = plain_batch.root;
        let sequence_mode = plain_batch.sequence_mode;

        let mut ids = Vec::with_capacity(plain_batch.entries.len());
        let mut messages = Vec::with_capacity(plain_batch.entries.len());
        let mut deltas = Vec::new();

        for (index, entry) in plain_batch.entries.iter().cloned().enumerate() {
            if let Some(Entry {
                id,
                sequence,
                message,
            }) = entry
            {
                ids.push(id);
                messages.push(message);

                if sequence != plain_batch.sequence_mode {
                    deltas.push(Delta::Nudge { index, sequence });
                }
            } else {
                ids.push(Default::default());
                messages.push(Default::default());
                deltas.push(Delta::Drop { index });
            }
        }

        let ids = VarCram::cram(ids.as_slice());

        CompressedBatch {
            root,
            ids,
            sequence_mode,
            messages,
            deltas,
        }
    }
}
