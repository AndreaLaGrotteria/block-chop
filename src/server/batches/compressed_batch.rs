use crate::{
    broadcast::{Entry, Message},
    server::batches::PlainBatch,
};
use talk::crypto::primitives::hash::Hash;
use varcram::VarCram;

#[derive(Clone)]
pub(crate) struct CompressedBatch {
    pub(in crate::server::batches) root: Option<Hash>,
    pub(in crate::server::batches) ids: VarCram,
    pub(in crate::server::batches) sequence_mode: u64,
    pub(in crate::server::batches) messages: Vec<Message>,
    pub(in crate::server::batches) deltas: Vec<Delta>,
}

#[derive(Clone)]
pub(in crate::server::batches) enum Delta {
    Nudge { index: usize, sequence: u64 },
    Drop { index: usize },
}

impl CompressedBatch {
    pub fn root(&self) -> Option<Hash> {
        self.root
    }
}

impl From<PlainBatch> for CompressedBatch {
    fn from(plain_batch: PlainBatch) -> Self {
        let PlainBatch {
            root,
            entries,
            sequence_mode,
        } = plain_batch;

        let mut ids = Vec::with_capacity(entries.len());
        let mut messages = Vec::with_capacity(entries.len());
        let mut deltas = Vec::new();

        for (index, entry) in entries.into_iter().enumerate() {
            if let Some(Entry {
                id,
                sequence,
                message,
            }) = entry
            {
                ids.push(id);
                messages.push(message);

                if sequence != sequence_mode {
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
