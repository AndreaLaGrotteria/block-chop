use crate::{
    broadcast::Entry,
    server::batches::{CompressedBatch, Delta, MerkleBatch},
};
use talk::crypto::primitives::hash::Hash;

#[derive(Clone)]
pub(crate) struct PlainBatch {
    pub(in crate::server::batches) root: Option<Hash>,
    pub(in crate::server::batches) entries: Vec<Option<Entry>>,
    pub(in crate::server::batches) sequence_mode: u64,
}

impl PlainBatch {
    pub fn root(&self) -> Option<Hash> {
        self.root
    }

    pub fn entries(&self) -> &[Option<Entry>] {
        self.entries.as_slice()
    }

    pub fn entries_mut(&mut self) -> &mut [Option<Entry>] {
        self.root = None;
        self.entries.as_mut_slice()
    }

    pub fn unwrap(self) -> Vec<Option<Entry>> {
        self.entries
    }
}

impl From<MerkleBatch> for PlainBatch {
    fn from(merkle_batch: MerkleBatch) -> Self {
        PlainBatch {
            root: Some(merkle_batch.root()),
            entries: merkle_batch.entries.into(),
            sequence_mode: merkle_batch.sequence_mode,
        }
    }
}

impl From<CompressedBatch> for PlainBatch {
    fn from(compressed_batch: CompressedBatch) -> Self {
        let CompressedBatch {
            root,
            ids,
            sequence_mode,
            messages,
            deltas,
        } = compressed_batch;

        let ids = ids.uncram().unwrap();

        let mut entries = ids
            .into_iter()
            .zip(messages)
            .map(|(id, message)| {
                Some(Entry {
                    id,
                    sequence: sequence_mode,
                    message,
                })
            })
            .collect::<Vec<_>>();

        for delta in deltas {
            match delta {
                Delta::Nudge { index, sequence } => {
                    entries.get_mut(index).unwrap().as_mut().unwrap().sequence = sequence;
                }
                Delta::Drop { index } => {
                    *entries.get_mut(index).unwrap() = None;
                }
            }
        }

        PlainBatch {
            root,
            entries,
            sequence_mode,
        }
    }
}
