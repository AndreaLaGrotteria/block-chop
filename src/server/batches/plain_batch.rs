use crate::{broadcast::Entry, server::batches::CompressedBatch};
use doomstack::{here, Doom, ResultExt, Top};
use talk::crypto::primitives::hash::Hash;

#[derive(Clone)]
pub(crate) struct PlainBatch {
    pub(in crate::server::batches) root: Option<Hash>,
    pub(in crate::server::batches) entries: Vec<Option<Entry>>,
}

#[derive(Doom)]
pub(crate) enum PlainBatchError {
    #[doom(description("Malformed ids (invalid `VarCram`)"))]
    MalformedIds,
}

impl PlainBatch {
    pub fn from_compressed(
        compressed_batch: &CompressedBatch,
    ) -> Result<Self, Top<PlainBatchError>> {
        let root = compressed_batch.root;

        let ids = compressed_batch
            .ids
            .uncram()
            .ok_or_else(|| PlainBatchError::MalformedIds.into_top())
            .spot(here!())?;

        let mut deltas = compressed_batch.deltas.iter().peekable();

        let entries = ids
            .into_iter()
            .zip(compressed_batch.messages.iter().cloned())
            .map(|(id, message)| {
                let sequence = if let Some(delta) = deltas.peek().filter(|delta| delta.id == id) {
                    let sequence = delta.sequence;
                    deltas.next().unwrap();

                    sequence
                } else {
                    compressed_batch.sequence_mode
                };

                Some(Entry {
                    id,
                    sequence,
                    message,
                })
            })
            .collect::<Vec<_>>();

        Ok(PlainBatch { root, entries })
    }

    pub fn root(&self) -> Option<Hash> {
        self.root
    }

    pub fn entries(&self) -> &[Option<Entry>] {
        self.entries.as_slice()
    }

    pub fn unwrap(self) -> Vec<Option<Entry>> {
        self.entries
    }
}
