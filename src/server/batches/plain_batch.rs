use crate::{
    broadcast::Entry,
    server::batches::{CompressedBatch, Delta, MerkleBatch},
};
use doomstack::{here, Doom, ResultExt, Top};
use talk::crypto::primitives::hash::Hash;

#[derive(Clone)]
pub(crate) struct PlainBatch {
    pub(in crate::server::batches) root: Option<Hash>,
    pub(in crate::server::batches) entries: Vec<Option<Entry>>,
    pub(in crate::server::batches) sequence_mode: u64,
}

#[derive(Doom)]
pub(crate) enum PlainBatchError {
    #[doom(description("Malformed ids (invalid `VarCram`)"))]
    MalformedIds,
    #[doom(description("`Delta` out of bound"))]
    DeltaOutOfBound,
    #[doom(description("`Delta` conflict (`Drop` then `Nudge`)"))]
    DeltaConflict,
}

impl PlainBatch {
    pub fn from_merkle(merkle_batch: &MerkleBatch) -> Self {
        let root = Some(merkle_batch.root());
        let entries = merkle_batch.entries.items().to_vec();
        let sequence_mode = merkle_batch.sequence_mode;

        PlainBatch {
            root,
            entries,
            sequence_mode,
        }
    }

    pub fn from_compressed(
        compressed_batch: &CompressedBatch,
    ) -> Result<Self, Top<PlainBatchError>> {
        let root = compressed_batch.root;
        let sequence_mode = compressed_batch.sequence_mode;

        let ids = compressed_batch
            .ids
            .uncram()
            .ok_or_else(|| PlainBatchError::MalformedIds.into_top())
            .spot(here!())?;

        let mut entries = ids
            .into_iter()
            .zip(compressed_batch.messages.iter().cloned())
            .map(|(id, message)| {
                Some(Entry {
                    id,
                    sequence: compressed_batch.sequence_mode,
                    message,
                })
            })
            .collect::<Vec<_>>();

        for delta in compressed_batch.deltas.iter().cloned() {
            match delta {
                Delta::Nudge { index, sequence } => {
                    let entry = entries
                        .get_mut(index)
                        .ok_or_else(|| PlainBatchError::DeltaOutOfBound.into_top())
                        .spot(here!())?;

                    let entry = entry
                        .as_mut()
                        .ok_or_else(|| PlainBatchError::DeltaConflict.into_top())
                        .spot(here!())?;

                    entry.sequence = sequence;
                }
                Delta::Drop { index } => {
                    let entry = entries
                        .get_mut(index)
                        .ok_or_else(|| PlainBatchError::DeltaOutOfBound.into_top())
                        .spot(here!())?;

                    *entry = None;
                }
            }
        }

        Ok(PlainBatch {
            root,
            entries,
            sequence_mode,
        })
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
