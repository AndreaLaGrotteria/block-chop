use crate::{
    broadcast::{Batch as BroadcastBatch, Entry, PACKING},
    crypto::statements::{Broadcast as BroadcastStatement, Reduction as ReductionStatement},
    server::batches::PlainBatch,
    system::Directory,
};
use doomstack::{here, Doom, ResultExt, Top};
use talk::crypto::primitives::{hash::Hash, sign::Signature};
use zebra::vector::Vector;

#[derive(Clone)]
pub(crate) struct MerkleBatch {
    pub(in crate::server::batches) entries: Vector<Option<Entry>, PACKING>,
}

#[derive(Doom)]
pub(crate) enum MerkleBatchError {
    #[doom(description("Malformed ids (invalid `VarCram`)"))]
    MalformedIds,
    #[doom(description("Empty batch"))]
    EmptyBatch,
    #[doom(description("Length mismatch between ids and messages'"))]
    LengthMismatch,
    #[doom(description("Ids are unsorted"))]
    IdsUnsorted,
    #[doom(description("Id unknown (no `KeyCard` available)"))]
    IdUnknown,
    #[doom(description("Invalid straggler signature"))]
    InvalidStraggler,
    #[doom(description("Missing reduction multisignature"))]
    MissingReduction,
    #[doom(description("Invalid reduction multisignature"))]
    InvalidReduction,
}

impl MerkleBatch {
    pub fn expand_verified(
        directory: &Directory,
        broadcast_batch: &BroadcastBatch,
    ) -> Result<Self, Top<MerkleBatchError>> {
        // Extract ids and messages

        let ids = broadcast_batch
            .ids
            .uncram()
            .ok_or(MerkleBatchError::MalformedIds.into_top())
            .spot(here!())?;

        if ids.is_empty() {
            return MerkleBatchError::EmptyBatch.fail().spot(here!());
        }

        let messages = &broadcast_batch.messages;

        if messages.len() != ids.len() {
            return MerkleBatchError::LengthMismatch.fail().spot(here!());
        }

        // Verify that ids are strictly increasing

        ids.windows(2) // TODO: Replace with `Iterator::is_sorted` when in Rust stable
            .map(|window| {
                if window[0] < window[1] {
                    Ok(())
                } else {
                    return MerkleBatchError::IdsUnsorted.fail().spot(here!());
                }
            })
            .collect::<Result<_, _>>()?;

        // Separate stragglers from reducers

        let mut stragglers = broadcast_batch.stragglers.iter().peekable();

        let mut reducer_multi_public_keys = Vec::with_capacity(ids.len());

        let mut straggler_public_keys = Vec::with_capacity(stragglers.len());
        let mut straggler_statements = Vec::with_capacity(stragglers.len());
        let mut straggler_signatures = Vec::with_capacity(stragglers.len());

        let mut straggler_entries = Vec::with_capacity(stragglers.len());

        for (index, (id, message)) in ids.iter().copied().zip(messages.iter()).enumerate() {
            if let Some(straggler) = stragglers.peek().filter(|straggler| straggler.id == id) {
                let public_key = directory
                    .get_public_key(id)
                    .ok_or_else(|| MerkleBatchError::IdUnknown.into_top().spot(here!()))?;

                let statement = BroadcastStatement {
                    sequence: &straggler.sequence,
                    message,
                };

                straggler_public_keys.push(public_key);
                straggler_statements.push(statement);
                straggler_signatures.push(&straggler.signature);

                straggler_entries.push((
                    index,
                    Some(Entry {
                        id,
                        sequence: straggler.sequence,
                        message: message.clone(),
                    }),
                ));

                stragglers.next();
            } else {
                let multi_public_key = directory
                    .get_multi_public_key(id)
                    .ok_or_else(|| MerkleBatchError::IdUnknown.into_top().spot(here!()))?;

                reducer_multi_public_keys.push(multi_public_key.clone());
            }
        }

        // Verify straggler `Signature`s

        Signature::batch_verify(
            straggler_public_keys,
            straggler_statements.iter(),
            straggler_signatures,
        )
        .pot(MerkleBatchError::InvalidStraggler, here!())?;

        // Build `Entry` Merkle tree

        let raise = broadcast_batch.raise;

        let entries = ids
            .into_iter()
            .zip(messages.iter().cloned())
            .map(|(id, message)| {
                Some(Entry {
                    id,
                    sequence: raise,
                    message,
                })
            })
            .collect::<Vec<_>>();

        let mut entries = Vector::<_, PACKING>::new(entries).unwrap();

        // Verify reducers' `MultiSignature`

        if !reducer_multi_public_keys.is_empty() {
            let multisignature = broadcast_batch
                .multisignature
                .ok_or(MerkleBatchError::MissingReduction.into_top())
                .spot(here!())?;

            let statement = ReductionStatement {
                root: &entries.root(),
            };

            multisignature
                .verify(reducer_multi_public_keys.iter(), &statement)
                .pot(MerkleBatchError::InvalidReduction, here!())?;
        }

        // Update straggler `entries`

        for (index, entry) in straggler_entries {
            entries.set(index, entry).unwrap();
        }

        Ok(MerkleBatch { entries })
    }

    pub fn expand_unverified(
        broadcast_batch: &BroadcastBatch,
    ) -> Result<Self, Top<MerkleBatchError>> {
        // Extract ids and messages

        let ids = broadcast_batch
            .ids
            .uncram()
            .ok_or(MerkleBatchError::MalformedIds.into_top())
            .spot(here!())?;

        let messages = &broadcast_batch.messages;
        let raise = broadcast_batch.raise;

        let mut stragglers = broadcast_batch.stragglers.iter().peekable();

        // Build `Entry` Merkle tree

        let entries = ids
            .into_iter()
            .zip(messages.iter().cloned())
            .map(|(id, message)| {
                let sequence = if stragglers
                    .peek()
                    .filter(|straggler| straggler.id == id)
                    .is_some()
                {
                    stragglers.next().unwrap().sequence
                } else {
                    raise
                };

                Some(Entry {
                    id,
                    sequence,
                    message,
                })
            })
            .collect::<Vec<_>>();

        let entries = Vector::<_, PACKING>::new(entries).unwrap();

        Ok(MerkleBatch { entries })
    }

    pub fn from_plain(plain_batch: &PlainBatch) -> Result<Self, Top<MerkleBatchError>> {
        if plain_batch.entries.is_empty() {
            return MerkleBatchError::EmptyBatch.fail().spot(here!());
        }

        Ok(MerkleBatch {
            entries: Vector::new(plain_batch.entries.clone()).unwrap(),
        })
    }

    pub fn root(&self) -> Hash {
        self.entries.root()
    }

    pub fn entries_mut(&mut self) -> &mut Vector<Option<Entry>, PACKING> {
        &mut self.entries
    }

    pub fn unwrap(self) -> Vec<Option<Entry>> {
        self.entries.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    impl MerkleBatch {
        pub(crate) fn from_entries(entries: Vector<Option<Entry>, PACKING>) -> Self {
            let mut sequence_histogram: HashMap<u64, usize> = HashMap::new();

            for entry in entries.items() {
                if let Some(entry) = entry {
                    *sequence_histogram.entry(entry.sequence).or_default() += 1;
                }
            }

            let mut sequence_histogram = sequence_histogram.into_iter().collect::<Vec<_>>();
            sequence_histogram.sort_unstable_by_key(|(_, height)| *height);

            MerkleBatch { entries }
        }

        pub(crate) fn expanded_batch_entries(
            broadcast_batch: BroadcastBatch,
        ) -> Vector<Option<Entry>, PACKING> {
            let MerkleBatch { entries, .. } =
                MerkleBatch::expand_unverified(&broadcast_batch).unwrap();

            entries
        }

        pub fn entries(&self) -> &Vector<Option<Entry>, PACKING> {
            &self.entries
        }
    }
}
