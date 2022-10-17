use crate::{
    broadcast::{CompressedBatch, Entry, PACKING},
    crypto::statements::{Broadcast as BroadcastStatement, Reduction as ReductionStatement},
    system::Directory,
};
use doomstack::{here, Doom, ResultExt, Top};
use talk::crypto::primitives::{hash::Hash, sign::Signature};
use zebra::vector::Vector;

#[derive(Clone)]
pub(crate) struct Batch {
    pub entries: Vector<Option<Entry>, PACKING>,
}

#[derive(Doom)]
pub(crate) enum BatchError {
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

impl Batch {
    pub fn expand_verified(
        directory: &Directory,
        compressed_batch: CompressedBatch,
    ) -> Result<Self, Top<BatchError>> {
        // Extract ids and messages

        let ids = compressed_batch
            .ids
            .uncram()
            .ok_or(BatchError::MalformedIds.into_top())
            .spot(here!())?;

        if ids.is_empty() {
            return BatchError::EmptyBatch.fail().spot(here!());
        }

        let messages = compressed_batch.messages;

        if messages.len() != ids.len() {
            return BatchError::LengthMismatch.fail().spot(here!());
        }

        // Verify that ids are strictly increasing

        ids.windows(2) // TODO: Replace with `Iterator::is_sorted` when in Rust stable
            .map(|window| {
                if window[0] < window[1] {
                    Ok(())
                } else {
                    return BatchError::IdsUnsorted.fail().spot(here!());
                }
            })
            .collect::<Result<_, _>>()?;

        // Separate stragglers from reducers

        let mut stragglers = compressed_batch.stragglers.iter().peekable();

        let mut reducer_multi_public_keys = Vec::with_capacity(ids.len());

        let mut straggler_public_keys = Vec::with_capacity(stragglers.len());
        let mut straggler_statements = Vec::with_capacity(stragglers.len());
        let mut straggler_signatures = Vec::with_capacity(stragglers.len());

        let mut straggler_entries = Vec::with_capacity(stragglers.len());

        for (index, (id, message)) in ids.iter().copied().zip(messages.iter()).enumerate() {
            if let Some(straggler) = stragglers.peek().filter(|straggler| straggler.id == id) {
                let public_key = directory
                    .get_public_key(id)
                    .ok_or_else(|| BatchError::IdUnknown.into_top().spot(here!()))?;

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
                    .ok_or_else(|| BatchError::IdUnknown.into_top().spot(here!()))?;

                reducer_multi_public_keys.push(multi_public_key.clone());
            }
        }

        // Verify straggler `Signature`s

        Signature::batch_verify(
            straggler_public_keys,
            straggler_statements.iter(),
            straggler_signatures,
        )
        .pot(BatchError::InvalidStraggler, here!())?;

        // Build `Entry` Merkle tree

        let raise = compressed_batch.raise;

        let entries = ids
            .into_iter()
            .zip(messages)
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
            let multisignature = compressed_batch
                .multisignature
                .ok_or(BatchError::MissingReduction.into_top())
                .spot(here!())?;

            let statement = ReductionStatement {
                root: &entries.root(),
            };

            multisignature
                .verify(reducer_multi_public_keys.iter(), &statement)
                .pot(BatchError::InvalidReduction, here!())?;
        }

        // Update straggler `entries`

        for (index, entry) in straggler_entries {
            entries.set(index, entry).unwrap();
        }

        Ok(Batch { entries })
    }

    pub fn expand_unverified(compressed_batch: CompressedBatch) -> Result<Self, Top<BatchError>> {
        // Extract ids and messages

        let ids = compressed_batch
            .ids
            .uncram()
            .ok_or(BatchError::MalformedIds.into_top())
            .spot(here!())?;

        let messages = compressed_batch.messages;
        let raise = compressed_batch.raise;

        let mut stragglers = compressed_batch.stragglers.iter().peekable();

        // Build `Entry` Merkle tree

        let entries = ids
            .into_iter()
            .zip(messages)
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

        Ok(Batch { entries })
    }

    pub fn root(&self) -> Hash {
        self.entries.root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Batch {
        pub(crate) fn expanded_batch_entries(
            compressed_batch: CompressedBatch,
        ) -> Vector<Option<Entry>, PACKING> {
            let Batch { entries } = Batch::expand_unverified(compressed_batch).unwrap();
            entries
        }
    }
}
