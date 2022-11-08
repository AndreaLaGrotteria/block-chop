use crate::{
    broadcast::{Entry, PACKING},
    server::batches::{MerkleBatch, PlainBatch},
};
use doomstack::{here, Doom, ResultExt, Top};
use talk::crypto::primitives::hash::Hash;
use zebra::vector::Vector;

pub(crate) struct InflatedBatch(BatchStore);

enum BatchStore {
    Merkle(MerkleBatch),
    Plain(PlainBatch),
}

#[derive(Doom)]
pub(crate) enum InflatedBatchError {
    #[doom(description("Root missing on `PlainBatch`"))]
    RootMissing,
    #[doom(description("Empty `PlainBatch`"))]
    EmptyBatch,
}

impl InflatedBatch {
    pub fn from_merkle(merkle_batch: MerkleBatch) -> Self {
        InflatedBatch(BatchStore::Merkle(merkle_batch))
    }

    pub fn from_plain(plain_batch: PlainBatch) -> Result<Self, Top<InflatedBatchError>> {
        if plain_batch.root.is_none() {
            return InflatedBatchError::RootMissing.fail().spot(here!());
        }

        if plain_batch.entries.is_empty() {
            return InflatedBatchError::EmptyBatch.fail().spot(here!());
        }

        Ok(InflatedBatch(BatchStore::Plain(plain_batch)))
    }

    pub fn root(&self) -> Hash {
        match &self.0 {
            BatchStore::Merkle(merkle_batch) => merkle_batch.root(),
            BatchStore::Plain(plain_batch) => plain_batch.root().unwrap(),
        }
    }

    pub fn entries(&self) -> &[Option<Entry>] {
        match &self.0 {
            BatchStore::Merkle(merkle_batch) => merkle_batch.entries.items(),
            BatchStore::Plain(plain_batch) => plain_batch.entries(),
        }
    }

    pub fn entries_mut(&mut self) -> &mut Vector<Option<Entry>, PACKING> {
        self.hydrate();

        match &mut self.0 {
            BatchStore::Merkle(merkle_batch) => merkle_batch.entries_mut(),
            _ => unreachable!(),
        }
    }

    fn hydrate(&mut self) {
        if let BatchStore::Plain(plain_batch) = &mut self.0 {
            let merkle_batch = MerkleBatch::from_plain(&plain_batch).unwrap();
            self.0 = BatchStore::Merkle(merkle_batch)
        }
    }
}
