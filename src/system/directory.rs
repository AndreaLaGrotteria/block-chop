use std::path::Path;

use doomstack::{here, Doom, ResultExt, Top};

use talk::crypto::KeyCard;

pub struct Directory {
    keycards: Vec<Option<KeyCard>>,
}

#[derive(Doom)]
pub enum DirectoryError {
    #[doom(description("Failed to open database: {:?}", source))]
    #[doom(wrap(open_failed))]
    OpenFailed { source: sled::Error },
    #[doom(description("Failed to load entry: {:?}", source))]
    #[doom(wrap(load_failed))]
    LoadFailed { source: sled::Error },
    #[doom(description("Failed to deserialize entry: {:?}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: bincode::Error },
    #[doom(description("Failed to clear database: {:?}", source))]
    #[doom(wrap(clear_failed))]
    ClearFailed { source: sled::Error },
    #[doom(description("Failed to save entry: {:?}", source))]
    #[doom(wrap(save_failed))]
    SaveFailed { source: sled::Error },
    #[doom(description("Failed to flush database: {:?}", source))]
    #[doom(wrap(flush_failed))]
    FlushFailed { source: sled::Error },
}

impl Directory {
    pub fn new() -> Self {
        Directory::from_keycards(Vec::new())
    }

    pub fn load<P>(path: P) -> Result<Self, Top<DirectoryError>>
    where
        P: AsRef<Path>,
    {
        let database = sled::open(path)
            .map_err(DirectoryError::open_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        let capacity = if let Some(entry) = database.iter().next_back() {
            let (key, _) = entry
                .map_err(DirectoryError::load_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            let index = bincode::deserialize::<u64>(key.as_ref())
                .map_err(DirectoryError::deserialize_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            (index as usize) + 1
        } else {
            0
        };

        let mut keycards = vec![None; capacity];

        for entry in database.iter() {
            let (key, value) = entry
                .map_err(DirectoryError::load_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            let index = bincode::deserialize::<u64>(key.as_ref())
                .map_err(DirectoryError::deserialize_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            let keycard = bincode::deserialize::<KeyCard>(value.as_ref())
                .map_err(DirectoryError::deserialize_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            *keycards.get_mut(index as usize).unwrap() = Some(keycard);
        }

        Ok(Directory { keycards })
    }

    pub(crate) fn from_keycards(keycards: Vec<Option<KeyCard>>) -> Self {
        Directory { keycards }
    }

    pub fn capacity(&self) -> usize {
        self.keycards.len()
    }

    pub fn get(&self, id: u64) -> Option<&KeyCard> {
        self.keycards.get(id as usize).map(Option::as_ref).flatten()
    }

    pub fn insert(&mut self, id: u64, keycard: KeyCard) {
        if self.keycards.len() <= (id as usize) {
            self.keycards.resize((id as usize) + 1, None);
        }

        *self.keycards.get_mut(id as usize).unwrap() = Some(keycard);
    }
}
