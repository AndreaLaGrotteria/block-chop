use doomstack::{here, Doom, ResultExt, Top};

use std::path::Path;

use talk::crypto::KeyCard;

#[derive(Clone)]
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
    #[doom(description("Failed to deserialize id"))]
    DeserializeIdFailed,
    #[doom(description("Failed to deserialize keycard: {:?}", source))]
    #[doom(wrap(deserialize_keycard_failed))]
    DeserializeKeyCardFailed { source: bincode::Error },
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

            let id = if key.len() == 8 {
                let mut buffer = [0u8; 8];
                buffer.clone_from_slice(key.as_ref());
                u64::from_be_bytes(buffer)
            } else {
                return DirectoryError::DeserializeIdFailed.fail().spot(here!());
            };

            (id as usize) + 1
        } else {
            0
        };

        let mut keycards = vec![None; capacity];

        for entry in database.iter() {
            let (key, value) = entry
                .map_err(DirectoryError::load_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            let id = if key.len() == 8 {
                let mut buffer = [0u8; 8];
                buffer.clone_from_slice(key.as_ref());
                u64::from_be_bytes(buffer)
            } else {
                return DirectoryError::DeserializeIdFailed.fail().spot(here!());
            };

            let keycard = bincode::deserialize::<KeyCard>(value.as_ref())
                .map_err(DirectoryError::deserialize_keycard_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            *keycards.get_mut(id as usize).unwrap() = Some(keycard);
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

    pub fn save<P>(&self, path: P) -> Result<(), Top<DirectoryError>>
    where
        P: AsRef<Path>,
    {
        let database = sled::open(path)
            .map_err(DirectoryError::open_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        database
            .clear()
            .map_err(DirectoryError::clear_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        for (index, keycard) in self.keycards.iter().enumerate() {
            if let Some(keycard) = keycard {
                let key = (index as u64).to_be_bytes();
                let value = bincode::serialize(&keycard).unwrap();

                database
                    .insert(key, value)
                    .map_err(DirectoryError::save_failed)
                    .map_err(DirectoryError::into_top)
                    .spot(here!())?;
            }
        }

        database
            .flush()
            .map_err(DirectoryError::flush_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        Ok(())
    }
}
