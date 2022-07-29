use doomstack::{here, Doom, ResultExt, Top};

use sled::Db;

use std::path::Path;

use talk::crypto::{Identity, KeyChain};

pub struct Passepartout {
    database: Db,
}

#[derive(Doom)]
pub enum PassepartoutError {
    #[doom(description("Failed to open database: {:?}", source))]
    #[doom(wrap(open_failed))]
    OpenFailed { source: sled::Error },
    #[doom(description("Failed to load entry: {:?}", source))]
    #[doom(wrap(load_failed))]
    LoadFailed { source: sled::Error },
    #[doom(description("Entry not found"))]
    EntryNotFound,
    #[doom(description("Failed to deserialize entry: {:?}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: bincode::Error },
}

impl Passepartout {
    pub fn new<P>(path: P) -> Result<Self, Top<PassepartoutError>>
    where
        P: AsRef<Path>,
    {
        let database = sled::open(path)
            .map_err(PassepartoutError::open_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        Ok(Passepartout { database })
    }

    pub fn get(&self, identity: &Identity) -> Result<KeyChain, Top<PassepartoutError>> {
        let key = bincode::serialize(&identity).unwrap();

        let value = self
            .database
            .get(key)
            .map_err(PassepartoutError::load_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?
            .ok_or(PassepartoutError::EntryNotFound.into_top())
            .spot(here!())?;

        let keychain = bincode::deserialize::<KeyChain>(value.as_ref())
            .map_err(PassepartoutError::deserialize_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        Ok(keychain)
    }
}
