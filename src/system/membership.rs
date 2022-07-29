use doomstack::{here, Doom, ResultExt, Top};

use std::{collections::BTreeMap, path::Path};

use talk::crypto::{Identity, KeyCard};

pub struct Membership {
    servers: BTreeMap<Identity, KeyCard>,
}

#[derive(Doom)]
pub enum MembershipError {
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

impl Membership {
    pub fn new<K>(servers: K) -> Self
    where
        K: IntoIterator<Item = KeyCard>,
    {
        let servers = servers
            .into_iter()
            .map(|keycard| (keycard.identity(), keycard))
            .collect::<BTreeMap<_, _>>();

        Membership { servers }
    }

    pub fn load<P>(path: P) -> Result<Self, Top<MembershipError>>
    where
        P: AsRef<Path>,
    {
        let database = sled::open(path)
            .map_err(MembershipError::open_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        let servers = database
            .iter()
            .map(|entry| {
                let (key, value) = entry
                    .map_err(MembershipError::load_failed)
                    .map_err(MembershipError::into_top)
                    .spot(here!())?;

                let identity = bincode::deserialize::<Identity>(key.as_ref())
                    .map_err(MembershipError::deserialize_failed)
                    .map_err(MembershipError::into_top)
                    .spot(here!())?;

                let keycard = bincode::deserialize::<KeyCard>(value.as_ref())
                    .map_err(MembershipError::deserialize_failed)
                    .map_err(MembershipError::into_top)
                    .spot(here!())?;

                Ok((identity, keycard))
            })
            .collect::<Result<BTreeMap<_, _>, Top<MembershipError>>>()?;

        Ok(Membership { servers })
    }

    pub fn servers(&self) -> &BTreeMap<Identity, KeyCard> {
        &self.servers
    }

    pub fn plurality(&self) -> usize {
        (self.servers.len() - 1) / 3 + 1
    }

    pub fn quorum(&self) -> usize {
        self.servers.len() - self.plurality() + 1
    }

    pub fn save<P>(&self, path: P) -> Result<(), Top<MembershipError>>
    where
        P: AsRef<Path>,
    {
        let database = sled::open(path)
            .map_err(MembershipError::open_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        database
            .clear()
            .map_err(MembershipError::clear_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        for (identity, keycard) in self.servers.iter() {
            let key = bincode::serialize(identity).unwrap();
            let value = bincode::serialize(keycard).unwrap();

            database
                .insert(key, value)
                .map_err(MembershipError::save_failed)
                .map_err(MembershipError::into_top)
                .spot(here!())?;
        }

        database
            .flush()
            .map_err(MembershipError::flush_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        Ok(())
    }
}
