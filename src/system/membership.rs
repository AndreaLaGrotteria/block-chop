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
}
