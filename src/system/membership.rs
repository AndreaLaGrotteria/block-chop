use doomstack::{here, Doom, ResultExt, Top};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha12Rng;
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};
use talk::crypto::{primitives::hash::Hash, Identity, KeyCard};

#[derive(Clone)]
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
    #[doom(description("Insufficient number of servers in database: {:?}"))]
    LoadExactFailed,
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

    #[cfg(feature = "benchmark")]
    pub fn load_exact<P>(path: P, size: usize) -> Result<Self, Top<MembershipError>>
    where
        P: AsRef<Path>,
    {
        let full_membership = Self::load(path)?;

        if full_membership.servers().len() < size {
            return MembershipError::LoadExactFailed.fail();
        }

        Ok(full_membership.truncate(size))
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

    pub fn random_subset(&self, seed: Hash, size: usize) -> HashSet<Identity> {
        let mut servers = self.servers().keys().copied().collect::<Vec<_>>();
        servers.shuffle(&mut ChaCha12Rng::from_seed(seed.to_bytes()));
        servers.into_iter().take(size).collect()
    }

    pub fn random_plurality(&self, seed: Hash) -> HashSet<Identity> {
        self.random_subset(seed, self.plurality())
    }

    pub fn random_quorum(&self, seed: Hash) -> HashSet<Identity> {
        self.random_subset(seed, self.quorum())
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

    #[cfg(feature = "benchmark")]
    pub fn truncate(self, size: usize) -> Membership {
        let servers = self
            .servers
            .into_iter()
            .take(size)
            .collect::<BTreeMap<_, _>>();

        Membership { servers }
    }
}
