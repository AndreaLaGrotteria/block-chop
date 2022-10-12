use doomstack::{here, Doom, ResultExt, Top};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha12Rng;
use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    path::Path,
};
use talk::crypto::{primitives::hash::Hash, Identity, KeyCard};

#[derive(Clone)]
pub struct Membership {
    servers: BTreeMap<Identity, KeyCard>,
}

#[derive(Doom)]
pub enum MembershipError {
    #[doom(description("Failed to open file: {:?}", source))]
    #[doom(wrap(open_failed))]
    OpenFailed { source: io::Error },
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Failed to serialize: {}", source))]
    #[doom(wrap(serialize_failed))]
    SerializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Failed to flush file: {:?}", source))]
    #[doom(wrap(flush_failed))]
    FlushFailed { source: io::Error },
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
        let file = File::open(path)
            .map_err(MembershipError::open_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        let mut file = BufReader::new(file);

        let servers = bincode::deserialize_from(&mut file)
            .map_err(MembershipError::deserialize_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

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
        let file = File::create(path)
            .map_err(MembershipError::open_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        let mut file = BufWriter::new(file);

        bincode::serialize_into(&mut file, &self.servers)
            .map_err(MembershipError::serialize_failed)
            .map_err(MembershipError::into_top)
            .spot(here!())?;

        file.flush()
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
