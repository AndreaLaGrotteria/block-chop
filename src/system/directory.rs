use doomstack::{here, Doom, ResultExt, Top};
#[cfg(feature = "benchmark")]
use memmap::{Mmap, MmapMut, MmapOptions};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::{
    fs,
    io::{self, BufReader, BufWriter},
    path::Path,
};
#[cfg(feature = "benchmark")]
use std::{
    fs::{File, OpenOptions},
    slice,
};
#[cfg(feature = "benchmark")]
use std::{
    io::{Read, Write},
    mem,
};
use talk::crypto::{
    primitives::{
        multi::{PublicKey as MultiPublicKey, Signer as MultiSigner},
        sign::{PublicKey, Signer},
    },
    Identity, KeyCard,
};

const BLOCK_SIZE: usize = 145;
const CHUNK_SIZE: usize = 16 * 1048576;

pub struct Directory {
    identities: Vec<Option<Identity>>,
    public_keys: Vec<Option<PublicKey>>,
    multi_public_keys: Vec<Option<MultiPublicKey>>,
    #[cfg(feature = "benchmark")]
    mmaps: Option<Mmaps>,
}

struct Mmaps {
    _identities: Mmap,
    _public_keys: Mmap,
    _multi_public_keys: Mmap,
}

#[derive(Doom)]
pub enum DirectoryError {
    #[doom(description("Failed to open file: {:?}", source))]
    #[doom(wrap(open_failed))]
    OpenFailed { source: io::Error },
    #[doom(description("Failed to obtain metadata: {:?}", source))]
    #[doom(wrap(metadata_unavailable))]
    MetadataUnavailable { source: io::Error },
    #[doom(description("Unexpected file size (not a multiple of `BLOCK_SIZE`)"))]
    UnexpectedFileSize,
    #[doom(description("Failed to read block: {:?}", source))]
    #[doom(wrap(read_failed))]
    ReadFailed { source: io::Error },
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Failed to write block: {:?}", source))]
    #[doom(wrap(write_failed))]
    WriteFailed { source: io::Error },
    #[doom(description("Failed to seek to block: {:?}", source))]
    #[doom(wrap(seek_failed))]
    SeekFailed { source: io::Error },
    #[doom(description("Failed to flush file: {:?}", source))]
    #[doom(wrap(flush_failed))]
    FlushFailed { source: io::Error },
}

impl Directory {
    pub fn new() -> Self {
        Directory::from_keycards(Vec::new())
    }

    pub(crate) fn from_keycards(keycards: Vec<Option<KeyCard>>) -> Self {
        let mut identities = Vec::with_capacity(keycards.len());
        let mut public_keys = Vec::with_capacity(keycards.len());
        let mut multi_public_keys = Vec::with_capacity(keycards.len());

        for keycard in keycards {
            let keycard = keycard.as_ref();

            identities.push(keycard.map(KeyCard::identity));
            public_keys.push(keycard.map(Signer::public_key).cloned());
            multi_public_keys.push(keycard.map(MultiSigner::public_key).cloned());
        }

        Directory {
            identities,
            public_keys,
            multi_public_keys,
            #[cfg(feature = "benchmark")]
            mmaps: None,
        }
    }

    pub fn load<P>(path: P) -> Result<Self, Top<DirectoryError>>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)
            .map_err(DirectoryError::open_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        let metadata = file
            .metadata()
            .map_err(DirectoryError::metadata_unavailable)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        if metadata.len() % (BLOCK_SIZE as u64) != 0 {
            return DirectoryError::UnexpectedFileSize.fail().spot(here!());
        }

        let mut keycards = Vec::with_capacity((metadata.len() / (BLOCK_SIZE as u64)) as usize);
        let mut file = BufReader::new(file);

        loop {
            // Load up to `CHUNK_SIZE` blocks from `file`

            let mut blocks = Vec::with_capacity(CHUNK_SIZE);

            for _ in 0..CHUNK_SIZE {
                let mut block = [0u8; BLOCK_SIZE];

                match file.read_exact(block.as_mut_slice()) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        if error.kind() == io::ErrorKind::UnexpectedEof {
                            // End of file is not actually unexpected: `file`' size
                            // is guaranteed to be a multiple of `BLOCK_SIZE`, but
                            // not a multiple of `BLOCK_SIZE * CHUNK_SIZE`
                            break;
                        } else {
                            Err(error)
                        }
                    }
                }
                .map_err(DirectoryError::read_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

                blocks.push(block);
            }

            if blocks.is_empty() {
                break;
            }

            // Deserialize `blocks` in parallel

            let mut chunk = blocks
                .into_par_iter()
                .map(|block| {
                    if block[0] == 0 {
                        bincode::deserialize::<KeyCard>(&block[1..]).map(|keycard| Some(keycard))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(DirectoryError::deserialize_failed)
                .map_err(DirectoryError::into_top)
                .spot(here!())?;

            // Flush `chunk` into `keycards`

            keycards.append(&mut chunk);
        }

        Ok(Directory::from_keycards(keycards))
    }

    #[cfg(feature = "benchmark")]
    pub unsafe fn load_raw<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        unsafe fn load_component<T>(root: &Path, file: &str) -> (Vec<Option<T>>, Mmap) {
            let mut path = root.to_path_buf();
            path.push(file);

            let mmap = MmapOptions::new()
                .map(&File::open(path.as_path()).unwrap())
                .unwrap();

            if mmap.len() % mem::size_of::<Option<T>>() != 0 {
                panic!("Called `Directory::load_raw` on non-aligned components");
            }

            let pointer = mmap.as_ptr() as *mut Option<T>;
            let capacity = mmap.len() / mem::size_of::<Option<T>>();
            let vector = Vec::from_raw_parts(pointer, capacity, capacity);

            (vector, mmap)
        }

        let (identities, identities_mmap) = load_component(path.as_ref(), "identities.raw");
        let (public_keys, public_keys_mmap) = load_component(path.as_ref(), "public_keys.raw");

        let (multi_public_keys, multi_public_keys_mmap) =
            load_component(path.as_ref(), "multi_public_keys.raw");

        if identities.len() != public_keys.len() || public_keys.len() != multi_public_keys.len() {
            panic!("Called `Directory::load_raw` on length-mismatched components");
        }

        Directory {
            identities,
            public_keys,
            multi_public_keys,
            mmaps: Some(Mmaps {
                _identities: identities_mmap,
                _public_keys: public_keys_mmap,
                _multi_public_keys: multi_public_keys_mmap,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.identities.len()
    }

    pub fn get_identity(&self, id: u64) -> Option<Identity> {
        self.identities
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
            .cloned()
    }

    pub fn get_public_key(&self, id: u64) -> Option<&PublicKey> {
        self.public_keys
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
    }

    pub fn get_multi_public_key(&self, id: u64) -> Option<&MultiPublicKey> {
        self.multi_public_keys
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
    }

    pub fn insert(&mut self, id: u64, keycard: KeyCard) {
        #[cfg(feature = "benchmark")]
        if self.mmaps.is_some() {
            panic!("Called `Directory::insert` on an `memmap`ed `Directory`");
        }

        if self.identities.len() <= (id as usize) {
            self.identities.resize((id as usize) + 1, None);
            self.public_keys.resize((id as usize) + 1, None);
            self.multi_public_keys.resize((id as usize) + 1, None);
        }

        *self.identities.get_mut(id as usize).unwrap() = Some(keycard.identity());

        *self.public_keys.get_mut(id as usize).unwrap() =
            Some(Signer::public_key(&keycard).clone());

        *self.multi_public_keys.get_mut(id as usize).unwrap() =
            Some(MultiSigner::public_key(&keycard).clone());
    }

    pub fn save<P>(&self, path: P) -> Result<(), Top<DirectoryError>>
    where
        P: AsRef<Path>,
    {
        let file = File::create(path)
            .map_err(DirectoryError::open_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        let mut file = BufWriter::new(file);

        for (public_key_chunk, multi_public_key_chunk) in self
            .public_keys
            .chunks(CHUNK_SIZE)
            .zip(self.multi_public_keys.chunks(CHUNK_SIZE))
        {
            // (In parallel) Assemble public keys into `KeyCard`s and serialize

            let blocks = public_key_chunk
                .into_par_iter()
                .zip(multi_public_key_chunk)
                .map(|(public_key, multi_public_key)| {
                    let keycard = match (public_key, multi_public_key) {
                        (Some(public_key), Some(multi_public_key)) => Some(
                            KeyCard::from_public_keys(public_key.clone(), multi_public_key.clone()),
                        ),
                        (None, None) => None,
                        _ => unreachable!(),
                    };

                    if let Some(keycard) = keycard {
                        let mut block = [0u8; BLOCK_SIZE];
                        bincode::serialize_into(&mut block[1..], &keycard).unwrap();
                        block
                    } else {
                        [u8::MAX; BLOCK_SIZE]
                    }
                })
                .collect::<Vec<_>>();

            // Flush `chunk` to `file`

            for block in blocks {
                file.write_all(block.as_slice())
                    .map_err(DirectoryError::write_failed)
                    .map_err(DirectoryError::into_top)
                    .spot(here!())?;
            }
        }

        file.flush()
            .map_err(DirectoryError::flush_failed)
            .map_err(DirectoryError::into_top)
            .spot(here!())?;

        Ok(())
    }

    #[cfg(feature = "benchmark")]
    pub unsafe fn save_raw<P>(&self, path: P)
    where
        P: AsRef<Path>,
    {
        fs::create_dir_all(path.as_ref()).unwrap();

        unsafe fn save_component<T>(root: &Path, file: &str, component: &Vec<Option<T>>) {
            let mut path = root.to_path_buf();
            path.push(file);

            let pointer = component.as_ptr() as *const u8;
            let bytes = component.len() * mem::size_of::<Option<T>>();

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap();

            file.set_len(bytes as u64).unwrap();

            let mut mmap = MmapMut::map_mut(&file).unwrap();

            (&mut mmap[..])
                .write_all(slice::from_raw_parts(pointer, bytes))
                .unwrap();

            mmap.flush().unwrap();
        }

        save_component(path.as_ref(), "identities.raw", &self.identities);
        save_component(path.as_ref(), "public_keys.raw", &self.public_keys);

        save_component(
            path.as_ref(),
            "multi_public_keys.raw",
            &self.multi_public_keys,
        );
    }
}

impl Clone for Directory {
    fn clone(&self) -> Self {
        Directory {
            identities: self.identities.clone(),
            public_keys: self.public_keys.clone(),
            multi_public_keys: self.multi_public_keys.clone(),
            #[cfg(feature = "benchmark")]
            mmaps: None,
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        #[cfg(feature = "benchmark")]
        if self.mmaps.is_some() {
            mem::forget(mem::take(&mut self.identities));
            mem::forget(mem::take(&mut self.public_keys));
            mem::forget(mem::take(&mut self.multi_public_keys));
        }
    }
}

#[cfg(test)]
#[cfg(feature = "benchmark")]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use std::{env, iter};
    use talk::crypto::KeyChain;

    #[test]
    fn raw_save_load() {
        let keycards = iter::repeat_with(|| Some(KeyChain::random().keycard()))
            .take(1024)
            .collect::<Vec<_>>();

        let directory = Directory::from_keycards(keycards);

        let before = (0..1024)
            .map(|id| directory.get_identity(id).unwrap())
            .collect::<Vec<_>>();

        let id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();

        let file = format!("test_directory_{id}");

        let mut path = env::temp_dir();
        path.push(file);

        let directory = unsafe {
            directory.save_raw(path.clone());
            Directory::load_raw(path)
        };

        let after = (0..1024)
            .map(|id| directory.get_identity(id).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(before, after);
    }
}
