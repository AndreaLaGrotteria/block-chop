use doomstack::{here, Doom, ResultExt, Top};
#[cfg(feature = "benchmark")]
use memmap::{Mmap, MmapMut, MmapOptions};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
#[cfg(feature = "benchmark")]
use std::{
    fs::{File, OpenOptions},
    slice,
};
use std::{
    io::{self, BufReader, BufWriter},
    path::Path,
};
#[cfg(feature = "benchmark")]
use std::{
    io::{Read, Write},
    mem,
};
use talk::crypto::{
    primitives::{
        multi::{PublicKey as MultiPublicKey, Signer as MultiSigner},
        sign::{PublicKey as SignPublicKey, Signer as SignSigner},
    },
    Identity, KeyCard,
};

const BLOCK_SIZE: usize = 145;
const CHUNK_SIZE: usize = 16 * 1048576;

#[cfg(feature = "benchmark")]
const ENTRY_SIZE: usize = mem::size_of::<Option<KeyCard>>();

pub struct Directory {
    keycards: Vec<Option<KeyCard>>,
    #[cfg(feature = "benchmark")]
    mmap: Option<Mmap>,
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

        Ok(Directory {
            keycards,
            #[cfg(feature = "benchmark")]
            mmap: None,
        })
    }

    #[cfg(feature = "benchmark")]
    pub unsafe fn load_raw<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let mmap = MmapOptions::new().map(&File::open(&path).unwrap()).unwrap();

        if mmap.len() % ENTRY_SIZE != 0 {
            panic!("Called `Directory::load_raw` on a non-aligned file");
        }

        let pointer = mmap.as_ptr() as *mut Option<KeyCard>;
        let capacity = mmap.len() / ENTRY_SIZE;

        Directory {
            keycards: Vec::from_raw_parts(pointer, capacity, capacity),
            mmap: Some(mmap),
        }
    }

    pub(crate) fn from_keycards(keycards: Vec<Option<KeyCard>>) -> Self {
        Directory {
            keycards,
            #[cfg(feature = "benchmark")]
            mmap: None,
        }
    }

    pub fn capacity(&self) -> usize {
        self.keycards.len()
    }

    pub fn get(&self, id: u64) -> Option<&KeyCard> {
        self.keycards.get(id as usize).map(Option::as_ref).flatten()
    }

    pub fn get_identity(&self, id: u64) -> Option<Identity> {
        self.keycards
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
            .map(KeyCard::identity)
    }

    pub fn get_public_key(&self, id: u64) -> Option<&SignPublicKey> {
        self.keycards
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
            .map(SignSigner::public_key)
    }

    pub fn get_multi_public_key(&self, id: u64) -> Option<&MultiPublicKey> {
        self.keycards
            .get(id as usize)
            .map(Option::as_ref)
            .flatten()
            .map(MultiSigner::public_key)
    }

    pub fn insert(&mut self, id: u64, keycard: KeyCard) {
        #[cfg(feature = "benchmark")]
        if self.mmap.is_some() {
            panic!("Called `Directory::insert` on an `memmap`ed `Directory`");
        }

        if self.keycards.len() <= (id as usize) {
            self.keycards.resize((id as usize) + 1, None);
        }

        *self.keycards.get_mut(id as usize).unwrap() = Some(keycard);
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

        for chunk in self.keycards.chunks(CHUNK_SIZE) {
            // Serialize `chunk` in parallel

            let blocks = chunk
                .into_par_iter()
                .map(|keycard| {
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
        let pointer = self.keycards.as_ptr() as *const u8;
        let bytes = self.keycards.len() * ENTRY_SIZE;

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
}

impl Clone for Directory {
    fn clone(&self) -> Self {
        Directory {
            keycards: self.keycards.clone(),
            #[cfg(feature = "benchmark")]
            mmap: None,
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        #[cfg(feature = "benchmark")]
        if self.mmap.is_some() {
            mem::forget(mem::take(&mut self.keycards));
        }
    }
}

#[cfg(test)]
#[cfg(feature = "benchmark")]
mod tests {
    use super::*;
    use std::{env, iter};
    use talk::crypto::KeyChain;

    #[test]
    fn raw_save_load() {
        let keycards = iter::repeat_with(|| Some(KeyChain::random().keycard()))
            .take(1024)
            .collect::<Vec<_>>();

        let directory = Directory::from_keycards(keycards);

        let before = (0..1024)
            .map(|id| directory.get(id).unwrap().identity())
            .collect::<Vec<_>>();

        let mut path = env::temp_dir();
        path.push("test_directory.bin");

        let directory = unsafe {
            directory.save_raw(path.clone());
            Directory::load_raw(path)
        };

        let after = (0..1024)
            .map(|id| directory.get(id).unwrap().identity())
            .collect::<Vec<_>>();

        assert_eq!(before, after);
    }
}
