use doomstack::{here, Doom, ResultExt, Top};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};
use talk::crypto::{Identity, KeyChain};

const BLOCK_SIZE: usize = 240;
const CHUNK_SIZE: usize = 16 * 1048576;

pub struct Passepartout {
    pub keychains: BTreeMap<Identity, KeyChain>,
}

#[derive(Doom)]
pub enum PassepartoutError {
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

impl Passepartout {
    pub fn new() -> Self {
        Passepartout {
            keychains: BTreeMap::new(),
        }
    }

    pub fn load<P>(path: P) -> Result<Self, Top<PassepartoutError>>
    where
        P: AsRef<Path>,
    {
        let mut keychains = BTreeMap::new();

        let file = File::open(path)
            .map_err(PassepartoutError::open_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        let metadata = file
            .metadata()
            .map_err(PassepartoutError::metadata_unavailable)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        if metadata.len() % (BLOCK_SIZE as u64) != 0 {
            return PassepartoutError::UnexpectedFileSize.fail().spot(here!());
        }

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
                .map_err(PassepartoutError::read_failed)
                .map_err(PassepartoutError::into_top)
                .spot(here!())?;

                blocks.push(block);
            }

            if blocks.is_empty() {
                break;
            }

            // Deserialize `blocks` in parallel

            let chunk = blocks
                .into_par_iter()
                .map(|block| bincode::deserialize::<(Identity, KeyChain)>(block.as_slice()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PassepartoutError::deserialize_failed)
                .map_err(PassepartoutError::into_top)
                .spot(here!())?;

            // Flush `chunk` into `keychains`

            for (identity, keychain) in chunk {
                keychains.insert(identity, keychain);
            }
        }

        Ok(Passepartout { keychains })
    }

    pub fn get(&self, identity: Identity) -> Option<KeyChain> {
        self.keychains.get(&identity).cloned()
    }

    pub fn insert(&mut self, identity: Identity, keychain: KeyChain) {
        self.keychains.insert(identity, keychain);
    }

    pub fn fetch<P>(path: P, query: Identity) -> Result<Option<KeyChain>, Top<PassepartoutError>>
    where
        P: AsRef<Path>,
    {
        let mut file = File::open(path)
            .map_err(PassepartoutError::open_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        let metadata = file
            .metadata()
            .map_err(PassepartoutError::metadata_unavailable)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        if metadata.len() % (BLOCK_SIZE as u64) != 0 {
            return PassepartoutError::UnexpectedFileSize.fail().spot(here!());
        }

        let blocks = metadata.len() / (BLOCK_SIZE as u64);

        let mut low = 0;
        let mut high = blocks - 1;

        while low <= high {
            let mid = (low + high) / 2;

            file.seek(SeekFrom::Start(mid * (BLOCK_SIZE as u64)))
                .map_err(PassepartoutError::seek_failed)
                .map_err(PassepartoutError::into_top)
                .spot(here!())?;

            let mut block = [0u8; BLOCK_SIZE];

            file.read_exact(block.as_mut_slice())
                .map_err(PassepartoutError::read_failed)
                .map_err(PassepartoutError::into_top)
                .spot(here!())?;

            let (identity, keychain) =
                bincode::deserialize::<(Identity, KeyChain)>(block.as_slice())
                    .map_err(PassepartoutError::deserialize_failed)
                    .map_err(PassepartoutError::into_top)
                    .spot(here!())?;

            if identity < query {
                low = mid + 1;
            } else if identity > query {
                high = mid - 1;
            } else {
                return Ok(Some(keychain));
            }
        }

        return Ok(None);
    }

    pub fn save<P>(&self, path: P) -> Result<(), Top<PassepartoutError>>
    where
        P: AsRef<Path>,
    {
        let file = File::create(path)
            .map_err(PassepartoutError::open_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        let mut file = BufWriter::new(file);
        let mut entries = self.keychains.iter();

        loop {
            // Collect up to `CHUNK_SIZE` elements of `entries`

            let mut chunk = Vec::with_capacity(CHUNK_SIZE);

            for _ in 0..CHUNK_SIZE {
                if let Some(entry) = entries.next() {
                    chunk.push(entry);
                } else {
                    break;
                }
            }

            if chunk.is_empty() {
                break;
            }

            // Serialize `chunk` in parallel

            let blocks = chunk
                .into_par_iter()
                .map(|(identity, keychain)| {
                    let mut block = [0u8; BLOCK_SIZE];
                    bincode::serialize_into(block.as_mut_slice(), &(identity, keychain)).unwrap();
                    block
                })
                .collect::<Vec<_>>();

            // Flush `chunk` to `file`

            for block in blocks {
                file.write_all(block.as_slice())
                    .map_err(PassepartoutError::write_failed)
                    .map_err(PassepartoutError::into_top)
                    .spot(here!())?;
            }
        }

        file.flush()
            .map_err(PassepartoutError::flush_failed)
            .map_err(PassepartoutError::into_top)
            .spot(here!())?;

        Ok(())
    }
}
