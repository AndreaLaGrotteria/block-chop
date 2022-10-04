use doomstack::{here, Doom, ResultExt, Top};
#[cfg(feature = "benchmark")]
use memmap::{Mmap, MmapMut, MmapOptions};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::path::Path;
#[cfg(feature = "benchmark")]
use std::{
    fs::{File, OpenOptions},
    slice,
};
#[cfg(feature = "benchmark")]
use std::{io::Write, mem};
use talk::crypto::KeyCard;

#[cfg(feature = "benchmark")]
const ENTRY_SIZE: usize = mem::size_of::<Option<KeyCard>>();

pub struct Directory {
    keycards: Vec<Option<KeyCard>>,
    #[cfg(feature = "benchmark")]
    mmap: Option<Mmap>,
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

        let entries = database
            .iter()
            .map(|entry| -> Result<_, Top<_>> {
                let (key, value) = entry
                    .map_err(DirectoryError::load_failed)
                    .map_err(DirectoryError::into_top)
                    .spot(here!())?;

                Ok((key, value))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let entries = entries
            .into_par_iter()
            .map(|(key, value)| -> Result<_, Top<_>> {
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

                Ok((id, keycard))
            })
            .collect::<Vec<_>>();

        for entry in entries {
            let (id, keycard) = entry?;
            *keycards.get_mut(id as usize).unwrap() = Some(keycard);
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
