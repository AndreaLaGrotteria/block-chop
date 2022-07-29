use talk::crypto::KeyCard;

pub struct Directory {
    keycards: Vec<Option<KeyCard>>,
}

impl Directory {
    pub fn new() -> Self {
        Directory::from_keycards(Vec::new())
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
}
