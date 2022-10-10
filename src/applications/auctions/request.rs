use crate::Entry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Bid { bidder: u64, token: u64, offer: u64 },
    Take { owner: u64, token: u64 },
}

impl Request {
    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message);

        let is_take = (message >> 63) == 1;
        let token = message >> 32 & ((1 << 31) - 1);

        if is_take {
            Request::Take {
                owner: entry.id,
                token,
            }
        } else {
            let offer = message & ((1 << 32) - 1);

            Request::Bid {
                bidder: entry.id,
                token,
                offer,
            }
        }
    }

    pub fn to_message(&self) -> (u64, [u8; 8]) {
        match self {
            Request::Bid {
                bidder,
                token,
                offer,
            } => {
                assert!(*token < (1 << 31));
                assert!(*offer < (1 << 32));

                let message = (token << 32) | offer;
                let message = message.to_le_bytes();

                (*bidder, message)
            }
            Request::Take { owner, token } => {
                assert!(*token < (1 << 31));

                let message = (1 << 63) | (token << 32);
                let message = message.to_le_bytes();

                (*owner, message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inversion() {
        for _ in 0..1024 {
            let original = if rand::random::<bool>() {
                Request::Bid {
                    bidder: rand::random::<u64>(),
                    token: rand::random::<u64>() % (1 << 31),
                    offer: rand::random::<u64>() % (1 << 32),
                }
            } else {
                Request::Take {
                    owner: rand::random::<u64>(),
                    token: rand::random::<u64>() % (1 << 31),
                }
            };

            let (id, message) = original.to_message();

            let entry = Entry {
                id,
                sequence: 0,
                message,
            };

            let parsed = Request::from_entry(entry);

            assert_eq!(parsed, original);
        }
    }
}
