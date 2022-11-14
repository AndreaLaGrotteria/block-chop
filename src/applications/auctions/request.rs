use crate::broadcast::{Entry, Message};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Bid { bidder: u64, token: u64, offer: u64 },
    Take { taker: u64, token: u64 },
}

impl Request {
    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message.bytes);

        let is_take = (message >> 63) == 1;
        let token = message >> 32 & ((1 << 31) - 1);

        if is_take {
            Request::Take {
                taker: entry.id,
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

    pub fn to_message(&self) -> (u64, Message) {
        match self {
            Request::Bid {
                bidder,
                token,
                offer,
            } => {
                debug_assert!(*token < (1 << 31));
                debug_assert!(*offer < (1 << 32));

                let message = (token << 32) | offer;
                let bytes = message.to_le_bytes();

                (*bidder, Message { bytes })
            }
            Request::Take { taker, token } => {
                debug_assert!(*token < (1 << 31));

                let message = (1 << 63) | (token << 32);
                let bytes = message.to_le_bytes();

                (*taker, Message { bytes })
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
                    taker: rand::random::<u64>(),
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
