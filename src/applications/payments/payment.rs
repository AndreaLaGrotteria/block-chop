use crate::{
    applications::payments::Deposit,
    broadcast::{Entry, Message},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payment {
    pub from: u64,
    pub to: u64,
    pub amount: u64,
}

impl Payment {
    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message.bytes);

        Payment {
            from: entry.id,
            to: message >> 32,
            amount: message & ((1 << 32) - 1),
        }
    }

    pub(in crate::applications::payments) fn deposit(&self) -> Deposit {
        Deposit {
            to: self.to,
            amount: self.amount,
        }
    }

    pub fn to_message(&self) -> (u64, Message) {
        debug_assert!(self.to < (1 << 32));
        debug_assert!(self.amount < (1 << 32));

        let message = (self.to << 32) | self.amount;
        let bytes = message.to_le_bytes();

        (self.from, Message { bytes })
    }

    #[cfg(feature = "benchmark")]
    pub fn generate(from: u64, num_accounts: u64, max_amount: u64) -> Payment {
        Payment {
            from,
            to: rand::random::<u32>() as u64 % num_accounts,
            amount: rand::random::<u32>() as u64 % max_amount,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inversion() {
        for _ in 0..1024 {
            let original = Payment {
                from: rand::random::<u32>() as u64,
                to: rand::random::<u32>() as u64,
                amount: rand::random::<u32>() as u64,
            };

            let (id, message) = original.to_message();

            let entry = Entry {
                id,
                sequence: 0,
                message,
            };

            let parsed = Payment::from_entry(entry);

            assert_eq!(parsed, original);
        }
    }
}
