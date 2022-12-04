use crate::{
    applications::pixel_war::{Color, Coordinates, CANVAS_EDGE},
    broadcast::{Entry, Message, MESSAGE_SIZE},
};
use std::sync::atomic::AtomicBool;

#[derive(Debug)]
pub struct Paint {
    pub painter: u64,
    pub coordinates: Coordinates,
    pub color: Color,
    pub throttle: AtomicBool,
}

impl Paint {
    pub fn random(painter: u64) -> Self {
        let entropy = rand::random::<u64>();

        let x = (entropy >> (64 - 16)) % (CANVAS_EDGE as u64);
        let y = ((entropy >> (64 - 32)) & ((1 << 16) - 1)) % (CANVAS_EDGE as u64);

        let red = (entropy >> (64 - 40)) & ((1 << 8) - 1);
        let green = (entropy >> (64 - 48)) & ((1 << 8) - 1);
        let blue = (entropy >> (64 - 56)) & ((1 << 8) - 1);

        Paint {
            painter,
            coordinates: Coordinates {
                x: x as u16,
                y: y as u16,
            },
            color: Color {
                red: red as u8,
                green: green as u8,
                blue: blue as u8,
            },
            throttle: AtomicBool::new(false),
        }
    }

    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message.bytes[0..8].try_into().unwrap());

        let x = message >> (64 - 16);
        let y = (message >> (64 - 32)) & ((1 << 16) - 1);

        let red = (message >> (64 - 40)) & ((1 << 8) - 1);
        let green = (message >> (64 - 48)) & ((1 << 8) - 1);
        let blue = (message >> (64 - 56)) & ((1 << 8) - 1);

        Paint {
            painter: entry.id,
            coordinates: Coordinates {
                x: x as u16,
                y: y as u16,
            },
            color: Color {
                red: red as u8,
                green: green as u8,
                blue: blue as u8,
            },
            throttle: AtomicBool::new(false),
        }
    }

    pub fn to_message(&self) -> (u64, Message) {
        let mut message = (self.coordinates.x as u64) << (64 - 16);
        message |= (self.coordinates.y as u64) << (64 - 32);

        message |= (self.color.red as u64) << (64 - 40);
        message |= (self.color.green as u64) << (64 - 48);
        message |= (self.color.blue as u64) << (64 - 56);

        let message = message.to_le_bytes();

        let mut bytes = [0; MESSAGE_SIZE];
        bytes[0..8].copy_from_slice(&message);

        (self.painter, Message { bytes })
    }
}

impl PartialEq for Paint {
    fn eq(&self, other: &Self) -> bool {
        self.painter == other.painter
            && self.coordinates == other.coordinates
            && self.color == other.color
    }
}

impl Eq for Paint {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applications::pixel_war::CANVAS_EDGE;

    #[test]
    fn inversion() {
        for _ in 0..1024 {
            let original = Paint {
                painter: rand::random::<u32>() as u64,
                coordinates: Coordinates {
                    x: rand::random::<u16>() % CANVAS_EDGE as u16,
                    y: rand::random::<u16>() % CANVAS_EDGE as u16,
                },
                color: Color {
                    red: rand::random::<u8>(),
                    green: rand::random::<u8>(),
                    blue: rand::random::<u8>(),
                },
                throttle: AtomicBool::new(false),
            };

            let (id, message) = original.to_message();

            let entry = Entry {
                id,
                sequence: 0,
                message,
            };

            let parsed = Paint::from_entry(entry);

            assert_eq!(parsed, original);
        }
    }
}
