use crate::{
    applications::pixel_war::{Color, Coordinates},
    broadcast::Entry,
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
    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message);

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

    pub fn to_message(&self) -> (u64, [u8; 8]) {
        let mut message = (self.coordinates.x as u64) << (64 - 16);
        message |= (self.coordinates.y as u64) << (64 - 32);

        message |= (self.color.red as u64) << (64 - 40);
        message |= (self.color.green as u64) << (64 - 48);
        message |= (self.color.blue as u64) << (64 - 56);

        let message = message.to_le_bytes();

        (self.painter, message)
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
