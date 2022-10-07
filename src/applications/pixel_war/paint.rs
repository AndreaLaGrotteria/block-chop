use std::sync::atomic::AtomicBool;

use crate::{applications::pixel_war::Color, broadcast::Entry};

#[derive(Debug)]
pub struct Paint {
    pub painter: u64,
    pub x: u16,
    pub y: u16,
    pub color: Color,
    pub throttle: AtomicBool,
}

impl Paint {
    pub fn from_entry(entry: Entry) -> Self {
        let message = u64::from_le_bytes(entry.message);

        let x = message >> (64 - 16) as usize;
        let y = (message >> (64 - 32)) & ((1 << 16) - 1);
        let r = (message >> (64 - 40)) & ((1 << 8) - 1);
        let g = (message >> (64 - 48)) & ((1 << 8) - 1);
        let b = (message >> (64 - 56)) & ((1 << 8) - 1);

        Paint {
            painter: entry.id,
            x: x as u16,
            y: y as u16,
            color: (r as u8, g as u8, b as u8),
            throttle: AtomicBool::new(false),
        }
    }
}
