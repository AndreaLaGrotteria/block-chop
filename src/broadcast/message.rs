use message_derive::message;
use rand::{
    distributions::{Distribution, Standard},
    Rng,
};

message!();

impl Distribution<Message> for Standard {
    fn sample<R>(&self, rng: &mut R) -> Message
    where
        R: Rng + ?Sized,
    {
        let mut bytes = [0u8; MESSAGE_SIZE];
        rng.fill_bytes(&mut bytes);

        Message { bytes }
    }
}
