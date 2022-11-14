use message_derive::message;
use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self, Formatter};

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

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(u64::from_be_bytes(self.bytes))
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};

        struct ByteVisitor;

        impl<'de> Visitor<'de> for ByteVisitor {
            type Value = Message;

            fn expecting(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
                f.write_str("byte representation of a `Message`")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Message {
                    bytes: v.to_be_bytes(),
                })
            }
        }

        deserializer.deserialize_u64(ByteVisitor)
    }
}
