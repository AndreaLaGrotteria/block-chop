use message_derive::message;
use rand::{
    distributions::{Distribution, Standard},
    Rng,
};
use serde::{de::SeqAccess, ser::SerializeTuple, Deserialize, Deserializer, Serialize, Serializer};
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
        let mut seq = serializer.serialize_tuple(MESSAGE_SIZE / 8)?;

        for chunk in self.bytes.chunks(8) {
            seq.serialize_element(&u64::from_be_bytes(chunk.try_into().unwrap()))?;
        }

        seq.end()
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};

        struct BlockVisitor;

        impl<'de> Visitor<'de> for BlockVisitor {
            type Value = Message;

            fn expecting(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
                f.write_str("a `Message` encoded as a tuple of `u64`")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes = [0u8; MESSAGE_SIZE];

                for (index, chunk) in bytes.chunks_mut(8).enumerate() {
                    let block = seq
                        .next_element::<u64>()?
                        .ok_or_else(|| Error::invalid_length(index, &self))?;

                    chunk.copy_from_slice(&block.to_be_bytes());
                }

                Ok(Message { bytes })
            }
        }

        deserializer.deserialize_tuple(MESSAGE_SIZE / 8, BlockVisitor)
    }
}
