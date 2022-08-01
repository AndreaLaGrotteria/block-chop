use serde::{Deserialize, Serialize};
use talk::crypto::primitives::sign::Signature;

use crate::{broadcast::Message, crypto::records::Height as HeightRecord};

#[derive(Serialize, Deserialize)]
pub(crate) enum Request {
    Broadcast {
        id: u64,
        message: Message,
        signature: Signature,
        height_record: Option<HeightRecord>,
    },
}
