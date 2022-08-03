use crate::{broadcast::Entry, crypto::records::Height as HeightRecord};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::{hash::Hash, multi::Signature as MultiSignature, sign::Signature};

#[derive(Serialize, Deserialize)]
pub(crate) enum Request {
    Broadcast {
        entry: Entry,
        signature: Signature,
        height_record: Option<HeightRecord>,
    },
    Reduction {
        root: Hash,
        id: u64,
        multisignature: MultiSignature,
    },
}
