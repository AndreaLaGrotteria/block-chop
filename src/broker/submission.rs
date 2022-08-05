use crate::broadcast::Entry;

use talk::crypto::primitives::{multi::Signature as MultiSignature, sign::Signature};

pub(in crate::broker) struct Submission {
    entry: Entry,
    signature: Signature,
    reduction: Option<MultiSignature>,
}
