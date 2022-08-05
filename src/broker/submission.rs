use std::net::SocketAddr;

use crate::broadcast::Entry;

use talk::crypto::primitives::{multi::Signature as MultiSignature, sign::Signature};

pub(in crate::broker) struct Submission {
    pub address: SocketAddr,
    pub entry: Entry,
    pub signature: Signature,
    pub reduction: Option<MultiSignature>,
}
