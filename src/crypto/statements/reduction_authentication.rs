use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{
    primitives::{hash::Hash, multi::Signature as MultiSignature},
    Statement,
};

#[derive(Serialize)]
pub(crate) struct ReductionAuthentication {
    pub root: Hash,
    pub multisignature: MultiSignature,
}

impl Statement for ReductionAuthentication {
    type Header = Header;
    const HEADER: Self::Header = Header::ReductionAuthentication;
}
