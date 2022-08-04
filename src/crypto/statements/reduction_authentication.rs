use crate::crypto::Header;

use serde::Serialize;

use talk::crypto::{
    primitives::{hash::Hash, multi::Signature as MultiSignature},
    Statement,
};

#[derive(Serialize)]
pub(crate) struct ReductionAuthentication<'a> {
    pub root: &'a Hash,
    pub multisignature: &'a MultiSignature,
}

impl<'a> Statement for ReductionAuthentication<'a> {
    type Header = Header;
    const HEADER: Self::Header = Header::ReductionAuthentication;
}
