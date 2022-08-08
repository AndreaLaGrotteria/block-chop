use talk::crypto::primitives::sign::Signature;

pub(crate) struct Straggler {
    pub id: u64,
    pub sequence: u64,
    pub signature: Signature,
}
