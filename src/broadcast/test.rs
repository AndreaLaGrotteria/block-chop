use crate::{
    broadcast::{CompressedBatch, PACKING},
    crypto::statements::Reduction,
    Entry,
};

use std::iter;

use talk::crypto::{primitives::multi::Signature as MultiSignature, KeyChain};

use varcram::VarCram;

use zebra::vector::Vector;

pub(crate) fn null_batch(client_keychains: &Vec<KeyChain>, size: usize) -> CompressedBatch {
    let entries = (0..(size as u64))
        .map(|id| {
            Some(Entry {
                id,
                sequence: 0,
                message: Default::default(),
            })
        })
        .collect::<Vec<_>>();

    let entries = Vector::<_, PACKING>::new(entries).unwrap();
    let root = entries.root();

    let multisignatures = client_keychains.iter().take(size).map(|keychain| {
        let reduction_statement = Reduction { root: &root };
        keychain.multisign(&reduction_statement).unwrap()
    });

    let ids = VarCram::cram(Vec::from_iter(0..size as u64).as_slice());
    let messages = Vec::from_iter(iter::repeat(Default::default()).take(size));
    let multisignature = Some(MultiSignature::aggregate(multisignatures).unwrap());

    CompressedBatch {
        ids,
        messages,
        raise: 0,
        multisignature,
        stragglers: vec![],
    }
}
