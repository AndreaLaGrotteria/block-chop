use crate::{
    broadcast::{CompressedBatch, PACKING},
    crypto::statements::Reduction,
    Entry,
};
use rand::seq::IteratorRandom;
use std::iter;
use talk::crypto::{
    primitives::{hash::Hash, multi::Signature as MultiSignature},
    KeyChain,
};
use varcram::VarCram;
use zebra::vector::Vector;

pub(crate) fn null_batch(client_keychains: &Vec<KeyChain>, size: usize) -> (Hash, CompressedBatch) {
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

    let compressed_batch = CompressedBatch {
        ids,
        messages,
        raise: 0,
        multisignature,
        stragglers: vec![],
    };

    (root, compressed_batch)
}

pub(crate) fn random_unauthenticated_batch(clients: usize, size: usize) -> (Hash, CompressedBatch) {
    let mut ids = (0..(clients as u64))
        .into_iter()
        .choose_multiple(&mut rand::thread_rng(), size);

    ids.sort_unstable();

    let messages = iter::repeat_with(rand::random)
        .take(size)
        .collect::<Vec<_>>();

    let entries = ids
        .iter()
        .copied()
        .zip(messages.iter().cloned())
        .map(|(id, message)| Entry {
            id,
            sequence: 0,
            message,
        })
        .collect::<Vec<_>>();

    let entries = Vector::<_, PACKING>::new(entries).unwrap();
    let root = entries.root();

    let ids = VarCram::cram(ids.as_slice());
    let raise = 0;

    let compressed_batch = CompressedBatch {
        ids,
        messages,
        raise,
        multisignature: None,
        stragglers: Vec::new(),
    };

    (root, compressed_batch)
}
