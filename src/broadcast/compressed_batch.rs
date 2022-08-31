use crate::broadcast::{Message, Straggler};

#[cfg(feature = "benchmark")]
use crate::{
    broadcast::Entry,
    crypto::statements::Reduction as ReductionStatement,
    system::{Directory, Passepartout},
};

#[cfg(feature = "benchmark")]
use rand::seq::IteratorRandom;

#[cfg(feature = "benchmark")]
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

#[cfg(feature = "benchmark")]
use std::iter;

use talk::crypto::primitives::multi::Signature as MultiSignature;

use varcram::VarCram;

#[cfg(feature = "benchmark")]
use zebra::vector::Vector;

pub(crate) struct CompressedBatch {
    pub ids: VarCram,
    pub messages: Vec<Message>,
    pub raise: u64,
    pub multisignature: Option<MultiSignature>,
    pub stragglers: Vec<Straggler>,
}

impl CompressedBatch {
    #[cfg(feature = "benchmark")]
    pub fn random_fully_reduced(
        directory: &Directory,
        passepartout: &Passepartout,
        size: usize,
    ) -> Self {
        let mut ids = (0..(directory.capacity() as u64))
            .into_iter()
            .choose_multiple(&mut rand::thread_rng(), size);

        ids.sort_unstable();

        let messages = iter::repeat_with(rand::random)
            .take(size)
            .collect::<Vec<_>>();

        let raise = 0;

        let entries = ids
            .iter()
            .copied()
            .zip(messages.iter().cloned())
            .map(|(id, message)| {
                Some(Entry {
                    id,
                    sequence: raise,
                    message,
                })
            })
            .collect::<Vec<_>>();

        let entries = Vector::<_>::new(entries).unwrap();

        let statement = ReductionStatement {
            root: &entries.root(),
        };

        let multisignatures = ids
            .par_iter()
            .copied()
            .map(|id| {
                let keycard = directory.get(id).unwrap();
                let identity = keycard.identity();
                let keychain = passepartout.get(identity).unwrap();

                keychain.multisign(&statement).unwrap()
            })
            .collect::<Vec<_>>();

        let multisignature = Some(MultiSignature::aggregate(multisignatures).unwrap());

        let ids = VarCram::cram(ids.as_slice());

        CompressedBatch {
            ids,
            messages,
            raise,
            multisignature,
            stragglers: Vec::new(),
        }
    }
}
