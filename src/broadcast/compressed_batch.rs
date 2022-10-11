use crate::broadcast::{Message, Straggler};
#[cfg(feature = "benchmark")]
use crate::{
    broadcast::{Entry, PACKING},
    crypto::statements::{Broadcast as BroadcastStatement, Reduction as ReductionStatement},
    system::{Directory, Passepartout},
};
#[cfg(feature = "benchmark")]
use rand::seq::index;
#[cfg(feature = "benchmark")]
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use talk::crypto::primitives::multi::Signature as MultiSignature;
#[cfg(feature = "benchmark")]
use talk::crypto::{primitives::hash::Hash, KeyChain};
use varcram::VarCram;

#[cfg(feature = "benchmark")]
use zebra::vector::Vector;

#[derive(Serialize, Deserialize)]
pub struct CompressedBatch {
    pub ids: VarCram,
    pub messages: Vec<Message>,
    pub raise: u64,
    pub multisignature: Option<MultiSignature>,
    pub stragglers: Vec<Straggler>,
}

impl CompressedBatch {
    #[cfg(feature = "benchmark")]
    pub fn assemble<R>(requests: R) -> (Hash, CompressedBatch)
    where
        R: IntoIterator<Item = (Entry, KeyChain, bool)>,
    {
        // Sort `requests` by id

        let mut requests = requests.into_iter().collect::<Vec<_>>();
        requests.sort_unstable_by_key(|(entry, ..)| entry.id);

        // Compute raise, extract ids and messages, produce raised `Entry`ies, generate `Straggler`s

        let raise = requests
            .iter()
            .map(|(entry, ..)| entry.sequence)
            .max()
            .unwrap();

        let mut ids = Vec::with_capacity(requests.len());
        let mut messages = Vec::with_capacity(requests.len());

        let mut entries = Vec::with_capacity(requests.len());
        let mut stragglers = Vec::new();

        for (entry, keychain, reduce) in requests.iter() {
            ids.push(entry.id);
            messages.push(entry.message.clone());

            entries.push(Some(Entry {
                id: entry.id,
                sequence: raise,
                message: entry.message.clone(),
            }));

            if !reduce {
                let broadcast_statement = BroadcastStatement {
                    sequence: &entry.sequence,
                    message: &entry.message,
                };

                let signature = keychain.sign(&broadcast_statement).unwrap();

                stragglers.push(Straggler {
                    id: entry.id,
                    sequence: entry.sequence,
                    signature,
                });
            }
        }

        let ids = VarCram::cram(ids.as_slice());
        let mut entries = Vector::<_, PACKING>::new(entries).unwrap();

        // Compute reduction `MultiSignature`

        let reduction_statement = ReductionStatement {
            root: &entries.root(),
        };

        let multisignatures = requests
            .par_iter()
            .filter_map(|(_, keychain, reduce)| {
                if *reduce {
                    Some(keychain.multisign(&reduction_statement).unwrap())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let multisignature = if !multisignatures.is_empty() {
            Some(MultiSignature::aggregate(multisignatures).unwrap())
        } else {
            None
        };

        // Reset straggler sequences in `entries`

        for straggler in stragglers.iter() {
            let index = entries
                .items()
                .binary_search_by_key(&straggler.id, |entry| entry.as_ref().unwrap().id)
                .unwrap();

            let mut entry = entries.items().get(index).cloned().unwrap().unwrap();
            entry.sequence = straggler.sequence;

            entries.set(index, Some(entry)).unwrap();
        }

        let compressed_batch = CompressedBatch {
            ids,
            messages,
            raise,
            multisignature,
            stragglers,
        };

        (entries.root(), compressed_batch)
    }

    #[cfg(feature = "benchmark")]
    pub fn random_fully_reduced(
        directory: &Directory,
        passepartout: &Passepartout,
        size: usize,
    ) -> (Hash, CompressedBatch) {
        let requests = index::sample(&mut rand::thread_rng(), directory.capacity(), size)
            .into_iter()
            .map(|id| {
                let id = id as u64;

                let identity = directory.get(id).unwrap().identity();
                let keychain = passepartout.get(identity).unwrap();

                let entry = Entry {
                    id: id as u64,
                    sequence: 0,
                    message: rand::random(),
                };

                (entry, keychain, true)
            });

        CompressedBatch::assemble(requests)
    }
}
