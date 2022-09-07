use crate::{
    broadcast::{Entry, PACKING},
    broker::{Batch, BatchStatus, Broker, Response, Submission},
    crypto::records::Height as HeightRecord,
    info,
};

use rayon::{
    iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};

use std::collections::HashMap;

use talk::net::DatagramSender;

use zebra::vector::Vector;

impl Broker {
    pub(in crate::broker::broker) async fn setup_batch(
        pool: HashMap<u64, Submission>,
        top_record: Option<HeightRecord>,
        sender: &DatagramSender<Response>,
    ) -> Batch {
        // Build `Batch` fields

        info!("Building batch..");

        let mut submissions = pool.into_values().collect::<Vec<_>>();
        submissions.par_sort_unstable_by_key(|submission| submission.entry.id);

        let raise = submissions
            .par_iter()
            .map(|submission| submission.entry.sequence)
            .max()
            .unwrap(); // `submissions` is always non-empty

        let entries = submissions
            .par_iter()
            .map(|submission| {
                Some(Entry {
                    id: submission.entry.id,
                    sequence: raise,
                    message: submission.entry.message.clone(),
                })
            })
            .collect::<Vec<_>>();

        let entries = Vector::<_, PACKING>::new(entries).unwrap();

        info!(
            "Batch built with root {:#?} ({} entries).",
            entries.root(),
            entries.len()
        );

        // Disseminate proofs of inclusion

        info!("Disseminating proofs of inclusion.");

        let inclusions = submissions
            .par_iter()
            .enumerate()
            .map(|(index, submission)| {
                let address = submission.address;

                let inclusion = Response::Inclusion {
                    id: submission.entry.id,
                    root: entries.root(),
                    proof: entries.prove(index),
                    raise,
                    top_record: top_record.clone(),
                };

                (address, inclusion)
            })
            .collect::<Vec<_>>();

        for (address, inclusion) in inclusions {
            sender.send(address, inclusion).await;
        }

        // Assemble and return `Batch`

        Batch {
            status: BatchStatus::Reducing,
            submissions,
            raise,
            entries,
        }
    }
}
