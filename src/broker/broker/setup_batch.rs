use crate::{
    broadcast::Entry,
    broker::{Batch, BatchStatus, Broker, Response, Submission},
    crypto::records::Height as HeightRecord,
};

use log::info;

use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
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

        let entries = Vector::new(entries).unwrap();

        info!("Batch built with root {:#?}.", entries.root());

        // Disseminate proofs of inclusion

        info!("Disseminating proofs of inclusion.");

        let inclusions = submissions
            .iter()
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

        sender.pace(inclusions, 65536.).await; // TODO: Add settings

        // Assemble and return `Batch`

        Batch {
            status: BatchStatus::Reducing,
            submissions,
            raise,
            entries,
        }
    }
}
