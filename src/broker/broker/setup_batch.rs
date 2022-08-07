use crate::{
    broadcast::Entry,
    broker::{Batch, BatchStatus, Broker, Response, Submission},
};

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
        sender: &DatagramSender<Response>,
    ) -> Batch {
        // Build `Batch`

        let mut submissions = pool.into_values().collect::<Vec<_>>();
        submissions.par_sort_unstable_by_key(|submission| submission.entry.id);

        let raise = submissions
            .par_iter()
            .map(|submission| submission.entry.sequence)
            .max()
            .unwrap();

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

        let batch = Batch {
            status: BatchStatus::Reducing,
            submissions,
            raise,
            entries,
        };

        // TODO: Disseminate proofs of inclusion

        todo!();

        batch
    }
}
