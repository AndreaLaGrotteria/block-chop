use crate::{
    broker::{LoadBroker, LoadBrokerSettings, Worker},
    info,
    system::Membership,
    warn,
};
use std::{cmp, collections::HashMap, sync::Arc, time::Instant};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    sync::fuse::Fuse,
};
use tokio::{
    sync::mpsc::{self},
    time::{self},
};

impl LoadBroker {
    pub(in crate::broker::load_broker) async fn broadcast_batches(
        membership: Arc<Membership>,
        mut workers: HashMap<Identity, Worker>,
        batches: Vec<(Hash, Vec<u8>)>,
        settings: LoadBrokerSettings,
    ) {
        // Setup worker recycling

        let (worker_recycler, mut available_workers) = mpsc::channel(workers.len());

        for identity in workers.keys().copied() {
            worker_recycler.send(identity).await.unwrap();
        }

        // Execute submission loop

        let mut submissions = batches.into_iter().enumerate();
        let mut last_tick = Instant::now();

        let fuse = Fuse::new();

        'submission: loop {
            let sleep_time = settings
                .minimum_rate_window
                .saturating_sub(last_tick.elapsed());

            if !sleep_time.is_zero() {
                time::sleep(sleep_time).await;
            } else {
                warn!(
                    "Submission lagged by {:?}.",
                    last_tick
                        .elapsed()
                        .saturating_sub(settings.minimum_rate_window)
                );
            }

            let window = last_tick.elapsed();
            last_tick = Instant::now();

            let submission_allowance = (cmp::min(window, settings.maximum_rate_window)
                .as_secs_f64()
                * settings.rate) as usize;

            for _ in 0..submission_allowance {
                let (index, (batch_root, compressed_batch)) =
                    if let Some(submission) = submissions.next() {
                        submission
                    } else {
                        // All batches submitted
                        break 'submission;
                    };

                let worker_identity = available_workers.recv().await.unwrap();
                let worker = workers.get_mut(&worker_identity).unwrap();

                info!("Sending batch {}.", index);

                {
                    let membership = membership.clone();
                    let sequence = worker.next_sequence;
                    let connector = worker.connector.clone();
                    let worker_recycler = worker_recycler.clone();
                    let settings = settings.clone();

                    fuse.spawn(async move {
                        LoadBroker::broadcast_batch(
                            worker_identity,
                            sequence,
                            batch_root,
                            compressed_batch,
                            membership,
                            connector,
                            settings,
                        )
                        .await;

                        worker_recycler.send(worker_identity).await.unwrap();
                    });
                }

                worker.next_sequence += 1;
            }
        }

        // Wait for all workers to be returned

        for _ in 0..workers.len() {
            let _ = available_workers.recv().await.unwrap();
        }
    }
}
