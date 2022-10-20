use crate::{
    broker::{LoadBroker, LoadBrokerSettings},
    info,
    system::Membership,
    warn,
};
use std::{sync::Arc, time::Instant};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::SessionConnector,
    sync::fuse::Fuse,
};
use tokio::{sync::mpsc, time};

impl LoadBroker {
    pub(in crate::broker::load_broker) async fn broadcast_batches(
        membership: Arc<Membership>,
        broker_identity: Identity,
        connector: SessionConnector,
        batches: Vec<(Hash, Vec<u8>)>,
        settings: LoadBrokerSettings,
    ) {
        // Setup worker recycling

        let connector = Arc::new(connector);

        let mut worker_sequences = vec![0; settings.workers as usize];
        let (worker_recycler, mut available_workers) = mpsc::channel(worker_sequences.len());

        for worker_index in 0..settings.workers {
            worker_recycler.send(worker_index).await.unwrap();
        }

        // Execute submission loop

        let mut submissions = batches.into_iter();

        let submission_start = Instant::now();
        let mut batch_index = 0;

        let fuse = Fuse::new();

        'submission: loop {
            time::sleep(settings.submission_interval).await;

            let target = (submission_start.elapsed().as_secs_f64() * settings.rate) as usize;

            while batch_index < target {
                let (batch_root, raw_batch) = if let Some(submission) = submissions.next() {
                    submission
                } else {
                    // All batches submitted
                    break 'submission;
                };

                let worker_index = match available_workers.try_recv() {
                    Ok(index) => index,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        let choke_start = Instant::now();
                        let index = available_workers.recv().await.unwrap();

                        warn!("Waited {:?} to get a worker.", choke_start.elapsed());

                        index
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => unreachable!(),
                };

                let next_sequence = worker_sequences.get_mut(worker_index as usize).unwrap();

                info!("Sending batch {}.", batch_index);

                {
                    let membership = membership.clone();
                    let connector = connector.clone();
                    let sequence = *next_sequence;
                    let worker_recycler = worker_recycler.clone();
                    let settings = settings.clone();

                    fuse.spawn(async move {
                        LoadBroker::broadcast_batch(
                            broker_identity,
                            worker_index,
                            sequence,
                            batch_root,
                            raw_batch,
                            membership,
                            connector,
                            settings,
                        )
                        .await;

                        worker_recycler.send(worker_index).await.unwrap();
                    });
                }

                *next_sequence += 1;

                batch_index += 1;
            }
        }

        // Wait for all workers to be returned

        for _ in 0..worker_sequences.len() {
            let _ = available_workers.recv().await.unwrap();
        }
    }
}
