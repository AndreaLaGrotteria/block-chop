use crate::{
    broker::{LoadBroker, LoadBrokerSettings, Worker},
    info,
    system::Membership,
    warn,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
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
        let (worker_recycler, mut available_workers) = mpsc::channel(workers.len());

        for identity in workers.keys().copied() {
            worker_recycler.send(identity).await.unwrap();
        }

        let fuse = Fuse::new();

        let mut start = Instant::now();

        for (index, (batch_root, compressed_batch)) in batches.into_iter().enumerate() {
            let identity = available_workers.recv().await.unwrap();

            let remaining_period =
                Duration::from_secs_f64(1. / settings.rate).saturating_sub(start.elapsed());

            if remaining_period.is_zero() {
                warn!(
                    "Broker unable to keep up with rate! Estimated appropriate rate: {} B/s",
                    1. / start.elapsed().as_secs_f64()
                );
            }

            time::sleep(remaining_period).await;

            start = Instant::now();

            info!("Sending batch {}.", index);

            let worker = workers.get_mut(&identity).unwrap();

            {
                let membership = membership.clone();
                let sequence = worker.next_sequence;
                let connector = worker.connector.clone();
                let worker_recycler = worker_recycler.clone();
                let settings = settings.clone();

                fuse.spawn(async move {
                    LoadBroker::broadcast_batch(
                        identity,
                        sequence,
                        batch_root,
                        compressed_batch,
                        membership,
                        connector,
                        settings,
                    )
                    .await;

                    worker_recycler.send(identity).await.unwrap();
                });
            }

            worker.next_sequence += 1;
        }

        // Wait for all workers to be done
        for _ in 0..workers.len() {
            let _ = available_workers.recv().await.unwrap();
        }
    }
}
