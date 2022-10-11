use crate::{
    broker::{LoadBroker, LoadBrokerSettings, Worker},
    info,
    system::Membership,
};
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    sync::fuse::Fuse,
};
use tokio::sync::mpsc::{self};

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

        for (batch_root, compressed_batch) in batches {
            let identity = available_workers.recv().await.unwrap();

            info!("Sending batch.");

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
    }
}
