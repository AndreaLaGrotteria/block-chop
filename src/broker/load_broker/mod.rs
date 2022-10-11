use crate::{
    broker::{LoadBrokerSettings, Worker},
    system::Membership,
};
use std::{collections::HashMap, sync::Arc};
use talk::{crypto::Identity, net::SessionConnector, sync::fuse::Fuse};

pub struct LoadBroker {
    _fuse: Fuse,
}

impl LoadBroker {
    pub async fn new<I>(membership: Membership, connectors: I, settings: LoadBrokerSettings) -> Self
    where
        I: IntoIterator<Item = (Identity, SessionConnector)>,
    {
        // Build `Arc`s

        let membership = Arc::new(membership);

        // Setup workers

        let workers = connectors
            .into_iter()
            .map(|(identity, connector)| {
                (
                    identity,
                    Worker {
                        connector: Arc::new(connector),
                        next_sequence: 0,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        // Load batches from disk

        let batches = vec![];

        // Spawn tasks

        let fuse = Fuse::new();

        fuse.spawn(LoadBroker::broadcast_batches(
            membership.clone(),
            workers,
            batches,
            settings.clone(),
        ));

        LoadBroker { _fuse: fuse }
    }
}

mod broadcast_batch;
mod broadcast_batches;
mod submit_batch;
