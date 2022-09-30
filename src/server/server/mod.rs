use crate::{
    order::Order,
    server::{Deduplicator, ServerSettings, TotalityManager},
    system::{Directory, Membership},
    Entry,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::KeyChain,
    net::{Connector, Listener, SessionListener},
    sync::fuse::Fuse,
};
use tokio::sync::{mpsc, mpsc::Receiver as MpscReceiver};

type BurstOutlet = MpscReceiver<Vec<Option<Entry>>>;

pub struct Server {
    next_batch_outlet: BurstOutlet,
    _fuse: Fuse,
}

impl Server {
    pub fn new<B, BL, TC, TL>(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: B,
        broker_listener: BL,
        totality_connector: TC,
        totality_listener: TL,
        settings: ServerSettings,
    ) -> Self
    where
        B: Order,
        BL: Listener,
        TC: Connector,
        TL: Listener,
    {
        // Preprocess arguments

        let broadcast = Arc::new(broadcast);
        let broker_listener = SessionListener::new(broker_listener);

        // Initialize components

        let broker_slots = Arc::new(Mutex::new(HashMap::new()));

        let totality_manager = TotalityManager::new(
            membership.clone(),
            totality_connector,
            totality_listener,
            Default::default(),
        );

        let deduplicator = Deduplicator::with_capacity(directory.capacity(), Default::default());

        // Initialize channels

        let (next_batch_inlet, next_batch_outlet) =
            mpsc::channel(settings.next_batch_channel_capacity);

        // Spawn tasks

        let fuse = Fuse::new();

        {
            let keychain = keychain.clone();
            let membership = membership.clone();
            let broadcast = broadcast.clone();
            let broker_slots = broker_slots.clone();
            let settings = settings.clone();

            fuse.spawn(async move {
                Server::listen(
                    keychain,
                    membership,
                    directory,
                    broadcast,
                    broker_slots,
                    broker_listener,
                    settings,
                )
                .await;
            });
        }

        {
            let broker_slots = broker_slots.clone();

            fuse.spawn(async move {
                Server::deliver(
                    keychain,
                    membership,
                    broadcast,
                    broker_slots,
                    totality_manager,
                    deduplicator,
                    next_batch_inlet,
                )
                .await;
            });
        }

        // Assemble `Server`

        Server {
            next_batch_outlet,
            _fuse: fuse,
        }
    }

    pub async fn next_batch(&mut self) -> impl Iterator<Item = Entry> {
        // The `Fuse` to `Server::deliver` is owned by `self`,
        // so `next_batch_inlet` cannot have been dropped

        self.next_batch_outlet
            .recv()
            .await
            .unwrap()
            .into_iter()
            .flatten()
    }
}

mod deliver;
mod listen;
