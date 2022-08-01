use crate::{
    broadcast::Message, broker::Request, client::Client,
    crypto::statements::Broadcast as BroadcastStatement,
};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramSender},
    sync::fuse::Fuse,
};

use tokio::{sync::mpsc::Receiver as MpscReceiver, time};

type BroadcastOutlet = MpscReceiver<Message>;

impl Client {
    pub(in crate::client::client) async fn run(
        id: u64,
        keychain: KeyChain,
        brokers: Arc<StdMutex<Vec<SocketAddr>>>,
        mut broadcast_outlet: BroadcastOutlet,
    ) {
        let dispatcher = DatagramDispatcher::bind(
            "0.0.0.0:0",
            DatagramDispatcherSettings {
                workers: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap(); // TODO: Determine if this error should be handled

        let (sender, _receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        let sequence = 0..=0;

        loop {
            // Wait for the next message to broadcast

            let message = match broadcast_outlet.recv().await {
                Some(message) => message,
                None => return, // `Client` has dropped, shutdown
            };

            // Spawn requesting task

            let fuse = Fuse::new();

            fuse.spawn(Client::request(
                id,
                keychain.clone(),
                brokers.clone(),
                sender.clone(),
                *sequence.start(),
                message,
            ));
        }
    }

    async fn request(
        id: u64,
        keychain: KeyChain,
        brokers: Arc<StdMutex<Vec<SocketAddr>>>,
        sender: Arc<DatagramSender>,
        sequence: u64,
        message: Message,
    ) {
        // Build request

        let statement = BroadcastStatement { sequence, message };
        let signature = keychain.sign(&statement).unwrap();

        let request = Request::Broadcast {
            id,
            message,
            signature,
            height_record: None,
        };

        let request = bincode::serialize(&request).unwrap();

        for index in 0.. {
            // Fetch next broker

            let broker = loop {
                if let Some(broker) = brokers.lock().unwrap().get(index).cloned() {
                    break broker;
                }

                time::sleep(Duration::from_secs(1)).await;
            };

            // Send request to `broker`

            sender.send(broker, request.clone()).await;

            // Wait for timeout

            time::sleep(Duration::from_secs(20)).await; // TODO: Add setting
        }
    }
}
