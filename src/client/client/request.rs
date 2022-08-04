use crate::{
    broadcast::{Entry, Message},
    broker::Request,
    client::Client,
    crypto::{
        records::Height as HeightRecord,
        statements::{
            Broadcast as BroadcastStatement,
            BroadcastAuthentication as BroadcastAuthenticationStatement,
        },
    },
};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use talk::{crypto::KeyChain, net::DatagramSender};

use tokio::time;

impl Client {
    pub(in crate::client::client) async fn request(
        id: u64,
        keychain: KeyChain,
        brokers: Arc<Mutex<Vec<SocketAddr>>>,
        sender: Arc<DatagramSender>,
        sequence: u64,
        message: Message,
        height_record: Option<HeightRecord>,
    ) {
        // Build request

        let entry = Entry {
            id,
            sequence,
            message,
        };

        let broadcast_statement = BroadcastStatement {
            sequence: &entry.sequence,
            message: &entry.message,
        };

        let signature = keychain.sign(&broadcast_statement).unwrap();

        let authentication = height_record.as_ref().map(|height_record| {
            let authentication_statement = BroadcastAuthenticationStatement { height_record };
            keychain.sign(&authentication_statement).unwrap()
        });

        let request = Request::Broadcast {
            entry,
            signature,
            height_record,
            authentication,
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
