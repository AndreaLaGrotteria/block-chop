use crate::{
    broadcast::Message, client::Client, crypto::records::Delivery as DeliveryRecord, Membership,
};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramDispatcherSettings},
    sync::fuse::Fuse,
};

use tokio::sync::{mpsc::Receiver as MpscReceiver, oneshot::Sender as OneshotSender};

type BroadcastOutlet = MpscReceiver<(Message, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<DeliveryRecord>;

impl Client {
    pub(in crate::client::client) async fn run(
        id: u64,
        keychain: KeyChain,
        membership: Membership,
        brokers: Arc<Mutex<Vec<SocketAddr>>>,
        mut broadcast_outlet: BroadcastOutlet,
    ) {
        let dispatcher = DatagramDispatcher::bind(
            "0.0.0.0:0",
            DatagramDispatcherSettings {
                workers: 1,
                ..Default::default() // TODO: Forward settings?
            },
        )
        .await
        .unwrap(); // TODO: Determine if this error should be handled

        let (sender, mut receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        let mut sequence_range = 0..=0;
        let mut height_record = None;

        loop {
            // Wait for the next message to broadcast

            let (message, delivery_inlet) = match broadcast_outlet.recv().await {
                Some(broadcast) => broadcast,
                None => return, // `Client` has dropped, shutdown
            };

            // Spawn requesting task

            let fuse = Fuse::new();

            fuse.spawn(Client::request(
                id,
                keychain.clone(),
                brokers.clone(),
                sender.clone(),
                *sequence_range.start(),
                message,
                height_record.clone(),
            ));

            // Handle `Response`s until `message` is delivered

            let delivery_record = loop {
                let (source, response) = receiver.receive().await;

                if let Ok(Some(delivery_record)) = Client::handle(
                    id,
                    &keychain,
                    &membership,
                    &mut sequence_range,
                    message,
                    source,
                    response,
                    sender.as_ref(),
                )
                .await
                {
                    break delivery_record;
                }
            };

            // Shift `sequence_range`, upgrade `height_record`

            sequence_range = (sequence_range.end() + 1)..=(sequence_range.end() + 1);
            height_record = Some(delivery_record.height());

            // Send `record` back the invoking `broadcast` method

            let _ = delivery_inlet.send(delivery_record);
        }
    }
}
