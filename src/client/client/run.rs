use crate::{
    broadcast::{Entry, Message},
    broker::{Request, Response},
    client::Client,
    crypto::{
        records::Delivery as DeliveryRecord,
        statements::{
            Reduction as ReductionStatement,
            ReductionAuthentication as ReductionAuthenticationStatement,
        },
    },
    Membership,
};

use std::{
    cmp,
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
                ..Default::default()
            },
        )
        .await
        .unwrap(); // TODO: Determine if this error should be handled

        let (sender, mut receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        let mut sequence = 0..=0;
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
                *sequence.start(),
                message,
                height_record.clone(),
            ));

            // React to `Response`s until `message` is delivered

            let delivery_record = loop {
                let (source, response) = receiver.receive().await;

                let response =
                    if let Ok(response) = bincode::deserialize::<Response>(response.as_slice()) {
                        response
                    } else {
                        continue;
                    };

                match response {
                    Response::Inclusion {
                        id: rid,
                        root,
                        proof,
                        raise,
                        height_record,
                    } => {
                        // Verify that the `Response` concerns the local `Client`

                        if rid != id {
                            continue;
                        }

                        // Verify that `message` is included in `root`

                        let entry = Entry {
                            id,
                            sequence: raise,
                            message,
                        };

                        if proof.verify(root, &entry).is_err() {
                            continue;
                        }

                        // Verify that `raise` does not rewind `sequence`

                        if raise < *sequence.start() {
                            continue;
                        }

                        // Verify that `raise` is justified by `height_record`

                        let height = if let Some(height_record) = height_record {
                            if height_record.verify(&membership).is_err() {
                                continue;
                            }

                            height_record.height()
                        } else {
                            0 // No `Height` record is necessary for height 0
                        };

                        if raise > height {
                            continue;
                        }

                        // Extend `sequence`

                        sequence = (*sequence.start())..=(cmp::max(*sequence.end(), raise));

                        // Multi-sign and authenticate `Reduction` statement

                        let reduction_statement = ReductionStatement { root };
                        let multisignature = keychain.multisign(&reduction_statement).unwrap();

                        let authentication_statement = ReductionAuthenticationStatement {
                            root,
                            multisignature,
                        };

                        let authentication = keychain.sign(&authentication_statement).unwrap();

                        // Send `Reduction` back to `source`

                        let request = Request::Reduction {
                            root,
                            id,
                            multisignature,
                            authentication,
                        };

                        let request = bincode::serialize(&request).unwrap();
                        sender.send(source, request).await;
                    }

                    Response::Delivery {
                        height,
                        root,
                        certificate,
                        sequence: dsequence,
                        proof,
                    } => {
                        // Verify that the delivered sequence is within the current sequence range

                        if !sequence.contains(&dsequence) {
                            continue;
                        }

                        // Build and verify `DeliveryRecord`

                        let entry = Entry {
                            id,
                            sequence: dsequence,
                            message,
                        };

                        let record = DeliveryRecord::new(height, root, certificate, entry, proof);

                        if record.verify(&membership).is_err() {
                            continue;
                        }

                        // Message delivered!

                        break record;
                    }
                }
            };

            // Shift `sequence`, upgrade `height_record`

            sequence = (sequence.end() + 1)..=(sequence.end() + 1);
            height_record = Some(delivery_record.height());

            // Send `record` back the invoking `broadcast` method

            let _ = delivery_inlet.send(delivery_record);
        }
    }
}
