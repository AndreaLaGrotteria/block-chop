use crate::{
    broker::{Broker, Request},
    crypto::statements::{
        Broadcast as BroadcastStatement,
        BroadcastAuthentication as BroadcastAuthenticationStatement,
        ReductionAuthentication as ReductionAuthenticationStatement,
    },
    system::Directory,
};

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

type DatagramOutlet = MpscReceiver<(SocketAddr, Vec<u8>)>;
type RequestInlet = MpscSender<(SocketAddr, Request)>;

impl Broker {
    pub(in crate::broker::broker) async fn process(
        directory: Arc<Directory>,
        mut datagram_outlet: DatagramOutlet,
        request_inlet: RequestInlet,
    ) {
        loop {
            // Get next `Request`

            let (source, request) = if let Some(datagram) = datagram_outlet.recv().await {
                datagram
            } else {
                // `Broker` has dropped, shutdown
                return;
            };

            // Deserialize `request`

            let request = if let Ok(request) = bincode::deserialize(request.as_slice()) {
                request
            } else {
                continue;
            };

            // Verify `request`'s signatures

            match &request {
                Request::Broadcast {
                    entry,
                    signature,
                    height_record,
                    authentication,
                } => {
                    // Fetch `KeyCard` from `directory`

                    let keycard = if let Some(keycard) = directory.get(entry.id) {
                        keycard
                    } else {
                        continue;
                    };

                    // Verify `signature`

                    let broadcast_statement = BroadcastStatement {
                        sequence: &entry.sequence,
                        message: &entry.message,
                    };

                    if signature.verify(keycard, &broadcast_statement).is_err() {
                        continue;
                    }

                    // Verify `authentication`

                    if let Some(height_record) = height_record {
                        let authentication = if let Some(authentication) = authentication {
                            authentication
                        } else {
                            continue;
                        };

                        let authentication_statement =
                            BroadcastAuthenticationStatement { height_record };

                        if authentication
                            .verify(keycard, &authentication_statement)
                            .is_err()
                        {
                            continue;
                        }
                    }
                }
                Request::Reduction {
                    root,
                    id,
                    multisignature,
                    authentication,
                } => {
                    // Fetch `KeyCard` from `directory`

                    let keycard = if let Some(keycard) = directory.get(*id) {
                        keycard
                    } else {
                        continue;
                    };

                    // Verify `authentication`

                    let authentication_statement = ReductionAuthenticationStatement {
                        root,
                        multisignature,
                    };

                    if authentication
                        .verify(keycard, &authentication_statement)
                        .is_err()
                    {
                        continue;
                    }
                }
            }

            // Send `request` over to `handle` task

            // This fails only if the `Broker` is shutting down
            let _ = request_inlet.send((source, request)).await;
        }
    }
}
