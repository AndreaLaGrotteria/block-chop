use crate::{
    broker::{Broker, Request},
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
                } => {}
                Request::Reduction {
                    root,
                    id,
                    multisignature,
                    authentication,
                } => {}
            }

            // Send `request` over to `handle` task

            // This fails only if the `Broker` is shutting down
            let _ = request_inlet.send((source, request)).await;
        }
    }
}
