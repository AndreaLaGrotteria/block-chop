use crate::broker::{Broker, Request};

use std::net::SocketAddr;

use talk::net::DatagramReceiver;

use tokio::sync::mpsc::Sender as MpscSender;

type RequestInlet = MpscSender<(SocketAddr, Request)>;

impl Broker {
    pub(in crate::broker::broker) async fn dispatch_requests(
        mut receiver: DatagramReceiver<Request>,
        authenticate_inlets: Vec<RequestInlet>,
    ) {
        let mut robin = 0;

        loop {
            let datagram = receiver.receive().await;

            // This fails only if the `Broker` is shutting down
            let _ = authenticate_inlets
                .get(robin % authenticate_inlets.len())
                .unwrap()
                .send(datagram);

            robin += 1;
        }
    }
}
