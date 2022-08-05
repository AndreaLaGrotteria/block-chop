use crate::broker::Broker;

use std::net::SocketAddr;

use talk::net::DatagramReceiver;

use tokio::sync::mpsc::Sender as MpscSender;

type DatagramInlet = MpscSender<(SocketAddr, Vec<u8>)>;

impl Broker {
    pub(in crate::broker::broker) async fn dispatch_requests(
        mut receiver: DatagramReceiver,
        datagram_inlets: Vec<DatagramInlet>,
    ) {
        let mut robin = 0;

        loop {
            let datagram = receiver.receive().await;

            // This fails only if the `Broker` is shutting down
            let _ = datagram_inlets
                .get(robin % datagram_inlets.len())
                .unwrap()
                .send(datagram);

            robin += 1;
        }
    }
}
