use crate::broker::Broker;

use std::net::SocketAddr;

use talk::net::DatagramReceiver;

use tokio::sync::mpsc::Sender as MpscSender;

type DatagramInlet = MpscSender<(SocketAddr, Vec<u8>)>;

impl Broker {
    pub(in crate::broker::broker) async fn dispatch(
        _receiver: DatagramReceiver,
        _datagram_inlets: Vec<DatagramInlet>,
    ) {
    }
}
