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
        _directory: Arc<Directory>,
        _datagram_outlet: DatagramOutlet,
        _request_inlet: RequestInlet,
    ) {
    }
}
