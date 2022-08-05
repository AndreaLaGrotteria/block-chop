use crate::{
    broker::{Broker, Request, Response},
    system::{Directory, Membership},
};

use std::{net::SocketAddr, sync::Arc};

use talk::net::{DatagramSender, SessionConnector};

use tokio::sync::mpsc::Receiver as MpscReceiver;

type RequestOutlet = MpscReceiver<(SocketAddr, Request)>;

impl Broker {
    pub(in crate::broker::broker) async fn handle_requests(
        _membership: Arc<Membership>,
        _directory: Arc<Directory>,
        _handle_outlet: RequestOutlet,
        _sender: Arc<DatagramSender<Response>>,
        _connector: Arc<SessionConnector>,
    ) {
    }
}
