use crate::{
    broker::{Broker, Reduction, Response, Submission},
    system::{Directory, Membership},
};

use std::{collections::HashMap, sync::Arc};

use talk::net::{DatagramSender, SessionConnector};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

type ReductionOutlet = BroadcastReceiver<Reduction>;

impl Broker {
    pub(in crate::broker::broker) async fn manage_batch(
        _membership: Arc<Membership>,
        _directory: Arc<Directory>,
        _pool: HashMap<u64, Submission>,
        _reduction_outlet: ReductionOutlet,
        _sender: Arc<DatagramSender<Response>>,
        _connector: Arc<SessionConnector>,
    ) {
    }
}
