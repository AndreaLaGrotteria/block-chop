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
        pool: HashMap<u64, Submission>,
        _reduction_outlet: ReductionOutlet,
        sender: Arc<DatagramSender<Response>>,
        _connector: Arc<SessionConnector>,
    ) {
        let batch = Broker::setup_batch(pool, sender.as_ref()).await;
    }
}
