use crate::{
    broker::{Broker, Reduction, Response, Submission},
    crypto::records::Height as HeightRecord,
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
        height_record: Option<HeightRecord>,
        _reduction_outlet: ReductionOutlet,
        sender: Arc<DatagramSender<Response>>,
        _connector: Arc<SessionConnector>,
    ) {
        let _batch = Broker::setup_batch(pool, height_record, sender.as_ref()).await;
    }
}
