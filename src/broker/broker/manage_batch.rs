use crate::{
    broker::{Broker, BrokerSettings, Reduction, Response, Submission},
    crypto::records::Height as HeightRecord,
    system::{Directory, Membership},
};

use std::{collections::HashMap, sync::Arc};

use talk::net::{DatagramSender, SessionConnector};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

type ReductionOutlet = BroadcastReceiver<Reduction>;

impl Broker {
    pub(in crate::broker::broker) async fn manage_batch(
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        pool: HashMap<u64, Submission>,
        top_record: Option<HeightRecord>,
        reduction_outlet: ReductionOutlet,
        sender: Arc<DatagramSender<Response>>,
        connector: Arc<SessionConnector>,
        settings: BrokerSettings,
    ) {
        let mut batch = Broker::setup_batch(pool, top_record, sender.as_ref()).await;

        let compressed_batch =
            Broker::reduce_batch(directory, &mut batch, reduction_outlet, &settings).await;

        let (_height, _delivery_certificate) = Broker::broadcast(
            &mut batch,
            compressed_batch,
            membership.clone(),
            connector,
            settings.clone(),
        )
        .await;

        // TODO: Send message to clients
        todo!()
    }
}
