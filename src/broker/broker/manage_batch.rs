use crate::{
    broker::{Broker, BrokerSettings, Reduction, Response, Submission},
    crypto::records::Height as HeightRecord,
    debug,
    system::{Directory, Membership},
};
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::Identity,
    net::{DatagramSender, SessionConnector},
};
use tokio::sync::{broadcast::Receiver as BroadcastReceiver, mpsc::Sender as MpscSender};

type ReductionOutlet = BroadcastReceiver<Reduction>;
type IndexInlet = MpscSender<u16>;

impl Broker {
    pub(in crate::broker::broker) async fn manage_batch(
        broker_identity: Identity,
        worker_index: u16,
        sequence: u64,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        pool: HashMap<u64, Submission>,
        top_record: Option<HeightRecord>,
        reduction_outlet: ReductionOutlet,
        sender: Arc<DatagramSender<Response>>,
        connector: Arc<SessionConnector>,
        worker_recycler: IndexInlet,
        settings: BrokerSettings,
    ) {
        let mut batch = Broker::setup_batch(pool, top_record, sender.as_ref()).await;

        let compressed_batch =
            Broker::reduce_batch(directory, &mut batch, reduction_outlet, &settings).await;

        let (height, delivery_certificate) = Broker::broadcast_batch(
            broker_identity,
            worker_index,
            sequence,
            &mut batch,
            compressed_batch,
            membership.clone(),
            connector,
            settings.clone(),
        )
        .await;

        worker_recycler.send(worker_index).await.unwrap();

        debug!("Got height and delivery certificate!");

        Broker::disseminate_deliveries(batch, height, delivery_certificate, sender.as_ref()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broadcast::MESSAGE_SIZE, client::Client, system::test::generate_system};
    use std::time::Duration;
    use talk::{
        crypto::KeyChain,
        net::{test::TestConnector, SessionConnector},
    };
    use tokio::time;

    #[tokio::test]
    async fn broker_manage_single_client_single_message() {
        let (_servers, membership, directory, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let broker_address = "127.0.0.1:9000";
        let keychain = KeyChain::random();
        let broker_identity = keychain.keycard().identity();
        let connector = TestConnector::new(keychain, connector_map.clone());
        let session_connector = SessionConnector::new(connector);

        let _broker = Broker::new(
            membership.clone(),
            directory,
            broker_address,
            broker_identity,
            session_connector,
            BrokerSettings {
                pool_timeout: Duration::from_millis(10),
                totality_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let client = client_keychains[0].clone();
        let client = Client::new(0, client, membership.clone(), "127.0.0.1:9001");
        client.add_broker(broker_address).await.unwrap();
        let _delivery_record = client.broadcast([1u8; MESSAGE_SIZE]).await;

        time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn broker_manage_single_client_multi_messages() {
        let (_servers, membership, directory, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let broker_address = "127.0.0.1:9000";
        let keychain = KeyChain::random();
        let broker_identity = keychain.keycard().identity();
        let connector = TestConnector::new(keychain, connector_map.clone());
        let session_connector = SessionConnector::new(connector);

        let _broker = Broker::new(
            membership.clone(),
            directory,
            broker_address,
            broker_identity,
            session_connector,
            BrokerSettings {
                pool_timeout: Duration::from_millis(10),
                totality_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let client = client_keychains[0].clone();
        let client = Client::new(0, client, membership.clone(), "127.0.0.1:9001");
        client.add_broker(broker_address).await.unwrap();
        let _delivery_record = client.broadcast([1u8; MESSAGE_SIZE]).await;
        let _delivery_record = client.broadcast([2u8; MESSAGE_SIZE]).await;
        let _delivery_record = client.broadcast([3u8; MESSAGE_SIZE]).await;

        time::sleep(Duration::from_millis(200)).await;
    }

    // TODO: Re-enable this test after implementing multi equal message support
    #[ignore]
    #[tokio::test]
    async fn broker_manage_single_client_multi_equal_messages() {
        let (_servers, membership, directory, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let broker_address = "127.0.0.1:9000";
        let keychain = KeyChain::random();
        let broker_identity = keychain.keycard().identity();
        let connector = TestConnector::new(keychain, connector_map.clone());
        let session_connector = SessionConnector::new(connector);

        let _broker = Broker::new(
            membership.clone(),
            directory,
            broker_address,
            broker_identity,
            session_connector,
            BrokerSettings {
                pool_timeout: Duration::from_millis(10),
                totality_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        );

        let client = client_keychains[0].clone();
        let client = Client::new(0, client, membership.clone(), "127.0.0.1:9001");
        client.add_broker(broker_address).await.unwrap();
        let _delivery_record = client.broadcast([1u8; MESSAGE_SIZE]).await;
        let _delivery_record = client.broadcast([1u8; MESSAGE_SIZE]).await;
        let _delivery_record = client.broadcast([1u8; MESSAGE_SIZE]).await;

        time::sleep(Duration::from_millis(200)).await;
    }
}
