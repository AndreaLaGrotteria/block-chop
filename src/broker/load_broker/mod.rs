use crate::{broker::LoadBrokerSettings, system::Membership};
use std::sync::Arc;
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::SessionConnector,
    sync::fuse::Fuse,
};

pub struct LoadBroker {
    _fuse: Fuse,
}

impl LoadBroker {
    pub fn new<B>(
        membership: Membership,
        broker_identity: Identity,
        connector: SessionConnector,
        batches: B,
        settings: LoadBrokerSettings,
    ) -> Self
    where
        B: IntoIterator<Item = (Hash, Vec<u8>)>,
    {
        // Build `Arc`s

        let membership = Arc::new(membership);

        // Spawn tasks

        let fuse = Fuse::new();

        fuse.spawn(LoadBroker::broadcast_batches(
            membership.clone(),
            broker_identity,
            connector,
            batches.into_iter().collect(),
            settings.clone(),
        ));

        LoadBroker { _fuse: fuse }
    }
}

mod broadcast_batch;
mod broadcast_batches;
mod submit_batch;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::{test::null_batch, Batch as BroadcastBatch},
        system::test::generate_system,
    };
    use std::time::Duration;
    use talk::{
        crypto::KeyChain,
        net::{test::TestConnector, SessionConnector},
    };
    use tokio::time;

    fn to_raw(val: (Hash, BroadcastBatch)) -> (Hash, Vec<u8>) {
        (val.0, bincode::serialize(&val.1).unwrap())
    }

    #[tokio::test]
    async fn broker_manage() {
        let (_servers, membership, _directory, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let keychain = KeyChain::random();
        let broker_identity = keychain.keycard().identity();
        let connector = TestConnector::new(keychain, connector_map.clone());
        let session_connector = SessionConnector::new(connector);

        let batches = std::iter::repeat(to_raw(null_batch(&client_keychains, 30))).take(50);

        let _load_broker = LoadBroker::new(
            membership.clone(),
            broker_identity,
            session_connector,
            batches,
            LoadBrokerSettings {
                rate: 16.,
                totality_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        );

        time::sleep(Duration::from_secs(10)).await;
    }
}
