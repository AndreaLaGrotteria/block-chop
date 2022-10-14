use crate::{
    broadcast::DeliveryShard,
    broker::{Broker, LoadBroker, LoadBrokerSettings},
    crypto::Certificate,
    warn,
};
use std::sync::Arc;
use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        Identity, KeyCard,
    },
    net::SessionConnector,
    sync::{board::Board, promise::Promise},
};
use tokio::sync::mpsc::Sender as MpscSender;

type MultiSignatureInlet = MpscSender<(Identity, MultiSignature)>;
type DeliveryShardInlet = MpscSender<(Identity, DeliveryShard)>;

impl LoadBroker {
    pub(in crate::broker::load_broker) async fn submit_batch(
        broker_identity: Identity,
        worker_index: u16,
        sequence: u64,
        root: Hash,
        raw_batch: Arc<Vec<u8>>,
        server: KeyCard,
        connector: Arc<SessionConnector>,
        mut verify: Promise<bool>,
        witness_shard_inlet: MultiSignatureInlet,
        mut witness: Board<Certificate>,
        delivery_shard_inlet: DeliveryShardInlet,
        settings: LoadBrokerSettings,
    ) {
        let mut witness_shard_inlet = Some(witness_shard_inlet);
        let mut delivery_shard_inlet = Some(delivery_shard_inlet);

        let mut agent = settings.submission_schedule.agent();

        while let Err(error) = Broker::try_submit_batch(
            broker_identity,
            worker_index,
            sequence,
            root,
            raw_batch.as_slice(),
            &server,
            connector.as_ref(),
            &mut verify,
            &mut witness_shard_inlet,
            &mut witness,
            &mut delivery_shard_inlet,
        )
        .await
        {
            warn!("{:?}", error);
            agent.step().await;
        }
    }
}
