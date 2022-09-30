use crate::{
    broadcast::{CompressedBatch, DeliveryShard},
    broker::Broker,
    crypto::{statements::BatchWitness, Certificate},
    debug, warn, BrokerSettings,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::sync::Arc;
use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        Identity, KeyCard,
    },
    net::SessionConnector,
    sync::promise::Promise,
};
use tokio::sync::oneshot::Sender as OneshotSender;

type MultiSignatureInlet = OneshotSender<(Identity, MultiSignature)>;
type DeliveryShardInlet = OneshotSender<(Identity, DeliveryShard)>;

#[derive(Doom)]
enum TrySubmitError {
    #[doom(description("Failed to connect"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Witness shard invalid"))]
    InvalidWitnessShard,
}

impl Broker {
    pub(in crate::broker::broker) async fn submit_batch(
        worker: Identity,
        sequence: u64,
        root: Hash,
        compressed_batch: &CompressedBatch,
        server: &KeyCard,
        connector: Arc<SessionConnector>,
        mut verify: Promise<bool>,
        mut witness_shard_inlet: Option<MultiSignatureInlet>,
        mut witness: Promise<Certificate>,
        mut delivery_shard_inlet: Option<DeliveryShardInlet>,
        settings: BrokerSettings,
    ) {
        let raw_batch = bincode::serialize(&compressed_batch).unwrap();

        let mut agent = settings.submission_schedule.agent();

        while let Err(error) = Broker::try_submit_batch(
            worker,
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

    async fn try_submit_batch(
        worker: Identity,
        sequence: u64,
        root: Hash,
        raw_batch: &[u8],
        server: &KeyCard,
        connector: &SessionConnector,
        verify: &mut Promise<bool>,
        witness_shard_inlet: &mut Option<MultiSignatureInlet>,
        witness: &mut Promise<Certificate>,
        delivery_shard_inlet: &mut Option<DeliveryShardInlet>,
    ) -> Result<(), Top<TrySubmitError>> {
        debug!("Submitting batch (worker {worker:?}, sequence {sequence})");

        let mut session = connector
            .connect(server.identity())
            .await
            .pot(TrySubmitError::ConnectFailed, here!())?;

        session
            .send(&(sequence, root))
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        session
            .send_raw_bytes(&raw_batch)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        let verify = if let Some(verify) = verify.as_ref().await {
            *verify
        } else {
            // `Broker` has dropped. Idle waiting for task to be cancelled.
            // (Note that `return`ing something meaningful is not possible)
            std::future::pending().await
        };

        if verify && witness_shard_inlet.is_some()
        // Otherwise either i) not needed to verify, or ii) have already verified and sent the witness shard
        {
            session
                .send(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard = session
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard_inlet = witness_shard_inlet.take().unwrap();

            if let Some(witness_shard) = witness_shard {
                witness_shard
                    .verify(
                        [server],
                        &BatchWitness {
                            broker: &worker,
                            sequence: &sequence,
                            root: &root,
                        },
                    )
                    .pot(TrySubmitError::InvalidWitnessShard, here!())?;

                let _ = witness_shard_inlet.send((server.identity(), witness_shard));
            }
        } else {
            session
                .send(&false)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            session
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            witness_shard_inlet.take();
        }

        let witness = if let Some(witness) = witness.as_ref().await {
            witness
        } else {
            // `Broker` has dropped. Idle waiting for task to be cancelled.
            // (Note that `return`ing something meaningful is not possible)
            std::future::pending().await
        };

        session
            .send(witness)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        // Here we must authenticate. Alternatively we could apply the shard's amendments
        // to the current batch, calculate the new root, and verify the multisignature
        // against it. For this we would need to clone the batch entries (costly) as the
        // amendments might be wrong (injected message). It's easier to just authenticate
        // and let the aggregation method (Broker::broadcast) figure out the correct set
        // of amendments by looking for a plurality of equal elements (one of which must
        // come from a correct server)
        let delivery_shard = session
            .receive::<DeliveryShard>()
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        let _ = delivery_shard_inlet
            .take()
            .unwrap()
            .send((server.identity(), delivery_shard));

        session.end();
        Ok(())
    }
}
