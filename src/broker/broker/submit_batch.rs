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
use tokio::sync::mpsc::Sender as MpscSender;

type MultiSignatureInlet = MpscSender<(Identity, MultiSignature)>;
type DeliveryShardInlet = MpscSender<(Identity, DeliveryShard)>;

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
        witness_shard_inlet: MultiSignatureInlet,
        mut witness: Promise<Certificate>,
        delivery_shard_inlet: DeliveryShardInlet,
        settings: BrokerSettings,
    ) {
        let raw_batch = bincode::serialize(&compressed_batch).unwrap();
        let mut witness_shard_inlet = Some(witness_shard_inlet);
        let mut delivery_shard_inlet = Some(delivery_shard_inlet);

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

        // Send request

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

        // Request and process witness shard

        // Remark: if `witness_shard_inlet.is_none()`, then the batch was
        // successfully conveyed to the server in a previous attempt: a
        // (possibly invalid yet authentic) reply to the witness shard
        // request has already been processed.
        if verify && witness_shard_inlet.is_some() {
            session
                .send(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard = session
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard_inlet = witness_shard_inlet.take().unwrap();

            // Remark: `witness_shard` could be `None` even if `server`
            // is correct. If so, `server` previously received a witness
            // produced by the local broker. If so, no additional witness
            // shard is required for the submission to succeed.
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

                let _ = witness_shard_inlet
                    .send((server.identity(), witness_shard))
                    .await;
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

        // Send witness

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

        // Collect delivery shard

        let delivery_shard = session
            .receive::<DeliveryShard>()
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        let _ = delivery_shard_inlet
            .take()
            .unwrap()
            .send((server.identity(), delivery_shard))
            .await;

        session.end();
        Ok(())
    }
}
