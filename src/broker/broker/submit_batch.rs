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

#[derive(Doom)]
enum TrySubmitError {
    #[doom(description("Failed to connect."))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Failed to serialize: {}", source))]
    #[doom(wrap(serialize_failed))]
    SerializeFailed { source: bincode::Error },
    #[doom(description("Witness error: server multisigned an unexpected batch root"))]
    WitnessError,
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
        mut witness_shard_sender: Option<OneshotSender<(Identity, MultiSignature)>>,
        mut witness: Promise<Certificate>,
        mut delivery_shard_sender: Option<OneshotSender<(Identity, DeliveryShard)>>,
        settings: BrokerSettings,
    ) {
        let mut agent = settings.submission_schedule.agent();

        loop {
            match Broker::try_submit_batch(
                worker,
                sequence,
                root,
                compressed_batch,
                &server,
                connector.as_ref(),
                &mut verify,
                &mut witness_shard_sender,
                &mut witness,
                &mut delivery_shard_sender,
            )
            .await
            {
                Ok(_) => break,
                Err(error) => {
                    warn!("{:?}", error);
                }
            }

            agent.step().await;
        }
    }

    async fn try_submit_batch(
        worker: Identity,
        sequence: u64,
        root: Hash,
        compressed_batch: &CompressedBatch,
        server: &KeyCard,
        connector: &SessionConnector,
        verify: &mut Promise<bool>,
        witness_shard_sender: &mut Option<OneshotSender<(Identity, MultiSignature)>>,
        witness: &mut Promise<Certificate>,
        delivery_shard_sender: &mut Option<OneshotSender<(Identity, DeliveryShard)>>,
    ) -> Result<(), Top<TrySubmitError>> {
        debug!("Submitting batch (worker {worker:?}, sequence {sequence})");

        let mut session = connector
            .connect(server.identity())
            .await
            .pot(TrySubmitError::ConnectFailed, here!())?;

        session
            .send_plain(&(sequence, root))
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        let raw_batch = bincode::serialize(&compressed_batch)
            .map_err(TrySubmitError::serialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

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

        if witness_shard_sender.is_some() && // Otherwise, either i) not even a verifier or backup verifier, or ii) have already verified and sent the witness shard
            verify
        // Otherwise, not needed to verify
        {
            session
                .send_raw(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard = session
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard_sender = witness_shard_sender.take().unwrap();

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
                    .pot(TrySubmitError::WitnessError, here!())?;

                let _ = witness_shard_sender.send((server.identity(), witness_shard));
            }
        } else {
            session
                .send_raw(&false)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            session
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;
        }

        let witness = if let Some(witness) = witness.as_ref().await {
            witness
        } else {
            // `Broker` has dropped. Idle waiting for task to be cancelled.
            // (Note that `return`ing something meaningful is not possible)
            std::future::pending().await
        };

        session
            .send_raw(witness)
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
            .receive_plain::<DeliveryShard>()
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        let _ = delivery_shard_sender
            .take()
            .unwrap()
            .send((server.identity(), delivery_shard));

        session.end();
        Ok(())
    }
}
