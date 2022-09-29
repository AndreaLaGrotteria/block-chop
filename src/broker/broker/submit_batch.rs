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
};
use tokio::sync::{oneshot::Sender as OneshotSender, watch::Receiver as WatchReceiver};

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
    pub(in crate::broker::broker) async fn submit(
        compressed_batch: &CompressedBatch,
        worker: Identity,
        sequence: u64,
        expected_root: Hash,
        server: &KeyCard,
        connector: Arc<SessionConnector>,
        verify_receiver: WatchReceiver<bool>,
        mut witness_shard_sender: Option<OneshotSender<(Identity, MultiSignature)>>,
        witness_receiver: WatchReceiver<Option<Certificate>>,
        mut delivery_shard_sender: Option<OneshotSender<(Identity, DeliveryShard)>>,
        settings: BrokerSettings,
    ) {
        let mut agent = settings.submission_schedule.agent();

        loop {
            match Broker::try_submit(
                compressed_batch,
                worker,
                sequence,
                expected_root,
                &server,
                connector.as_ref(),
                verify_receiver.clone(),
                &mut witness_shard_sender,
                witness_receiver.clone(),
                &mut delivery_shard_sender,
            )
            .await
            {
                Err(error) => {
                    warn!("{:?}", error);
                }
                Ok(_) => break,
            }

            agent.step().await;
        }
    }

    async fn try_submit(
        compressed_batch: &CompressedBatch,
        worker: Identity,
        sequence: u64,
        expected_root: Hash,
        server: &KeyCard,
        connector: &SessionConnector,
        mut verify_receiver: WatchReceiver<bool>,
        witness_shard_sender: &mut Option<OneshotSender<(Identity, MultiSignature)>>,
        mut witness_receiver: WatchReceiver<Option<Certificate>>,
        delivery_shard_sender: &mut Option<OneshotSender<(Identity, DeliveryShard)>>,
    ) -> Result<(), Top<TrySubmitError>> {
        debug!("Submitting batch (worker {worker:?}, sequence {sequence})");

        let mut session = connector
            .connect(server.identity())
            .await
            .pot(TrySubmitError::ConnectFailed, here!())?;

        session
            .send_plain(&(sequence, expected_root))
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

        if witness_shard_sender.is_some()  // Otherwise, either i) not even a verifier or backup verifier, or ii) have already verified and sent the witness shard
            && verify_receiver.changed().await.is_ok()  // Otherwise, submission process is over but must still push the batch and its witness
            && *verify_receiver.borrow()
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
                            root: &expected_root,
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

        // If `changed()` returns an `Err`, this means that `witness_sender` was
        // dropped. However, before being dropped, `witness_sender` always sends
        // the witness, which means that `witness` will be available both if
        // `witness_sender` returns `Ok` (the witness was sent and the sender
        // is still alive) or `Err` (the witness was sent and the sender was
        // dropped).
        let _ = witness_receiver.changed().await;

        let witness = witness_receiver.borrow().clone().unwrap();

        session
            .send_raw(&witness)
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
