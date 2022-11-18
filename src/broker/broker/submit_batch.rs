use crate::{
    broadcast::{Batch as BroadcastBatch, DeliveryShard},
    broker::Broker,
    crypto::{statements::BatchWitness, Certificate},
    debug,
    heartbeat::{self, BrokerEvent},
    warn, BrokerSettings,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{collections::HashMap, sync::Arc};
use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        Identity, KeyCard,
    },
    net::{MultiplexId, PlexConnector},
    sync::{board::Board, promise::Promise},
};
use tokio::sync::mpsc::Sender as MpscSender;

type MultiSignatureInlet = MpscSender<(Identity, MultiSignature)>;
type DeliveryShardInlet = MpscSender<(Identity, DeliveryShard)>;

#[derive(Doom)]
pub(in crate::broker) enum TrySubmitError {
    #[doom(description("Failed to connect"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Witness shard invalid"))]
    InvalidWitnessShard,
}

impl Broker {
    pub(in crate::broker::broker) async fn submit_batch(
        broker_identity: Identity,
        worker_index: u16,
        sequence: u64,
        root: Hash,
        broadcast_batch: Arc<BroadcastBatch>,
        server: KeyCard,
        connector: Arc<PlexConnector>,
        mut verify: Promise<bool>,
        witness_shard_inlet: MultiSignatureInlet,
        mut witness: Board<Certificate>,
        delivery_shard_inlet: DeliveryShardInlet,
        settings: BrokerSettings,
    ) {
        let raw_batch = bincode::serialize(broadcast_batch.as_ref()).unwrap();

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
            None,
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

    pub(in crate::broker) async fn try_submit_batch(
        broker_identity: Identity,
        worker_index: u16,
        sequence: u64,
        root: Hash,
        raw_batch: &[u8],
        server: &KeyCard,
        connector: &PlexConnector,
        affinities: Option<&HashMap<Identity, MultiplexId>>,
        verify: &mut Promise<bool>,
        witness_shard_inlet: &mut Option<MultiSignatureInlet>,
        witness: &mut Board<Certificate>,
        delivery_shard_inlet: &mut Option<DeliveryShardInlet>,
    ) -> Result<(), Top<TrySubmitError>> {
        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::SubmissionStarted {
            root,
            server: server.identity(),
        });

        debug!("Submitting batch (worker {worker_index}, sequence {sequence})");

        // Send request

        let mut plex = if let Some(affinities) = affinities {
            let (multiplex_id, plex) = connector
                .connect_with_affinity(server.identity(), affinities[&server.identity()])
                .await
                .pot(TrySubmitError::ConnectFailed, here!())?;

            if multiplex_id != affinities[&server.identity()] {
                warn!("Impossible to establish `Plex` with desired affinity.");
            }

            plex
        } else {
            connector
                .connect(server.identity())
                .await
                .pot(TrySubmitError::ConnectFailed, here!())?
        };

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::ServerConnected {
            root,
            server: server.identity(),
        });

        plex.send(&(worker_index, sequence, root))
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        plex.send_raw_bytes(&raw_batch)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::BatchSent {
            root,
            server: server.identity(),
        });

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
            plex.send(&true)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            #[cfg(feature = "benchmark")]
            heartbeat::log(BrokerEvent::WitnessShardRequested {
                root,
                server: server.identity(),
            });

            let witness_shard = plex
                .receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            let witness_shard_inlet = witness_shard_inlet.take().unwrap();

            // Remark: `witness_shard` could be `None` even if `server`
            // is correct. If so, `server` previously received a witness
            // produced by the local broker. If so, no additional witness
            // shard is required for the submission to succeed.
            if let Some(witness_shard) = witness_shard {
                #[cfg(feature = "benchmark")]
                heartbeat::log(BrokerEvent::WitnessShardReceived {
                    root,
                    server: server.identity(),
                });

                witness_shard
                    .verify(
                        [server],
                        &BatchWitness {
                            broker: &broker_identity,
                            worker: &worker_index,
                            sequence: &sequence,
                            root: &root,
                        },
                    )
                    .pot(TrySubmitError::InvalidWitnessShard, here!())?;

                #[cfg(feature = "benchmark")]
                heartbeat::log(BrokerEvent::WitnessShardVerified {
                    root,
                    server: server.identity(),
                });

                let _ = witness_shard_inlet
                    .send((server.identity(), witness_shard))
                    .await;
            }
        } else {
            plex.send(&false)
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            #[cfg(feature = "benchmark")]
            heartbeat::log(BrokerEvent::WitnessShardWaived {
                root,
                server: server.identity(),
            });

            plex.receive::<Option<MultiSignature>>()
                .await
                .pot(TrySubmitError::ConnectionError, here!())?;

            witness_shard_inlet.take();
        }

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::WitnessShardConcluded {
            root,
            server: server.identity(),
        });

        // Send witness

        let witness = if let Some(witness) = witness.as_ref().await {
            witness
        } else {
            // `Broker` has dropped. Idle waiting for task to be cancelled.
            // (Note that `return`ing something meaningful is not possible)
            std::future::pending().await
        };

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::WitnessAcquired {
            root,
            server: server.identity(),
        });

        plex.send(witness)
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::WitnessSent {
            root,
            server: server.identity(),
        });

        // Collect delivery shard

        let delivery_shard = plex
            .receive::<DeliveryShard>()
            .await
            .pot(TrySubmitError::ConnectionError, here!())?;

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::DeliveryShardReceived {
            root,
            server: server.identity(),
        });

        let _ = delivery_shard_inlet
            .take()
            .unwrap()
            .send((server.identity(), delivery_shard))
            .await;

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::SubmissionCompleted {
            root,
            server: server.identity(),
        });

        Ok(())
    }
}
