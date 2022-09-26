use crate::{
    broadcast::DeliveryShard,
    crypto::{statements::BatchWitness, Certificate},
    debug,
    order::Order,
    server::{Batch, BatchError, Deduplicator, ServerSettings},
    system::{Directory, Membership},
    warn, Entry,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, Identity, KeyChain},
    net::{Session, SessionListener},
    sync::fuse::Fuse,
};
use tokio::{
    sync::{mpsc, mpsc::Receiver as MpscReceiver, Semaphore},
    task,
};

type ApplyOutlet = MpscReceiver<Vec<Option<Entry>>>;

pub struct Server {
    apply_outlet: ApplyOutlet,
    _fuse: Fuse,
}

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: bincode::Error },
    #[doom(description("Batch invalid"))]
    BatchInvalid,
    #[doom(description("Witness invalid"))]
    WitnessInvalid,
    #[doom(description("Old batch sequence"))]
    OldBatchSequence,
    #[doom(description("Far future batch sequence"))]
    FutureBatchSequence,
}

impl Server {
    pub fn new<B>(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: B,
        listener: SessionListener,
        settings: ServerSettings,
    ) -> Self
    where
        B: Order,
    {
        let brokers = Arc::new(Mutex::new(HashMap::<Identity, BrokerState>::new()));
        let directory_capacity = directory.capacity();

        let keychain = Arc::new(keychain);
        let broadcast = Arc::new(broadcast);
        let fuse = Fuse::new();

        {
            let keychain = keychain.clone();
            let membership = membership.clone();
            let broadcast = broadcast.clone();
            let settings = settings.clone();
            let brokers = brokers.clone();

            fuse.spawn(async move {
                Server::listen(
                    keychain, membership, directory, broadcast, listener, brokers, settings,
                )
                .await;
            });
        }

        let deduplicator = Deduplicator::with_capacity(directory_capacity, Default::default());
        let (apply_inlet, apply_outlet) = mpsc::channel(settings.apply_channel_capacity);

        {
            let brokers = brokers.clone();

            fuse.spawn(async move {
                Server::deliver(
                    keychain,
                    membership,
                    broadcast,
                    brokers,
                    deduplicator,
                    apply_inlet,
                )
                .await;
            });
        }

        Server {
            apply_outlet,
            _fuse: fuse,
        }
    }

    pub async fn next_batch(&mut self) -> impl Iterator<Item = Entry> {
        self.apply_outlet
            .recv()
            .await
            .unwrap()
            .into_iter()
            .flatten()
    }

    async fn listen(
        keychain: Arc<KeyChain>,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Order>,
        mut listener: SessionListener,
        brokers: Arc<Mutex<HashMap<Identity, BrokerState>>>,
        settings: ServerSettings,
    ) {
        let membership = Arc::new(membership);
        let directory = Arc::new(directory);

        let semaphore = Semaphore::new(settings.serve_tasks);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (broker, session) = listener.accept().await;

            let keychain = keychain.clone();
            let membership = membership.clone();
            let directory = directory.clone();
            let broadcast = broadcast.clone();
            let semaphore = semaphore.clone();
            let brokers = brokers.clone();

            fuse.spawn(async move {
                if let Err(error) = Server::serve(
                    keychain, membership, directory, broadcast, semaphore, session, broker, brokers,
                )
                .await
                {
                    warn!("{:?}", error);
                }
            });
        }
    }

    async fn serve(
        keychain: Arc<KeyChain>,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        broadcast: Arc<dyn Order>,
        semaphore: Arc<Semaphore>,
        mut session: Session,
        broker: Identity,
        brokers: Arc<Mutex<HashMap<Identity, BrokerState>>>,
    ) -> Result<(), Top<ServeError>> {
        let raw_batch = session
            .receive_raw_bytes()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let compressed_batch = bincode::deserialize(&raw_batch)
            .map_err(ServeError::deserialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

        let sequence = session
            .receive_raw::<u64>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let verify = session
            .receive_raw::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let expected_sequence = brokers
            .lock()
            .unwrap()
            .entry(broker.clone())
            .or_insert(Default::default())
            .next_sequence;

        if sequence > expected_sequence {
            session.end();
            return ServeError::FutureBatchSequence.fail().spot(here!());
        }

        // Now guaranteed: sequence <= expected_sequence

        if sequence + 1 < expected_sequence {
            session.end();
            return ServeError::OldBatchSequence.fail().spot(here!());
        }

        // Now guaranteed: sequence == expected_sequence OR sequence == (expected_sequence - 1)

        let (batch, witness_shard) = {
            let keychain = keychain.clone();
            let _permit = semaphore.acquire().await.unwrap();

            task::spawn_blocking(
                move || -> Result<(Batch, Option<MultiSignature>), Top<BatchError>> {
                    if verify {
                        let batch = if sequence == expected_sequence {
                            // New batch, must verify
                            Batch::expand_verified(&directory, compressed_batch)?
                        } else {
                            // Already ordered, consistency guaranteed by TOB
                            Batch::expand_unverified(compressed_batch)?
                        };

                        let witness_shard = keychain
                            .multisign(&BatchWitness {
                                broker: &broker,
                                sequence: &sequence,
                                root: &batch.root(),
                            })
                            .unwrap();

                        Ok((batch, Some(witness_shard)))
                    } else {
                        let batch = Batch::expand_unverified(compressed_batch)?;

                        Ok((batch, None))
                    }
                },
            )
            .await
            .unwrap()
            .pot(ServeError::BatchInvalid, here!())?
        };

        let root = batch.root();

        // Store the expanded batch (and its raw format) in the `brokers` map

        let mut delivery_shard_watch = {
            let mut guard = brokers.lock().unwrap();

            let mut broker_state = guard.entry(broker.clone()).or_insert(Default::default());

            if sequence == broker_state.next_sequence && broker_state.expected_batch.is_none() {
                broker_state.expected_batch = Some((raw_batch, batch));
            }

            broker_state.last_delivery_shard.subscribe()
        };

        // Send the witness shard to the broker

        if let Some(witness_shard) = witness_shard {
            session
                .send_raw::<MultiSignature>(&witness_shard)
                .await
                .pot(ServeError::ConnectionError, here!())?;
        }

        // Receive and validate the witness certificate

        let witness = session
            .receive_raw::<Certificate>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        witness
            .verify_plurality(
                membership.as_ref(),
                &BatchWitness {
                    broker: &broker,
                    sequence: &sequence,
                    root: &root,
                },
            )
            .pot(ServeError::WitnessInvalid, here!())?;

        debug!("Witness certificate valid!");

        // Submit the batch to be ordered by TOB

        let submission = bincode::serialize(&(broker, root, witness)).unwrap();
        broadcast.order(submission.as_slice()).await;

        // Wait for the batch's delivery by TOB and the corresponding delivery shard

        let delivery_shard = loop {
            if let Some((last_sequence, delivery_shard)) =
                delivery_shard_watch.borrow_and_update().as_ref()
            {
                if sequence == *last_sequence {
                    break delivery_shard.clone();
                } else if sequence < *last_sequence {
                    session.end();
                    return ServeError::OldBatchSequence.fail().spot(here!());
                }
            }

            let _ = delivery_shard_watch.changed().await;
        };

        // Send the delivery shard to the broker

        session
            .send_plain::<DeliveryShard>(&delivery_shard)
            .await
            .pot(ServeError::ConnectionError, here!())?;

        debug!("Sent delivery shard!");

        session.end();

        Ok(())
    }
}

mod broker_state;
mod deliver;

use broker_state::BrokerState;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        broadcast::{test::null_batch, Amendment},
        crypto::statements::BatchDelivery,
        system::test::generate_system,
    };

    use futures::stream::{FuturesUnordered, StreamExt};

    use std::collections::HashMap;

    use talk::net::{test::TestConnector, SessionConnector};

    #[tokio::test]
    async fn server_interact() {
        let (_servers, membership, _, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let connector = SessionConnector::new(connector);

        let compressed_batch = null_batch(&client_keychains, 1);
        let ids = compressed_batch.ids.uncram().unwrap();

        let mut sessions = membership
            .servers()
            .keys()
            .map(|server| async {
                (
                    server.clone(),
                    connector.connect(server.clone()).await.unwrap(),
                )
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        let mut responses = Vec::new();
        for (identity, session) in sessions[0..2].iter_mut() {
            let raw_batch = bincode::serialize(&compressed_batch).unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send_raw(&0u64).await.unwrap();
            session.send_raw(&true).await.unwrap();

            let response: MultiSignature = session.receive_raw().await.unwrap();
            responses.push((*identity, response));
        }

        for (_, session) in sessions[2..].iter_mut() {
            let raw_batch = bincode::serialize(&compressed_batch).unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send_raw(&0u64).await.unwrap();
            session.send_raw(&false).await.unwrap();
        }

        let mut batch = Batch::expand_unverified(compressed_batch).unwrap();
        let batch_root = batch.root();

        for (identity, multisignature) in responses.iter() {
            let statement = BatchWitness {
                broker: &broker.keycard().identity(),
                sequence: &0,
                root: &batch_root,
            };
            let keycard = membership.servers().get(identity).unwrap();

            multisignature.verify([keycard], &statement).unwrap();
        }

        let certificate = Certificate::aggregate_plurality(&membership, responses);

        for (_, session) in sessions.iter_mut() {
            session.send_raw(&certificate).await.unwrap();
        }

        let mut responses = Vec::new();
        for (identity, session) in sessions.iter_mut() {
            let response = session.receive_plain::<DeliveryShard>().await.unwrap();

            responses.push((*identity, response));
        }

        let mut counts = HashMap::new();
        for core_response in responses.iter_mut().map(|x| (&x.1.amendments, &x.1.height)) {
            *counts
                .entry((core_response.0, core_response.1))
                .or_insert(0) += 1;
        }

        let ((amendments, height), _) = counts.into_iter().max_by_key(|&(_, count)| count).unwrap();
        let amendments = amendments.clone();
        let height = *height;

        for amendment in amendments.iter() {
            match amendment {
                Amendment::Nudge { id, sequence } => {
                    let index = ids.binary_search_by(|probe| probe.cmp(id)).unwrap();
                    let mut entry = batch.entries.items()[index].clone().unwrap();
                    entry.sequence = *sequence;
                    batch.entries.set(index, Some(entry)).unwrap();
                }
                Amendment::Drop { id } => {
                    let index = ids.binary_search_by(|probe| probe.cmp(id)).unwrap();
                    batch.entries.set(index, None).unwrap();
                }
            }
        }

        let good_responses = responses.iter().filter_map(|(identity, delivery_shard)| {
            if (&delivery_shard.amendments, delivery_shard.height) == (&amendments, height) {
                Some((*identity, delivery_shard.multisignature))
            } else {
                None
            }
        });

        let statement = BatchDelivery {
            height: &height,
            root: &batch.root(),
        };

        let certificate = Certificate::aggregate_quorum(&membership, good_responses);

        certificate.verify_quorum(&membership, &statement).unwrap();
    }
}
