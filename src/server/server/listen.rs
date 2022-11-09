#[cfg(feature = "benchmark")]
use crate::heartbeat::{self, ServerEvent};
use crate::{
    broadcast::{Batch as BroadcastBatch, DeliveryShard},
    crypto::{statements::BatchWitness as BatchWitnessStatement, Certificate},
    debug,
    order::Order,
    server::{BrokerSlot, CompressedBatch, MerkleBatch, Server, ServerSettings, WitnessCache},
    system::{Directory, Membership},
    warn,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::{primitives::hash::Hash, Identity, KeyChain},
    net::{Session, SessionListener},
    sync::fuse::Fuse,
};
use tokio::{sync::Semaphore, task};

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
    #[doom(description("Old batch sequence number"))]
    OldBatchSequence,
    #[doom(description("Future batch sequence number"))]
    FutureBatchSequence,
    #[doom(description("Authenticated root does not match that of the batch provided"))]
    RootMismatch,
}

impl Server {
    pub(in crate::server::server) async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Order>,
        broker_slots: Arc<Mutex<HashMap<(Identity, u16), BrokerSlot>>>,
        witness_cache: Arc<Mutex<WitnessCache>>,
        mut listener: SessionListener,
        settings: ServerSettings,
    ) {
        let membership = Arc::new(membership);
        let directory = Arc::new(directory);

        let semaphore = Semaphore::new(settings.expand_tasks);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (broker, session) = listener.accept().await;

            let keychain = keychain.clone();
            let membership = membership.clone();
            let directory = directory.clone();
            let broadcast = broadcast.clone();
            let broker_slots = broker_slots.clone();
            let semaphore = semaphore.clone();
            let witness_cache = witness_cache.clone();

            fuse.spawn(async move {
                if let Err(error) = Server::serve(
                    broker,
                    session,
                    keychain,
                    membership,
                    directory,
                    broadcast,
                    broker_slots,
                    semaphore,
                    witness_cache,
                )
                .await
                {
                    warn!("{:?}", error);
                }
            });
        }
    }

    async fn serve(
        broker: Identity,
        mut session: Session,
        keychain: KeyChain,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        broadcast: Arc<dyn Order>,
        broker_slots: Arc<Mutex<HashMap<(Identity, u16), BrokerSlot>>>,
        semaphore: Arc<Semaphore>,
        witness_cache: Arc<Mutex<WitnessCache>>,
    ) -> Result<(), Top<ServeError>> {
        // Receive broker request

        let (worker, sequence, root) = session
            .receive::<(u16, u64, Hash)>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchAnnounced { root });

        let raw_batch = session
            .receive_raw_bytes()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        session.free_receive_buffer();

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchReceived { root });

        let broadcast_batch = bincode::deserialize::<BroadcastBatch>(raw_batch.as_slice())
            .map_err(ServeError::deserialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

        drop(raw_batch);

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchDeserialized {
            root,
            entries: broadcast_batch.messages.len() as u32,
            stragglers: broadcast_batch.stragglers.len() as u32,
        });

        let verify = session
            .receive::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // Reject request if `sequence` is too old or future

        let next_sequence = broker_slots
            .lock()
            .unwrap()
            .entry((broker, worker))
            .or_default()
            .next_sequence;

        if sequence + 1 < next_sequence {
            return ServeError::OldBatchSequence.fail().spot(here!());
        }

        if sequence > next_sequence {
            return ServeError::FutureBatchSequence.fail().spot(here!());
        }

        // Expand `broadcast_batch`

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchExpansionStarted { root, verify });

        let (broadcast_batch, merkle_batch) = {
            let _permit = semaphore.acquire().await.unwrap(); // This limits concurrent expansion tasks

            task::spawn_blocking(move || {
                let merkle_batch = if verify {
                    MerkleBatch::expand_verified(&directory, &broadcast_batch)
                } else {
                    MerkleBatch::expand_unverified(&broadcast_batch)
                };

                merkle_batch.map(|merkle_batch| (broadcast_batch, merkle_batch))
            })
            .await
            .unwrap()
            .pot(ServeError::BatchInvalid, here!())?
        };

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchExpansionCompleted { root });

        // Reject request if `merkle_batch.root()` does not match `root`. In case of
        // root mismatch, either the broker is Byzantine, or some miscommunication
        // happened while `send_raw_bytes()`ing `raw_batch` (e.g., malicious
        // routing node). In either case, a witness cannot be produced:
        //  - On `root`, as the broker never provided a batch whose root is `root`;
        //  - On `merkle_batch.root()`, as the broker never authenticated its intention
        //    to submit a batch with root `merkle_batch.root()`.
        // Remark: `batch_raw` is sent unauthenticated to save hashing (computing
        // `batch_raw`'s Merkle tree is sufficient to authenticate `merkle_batch`).

        if root != merkle_batch.root() {
            return ServeError::RootMismatch.fail().spot(here!());
        }

        // Compress and store `merkle_batch`, retrieve a copy of the delivery shard outlet

        let compressed_batch = CompressedBatch::from_broadcast(root, broadcast_batch);

        let mut last_delivery_shard;

        let store = {
            let mut broker_slots = broker_slots.lock().unwrap();
            let mut broker_slot = broker_slots.entry((broker, worker)).or_default();
            last_delivery_shard = broker_slot.last_delivery_shard.subscribe();

            if sequence == broker_slot.next_sequence {
                if broker_slot.expected_batch.is_none() {
                    broker_slot.expected_batch = Some(compressed_batch);
                    true
                } else {
                    // `broker_slot`'s `CompressedBatch` was never edited since
                    // compression: its `root()` is guaranteed to be `Some`
                    broker_slot.expected_batch.as_ref().unwrap().root().unwrap() == root
                }
            } else {
                false
            }
        };

        // Produce a witness shard if `verify` and `store`:
        //  - Because `root` was authenticated and `batch.root() == root`,
        //    batch can be safely attributed to the broker.
        //  - Because `verify`, `batch` was verified, so its correctness
        //    can be attested.
        //  - Because `store`, `batch` is the next expected batch and was
        //    stored, either anew or redundantly (i.e., `batch` was already
        //    stored in the broker's slot).

        let witness_shard = if verify && store {
            let witness_shard = keychain
                .multisign(&BatchWitnessStatement {
                    broker: &broker,
                    worker: &worker,
                    sequence: &sequence,
                    root: &root,
                })
                .unwrap();

            Some(witness_shard)
        } else {
            None
        };

        session
            .send(&witness_shard)
            .await
            .pot(ServeError::ConnectionError, here!())?;

        #[cfg(feature = "benchmark")]
        if witness_shard.is_some() {
            heartbeat::log(ServerEvent::BatchWitnessed { root });
        }

        // Receive and verify the witness certificate

        let witness = session
            .receive::<Certificate>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        witness
            .verify_plurality(
                membership.as_ref(),
                &BatchWitnessStatement {
                    broker: &broker,
                    worker: &worker,
                    sequence: &sequence,
                    root: &root,
                },
            )
            .pot(ServeError::WitnessInvalid, here!())?;

        witness_cache
            .lock()
            .unwrap()
            .store(&broker, &worker, &sequence, &root, &witness);

        debug!("Witness certificate valid.");

        // Submit the batch for ordering by Total-Order Broadcast (TOB)

        let submission = bincode::serialize(&(broker, worker, root, witness)).unwrap();
        broadcast.order(submission.as_slice()).await;

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchSubmitted { root });

        // Wait for the batch's delivery shard (produced after TOB-delivery and deduplication)

        let delivery_shard = loop {
            if let Some((last_sequence, last_shard)) =
                last_delivery_shard.borrow_and_update().as_ref()
            {
                if sequence == *last_sequence {
                    break last_shard.clone();
                } else if sequence < *last_sequence {
                    // A delivery shard was produced for a later batch than the one
                    // being processed. This means that the broker moved on, having
                    // already successfully attained a delivery shard for the current
                    // batch: break the connection and return.
                    return ServeError::OldBatchSequence.fail().spot(here!());
                }
            }

            let _ = last_delivery_shard.changed().await;
        };

        // Send `delivery_shard` to the broker and end the session

        session
            .send::<DeliveryShard>(&delivery_shard)
            .await
            .pot(ServeError::ConnectionError, here!())?;

        debug!("Delivery shard sent.");

        #[cfg(feature = "benchmark")]
        heartbeat::log(ServerEvent::BatchServed { root });

        session.end();

        Ok(())
    }
}

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
    use talk::{
        crypto::primitives::multi::Signature as MultiSignature,
        net::{test::TestConnector, SessionConnector},
    };

    #[tokio::test]
    async fn server_interact() {
        let (_servers, membership, _, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let connector = SessionConnector::new(connector);

        let (root, broadcast_batch) = null_batch(&client_keychains, 1);
        let ids = broadcast_batch.ids.uncram().unwrap();

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
            let raw_batch = bincode::serialize(&broadcast_batch).unwrap();

            session.send(&(0u16, 0u64, root)).await.unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send(&true).await.unwrap();

            let response = session
                .receive::<Option<MultiSignature>>()
                .await
                .unwrap()
                .unwrap();

            responses.push((*identity, response));
        }

        for (_, session) in sessions[2..].iter_mut() {
            let raw_batch = bincode::serialize(&broadcast_batch).unwrap();

            session.send(&(0u16, 0u64, root)).await.unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send(&false).await.unwrap();

            session.receive::<Option<MultiSignature>>().await.unwrap();
        }

        let mut batch = MerkleBatch::expand_unverified(&broadcast_batch).unwrap();

        for (identity, multisignature) in responses.iter() {
            let statement = BatchWitnessStatement {
                broker: &broker.keycard().identity(),
                worker: &0,
                sequence: &0,
                root: &root,
            };
            let keycard = membership.servers().get(identity).unwrap();

            multisignature.verify([keycard], &statement).unwrap();
        }

        let certificate = Certificate::aggregate_plurality(&membership, responses);

        for (_, session) in sessions.iter_mut() {
            session.send(&certificate).await.unwrap();
        }

        let mut responses = Vec::new();
        for (identity, session) in sessions.iter_mut() {
            let response = session.receive::<DeliveryShard>().await.unwrap();

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
                    let mut entry = batch.entries().items()[index].clone().unwrap();
                    entry.sequence = *sequence;
                    batch.entries_mut().set(index, Some(entry)).unwrap();
                }
                Amendment::Drop { id } => {
                    let index = ids.binary_search_by(|probe| probe.cmp(id)).unwrap();
                    batch.entries_mut().set(index, None).unwrap();
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
