use crate::{
    broadcast::DeliveryShard,
    crypto::{statements::BatchWitness, Certificate},
    debug,
    order::Order,
    server::{Batch, BrokerSlot, Deduplicator, ServerSettings, TotalityManager},
    system::{Directory, Membership},
    warn, Entry,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::{primitives::hash::Hash, Identity, KeyChain},
    net::{Connector, Listener, Session, SessionListener},
    sync::fuse::Fuse,
};
use tokio::{
    sync::{mpsc, mpsc::Receiver as MpscReceiver, Semaphore},
    task,
};

type BurstOutlet = MpscReceiver<Vec<Option<Entry>>>;

pub struct Server {
    next_batch_outlet: BurstOutlet,
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
    #[doom(description("Old batch sequence number"))]
    OldBatchSequence,
    #[doom(description("Future batch sequence number"))]
    FutureBatchSequence,
    #[doom(description("Authenticated root does not match that of the batch provided"))]
    RootMismatch,
}

impl Server {
    pub fn new<B, BL, TC, TL>(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: B,
        broker_listener: BL,
        totality_connector: TC,
        totality_listener: TL,
        settings: ServerSettings,
    ) -> Self
    where
        B: Order,
        BL: Listener,
        TC: Connector,
        TL: Listener,
    {
        // Preprocess arguments

        let broadcast = Arc::new(broadcast);
        let broker_listener = SessionListener::new(broker_listener);

        // Initialize components

        let broker_slots = Arc::new(Mutex::new(HashMap::new()));

        let totality_manager = TotalityManager::new(
            membership.clone(),
            totality_connector,
            totality_listener,
            Default::default(),
        );

        let deduplicator = Deduplicator::with_capacity(directory.capacity(), Default::default());

        // Initialize channels

        let (next_batch_inlet, next_batch_outlet) =
            mpsc::channel(settings.next_batch_channel_capacity);

        // Spawn tasks

        let fuse = Fuse::new();

        {
            let keychain = keychain.clone();
            let membership = membership.clone();
            let broadcast = broadcast.clone();
            let broker_slots = broker_slots.clone();
            let settings = settings.clone();

            fuse.spawn(async move {
                Server::listen(
                    keychain,
                    membership,
                    directory,
                    broadcast,
                    broker_slots,
                    broker_listener,
                    settings,
                )
                .await;
            });
        }

        {
            let broker_slots = broker_slots.clone();

            fuse.spawn(async move {
                Server::deliver(
                    keychain,
                    membership,
                    broadcast,
                    broker_slots,
                    totality_manager,
                    deduplicator,
                    next_batch_inlet,
                )
                .await;
            });
        }

        // Assemble `Server`

        Server {
            next_batch_outlet,
            _fuse: fuse,
        }
    }

    pub async fn next_batch(&mut self) -> impl Iterator<Item = Entry> {
        // The `Fuse` to `Server::deliver` is owned by `self`,
        // so `next_batch_inlet` cannot have been dropped

        self.next_batch_outlet
            .recv()
            .await
            .unwrap()
            .into_iter()
            .flatten()
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Order>,
        broker_slots: Arc<Mutex<HashMap<Identity, BrokerSlot>>>,
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
        broker_slots: Arc<Mutex<HashMap<Identity, BrokerSlot>>>,
        semaphore: Arc<Semaphore>,
    ) -> Result<(), Top<ServeError>> {
        // Receive broker request

        let (sequence, root) = session
            .receive_plain::<(u64, Hash)>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let raw_batch = session
            .receive_raw_bytes()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let compressed_batch = bincode::deserialize(raw_batch.as_slice())
            .map_err(ServeError::deserialize_failed)
            .map_err(Doom::into_top)
            .spot(here!())?;

        let verify = session
            .receive_raw::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        // Reject request if `sequence` is too old or future

        let next_sequence = broker_slots
            .lock()
            .unwrap()
            .entry(broker.clone())
            .or_default()
            .next_sequence;

        if sequence + 1 < next_sequence {
            session.end();
            return ServeError::OldBatchSequence.fail().spot(here!());
        }

        if sequence > next_sequence {
            return ServeError::FutureBatchSequence.fail().spot(here!());
        }

        // Expand `compressed_batch`

        let batch = {
            let _permit = semaphore.acquire().await.unwrap(); // This limits concurrent expansion tasks

            task::spawn_blocking(move || {
                if verify {
                    Batch::expand_verified(&directory, compressed_batch)
                } else {
                    Batch::expand_unverified(compressed_batch)
                }
            })
            .await
            .unwrap()
            .pot(ServeError::BatchInvalid, here!())?
        };

        // Reject request if `batch.root()` does not match `root`. In case of
        // root mismatch, either the broker is Byzantine, or some miscommunication
        // happened while `send_raw_bytes()`ing `raw_batch` (e.g., malicious
        // routing node). In either case, a witness cannot be produced:
        //  - On `root`, as the broker never provided a batch whose root is `root`;
        //  - On `batch.root()`, as the broker never authenticated its intention
        //    to submit a batch with root `batch.root()`.
        // Remark: `batch_raw` is sent unauthenticated to save hashing (computing
        // `batch_raw`'s Merkle tree is sufficient to authenticate `batch`).

        if root != batch.root() {
            return ServeError::RootMismatch.fail().spot(here!());
        }

        // Store `raw_batch` and `batch`, retrieve a copy of the delivery shard outlet

        let mut delivery_shard_watch;

        let store = {
            let mut broker_slots = broker_slots.lock().unwrap();
            let mut broker_slot = broker_slots.entry(broker).or_default();
            delivery_shard_watch = broker_slot.last_delivery_shard.subscribe();

            if sequence == broker_slot.next_sequence {
                if broker_slot.expected_batch.is_none() {
                    broker_slot.expected_batch = Some((raw_batch, batch));
                    true
                } else {
                    broker_slot.expected_batch.as_ref().unwrap().1.root() == root
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
                .multisign(&BatchWitness {
                    broker: &broker,
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

mod deliver;

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

        let (root, compressed_batch) = null_batch(&client_keychains, 1);
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

            session.send_plain(&(0u64, root)).await.unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send_raw(&true).await.unwrap();

            let response = session
                .receive::<Option<MultiSignature>>()
                .await
                .unwrap()
                .unwrap();

            responses.push((*identity, response));
        }

        for (_, session) in sessions[2..].iter_mut() {
            let raw_batch = bincode::serialize(&compressed_batch).unwrap();

            session.send_plain(&(0u64, root)).await.unwrap();
            session.send_raw_bytes(&raw_batch).await.unwrap();
            session.send_raw(&false).await.unwrap();

            session.receive::<Option<MultiSignature>>().await.unwrap();
        }

        let mut batch = Batch::expand_unverified(compressed_batch).unwrap();

        for (identity, multisignature) in responses.iter() {
            let statement = BatchWitness {
                broker: &broker.keycard().identity(),
                sequence: &0,
                root: &root,
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
