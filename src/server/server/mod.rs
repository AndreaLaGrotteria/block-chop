use crate::{
    broadcast::{CompressedBatch, DeliveryShard},
    crypto::{
        statements::{BatchDelivery, BatchWitness},
        Certificate,
    },
    debug,
    order::Order,
    server::{
        deduplicator::Deduplicator, server::deliver::AmendedDelivery, Batch, BatchError,
        ServerSettings,
    },
    system::{Directory, Membership},
    Entry,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::sync::Arc;

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, KeyChain},
    net::{Session, SessionListener},
    sync::fuse::Fuse,
};

use tokio::{
    sync::{
        mpsc,
        mpsc::{Receiver as MpscReceiver, Sender as MpscSender},
        oneshot,
        oneshot::Sender as OneshotSender,
        Semaphore,
    },
    task,
};

type BatchInlet = MpscSender<(Batch, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<AmendedDelivery>;
type ApplyOutlet = MpscReceiver<Batch>;

pub struct Server {
    apply_outlet: ApplyOutlet,
    _fuse: Fuse,
}

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Batch invalid"))]
    BatchInvalid,
    #[doom(description("Witness invalid"))]
    WitnessInvalid,
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
        let broadcast = Arc::new(broadcast);
        let fuse = Fuse::new();

        let (batch_inlet, batch_outlet) = mpsc::channel(settings.batch_channel_capacity);

        {
            let membership = membership.clone();
            let broadcast = broadcast.clone();
            let settings = settings.clone();

            fuse.spawn(async move {
                Server::listen(
                    keychain,
                    membership,
                    directory,
                    broadcast,
                    batch_inlet,
                    listener,
                    settings,
                )
                .await;
            });
        }

        let deduplicator = Deduplicator::new();
        let (apply_inlet, apply_outlet) = mpsc::channel(settings.apply_channel_capacity);

        fuse.spawn(async move {
            Server::deliver(
                membership,
                broadcast,
                batch_outlet,
                deduplicator,
                apply_inlet,
            )
            .await;
        });

        Server {
            apply_outlet,
            _fuse: fuse,
        }
    }

    pub async fn next_batch(&mut self) -> Vec<Entry> {
        Vec::from(self.apply_outlet.recv().await.unwrap().entries)
            .into_iter()
            .flat_map(|entry| entry)
            .collect()
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Order>,
        batch_inlet: BatchInlet,
        mut listener: SessionListener,
        settings: ServerSettings,
    ) {
        let membership = Arc::new(membership);
        let directory = Arc::new(directory);
        let batch_inlet = Arc::new(batch_inlet);

        let semaphore = Semaphore::new(settings.serve_tasks);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (_, session) = listener.accept().await;

            let keychain = keychain.clone();
            let membership = membership.clone();
            let directory = directory.clone();
            let broadcast = broadcast.clone();
            let batch_inlet = batch_inlet.clone();
            let semaphore = semaphore.clone();

            fuse.spawn(async move {
                if let Err(error) = Server::serve(
                    keychain,
                    membership,
                    directory,
                    broadcast,
                    batch_inlet,
                    semaphore,
                    session,
                )
                .await
                {
                    println!("{:?}", error);
                }
            });
        }
    }

    async fn serve(
        keychain: KeyChain,
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        broadcast: Arc<dyn Order>,
        batch_inlet: Arc<BatchInlet>,
        semaphore: Arc<Semaphore>,
        mut session: Session,
    ) -> Result<(), Top<ServeError>> {
        let compressed_batch = session
            .receive_raw::<CompressedBatch>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let verify = session
            .receive_raw::<bool>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        let (batch, witness_shard) = {
            let keychain = keychain.clone();
            let _permit = semaphore.acquire().await.unwrap();

            task::spawn_blocking(
                move || -> Result<(Batch, Option<MultiSignature>), Top<BatchError>> {
                    if verify {
                        let batch = Batch::expand_verified(&directory, compressed_batch)?;

                        let witness_shard = keychain
                            .multisign(&BatchWitness::new(batch.root()))
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

        if let Some(witness_shard) = witness_shard {
            session
                .send_raw::<MultiSignature>(&witness_shard)
                .await
                .pot(ServeError::ConnectionError, here!())?;
        }

        let witness = session
            .receive_raw::<Certificate>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        witness
            .verify_plurality(membership.as_ref(), &BatchWitness::new(root))
            .pot(ServeError::WitnessInvalid, here!())?;

        debug!("Certificate valid!");

        // Sending batch to be processed upon TOB delivery

        let (delivery_inlet, delivery_outlet) = oneshot::channel();
        let _ = batch_inlet.send((batch, delivery_inlet)).await;

        // Sending batch to be ordered by TOB

        let submission = bincode::serialize(&(root, witness)).unwrap();
        broadcast.order(submission.as_slice()).await;

        // Always Ok unless `Server` is shutting down
        if let Ok(AmendedDelivery {
            height,
            amended_root,
            amendments,
        }) = delivery_outlet.await
        {
            let statement = BatchDelivery {
                height: &height,
                root: &amended_root,
            };

            let multisignature = keychain.multisign(&statement).unwrap();

            let amended_delivery = DeliveryShard {
                height,
                amendments,
                multisignature,
            };

            session
                .send_plain::<DeliveryShard>(&amended_delivery)
                .await
                .pot(ServeError::ConnectionError, here!())?;

            debug!("Sent delivery shard!");
        }

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
        system::test::generate_system,
    };

    use std::collections::BTreeMap;

    use futures::stream::{FuturesUnordered, StreamExt};

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
            session.send_raw(&compressed_batch).await.unwrap();
            session.send_raw(&true).await.unwrap();

            let response: MultiSignature = session.receive_raw().await.unwrap();
            responses.push((*identity, response));
        }

        for (_, session) in sessions[2..].iter_mut() {
            session.send_raw(&compressed_batch).await.unwrap();
            session.send_raw(&false).await.unwrap();
        }

        let mut batch = Batch::expand_unverified(compressed_batch).unwrap();
        let batch_root = batch.root();

        for (identity, multisignature) in responses.iter() {
            let statement = BatchWitness::new(batch_root);
            let keycard = membership.servers().get(identity).unwrap();

            multisignature.verify([keycard], &statement).unwrap();
        }

        let certificate = Certificate::aggregate_plurality(&membership, responses);

        for (_, session) in sessions.iter_mut() {
            session.send_raw(&certificate).await.unwrap();
        }

        let mut responses = Vec::new();
        for (identity, session) in sessions.iter_mut() {
            let mut delivery_shard = session.receive_plain::<DeliveryShard>().await.unwrap();

            delivery_shard.amendments.sort();

            responses.push((*identity, delivery_shard));
        }

        let mut counts = BTreeMap::new();
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

        println!("Obtained delivery certificate!");
    }
}
