use crate::{
    broadcast::{CompressedBatch, DeliveryShard},
    crypto::{
        statements::{BatchDelivery, BatchWitness},
        Certificate,
    },
    server::{server::deliver::AmendedDelivery, Batch, BatchError, ServerSettings},
    system::{Directory, Membership},
    total_order::Broadcast,
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
        mpsc, mpsc::Sender as MpscSender, oneshot, oneshot::Sender as OneshotSender, Semaphore,
    },
    task,
};

type BatchInlet = MpscSender<(Batch, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<AmendedDelivery>;

pub struct Server {
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
        B: Broadcast,
    {
        let broadcast = Arc::new(broadcast);
        let fuse = Fuse::new();

        let (batch_inlet, batch_outlet) = mpsc::channel(settings.batch_channel_capacity);

        {
            let membership = membership.clone();
            let broadcast = broadcast.clone();

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

        fuse.spawn(async move {
            Server::deliver(membership, broadcast, batch_outlet).await;
        });

        Server { _fuse: fuse }
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        broadcast: Arc<dyn Broadcast>,
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
        broadcast: Arc<dyn Broadcast>,
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

        println!("Certificate valid!");

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
                .send_raw::<DeliveryShard>(&amended_delivery)
                .await
                .pot(ServeError::ConnectionError, here!())?;
        }

        session.end();

        Ok(())
    }
}

#[allow(dead_code)]
mod deliver;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{broadcast::{PACKING, Amendment}, crypto::statements::Reduction, total_order::LoopBack, Entry};

    use std::{
        collections::{BTreeMap, HashMap},
        iter,
        net::SocketAddr,
    };

    use futures::stream::{FuturesUnordered, StreamExt};

    use talk::{
        crypto::Identity,
        net::{
            test::{TestConnector, TestListener},
            SessionConnector,
        },
    };

    use varcram::VarCram;
    use zebra::vector::Vector;

    async fn generate_system(
        clients: usize,
        servers: usize,
    ) -> (
        Vec<KeyChain>,
        Vec<Server>,
        Membership,
        Directory,
        HashMap<Identity, SocketAddr>,
    ) {
        let clients = iter::repeat_with(KeyChain::random)
            .take(clients)
            .collect::<Vec<_>>();
        let mut directory = Directory::new();

        for (index, keychain) in clients.iter().enumerate() {
            directory.insert(index as u64, keychain.keycard());
        }

        let server_keychains = iter::repeat_with(KeyChain::random)
            .take(servers)
            .collect::<Vec<_>>();

        let membership =
            Membership::new(server_keychains.iter().map(|keychain| keychain.keycard()));

        let mut connector_map = HashMap::new();

        let mut servers = Vec::with_capacity(servers);
        for keychain in server_keychains {
            let broadcast = LoopBack::new();

            let (listener, address) = TestListener::new(keychain.clone()).await;
            let listener = SessionListener::new(listener);

            let server = Server::new(
                keychain.clone(),
                membership.clone(),
                directory.clone(),
                broadcast,
                listener,
                Default::default(),
            );

            connector_map.insert(keychain.keycard().identity(), address);
            servers.push(server)
        }

        (clients, servers, membership, directory, connector_map)
    }

    fn fake_batch(clients: &Vec<KeyChain>, batch_size: u64) -> CompressedBatch {
        let entries = (0..batch_size)
            .map(|id| {
                Some(Entry {
                    id,
                    sequence: 0,
                    message: [0; 8],
                })
            })
            .collect::<Vec<_>>();

        let entries = Vector::<_, PACKING>::new(entries).unwrap();
        let root = entries.root();

        let multisignatures = clients
            .iter()
            .take(batch_size as usize)
            .map(|keychain| {
                let reduction_statement = Reduction { root: &root };

                keychain.multisign(&reduction_statement).unwrap()
            })
            .collect::<Vec<_>>();

        let multisignature = Some(MultiSignature::aggregate(multisignatures).unwrap());

        CompressedBatch {
            ids: VarCram::cram(Vec::from_iter(0..batch_size).as_slice()),
            messages: Vec::from_iter(iter::repeat([0u8; 8]).take(batch_size as usize)),
            raise: 0,
            multisignature,
            stragglers: vec![],
        }
    }

    #[tokio::test]
    async fn server_interact() {
        let (clients, _servers, membership, _, connector_map) = generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let connector = SessionConnector::new(connector);

        let compressed_batch = fake_batch(&clients, 1);
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
            let mut delivery_shard = session.receive_raw::<DeliveryShard>().await.unwrap();

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
                },
                Amendment::Drop { id } => {
                    let index = ids.binary_search_by(|probe| probe.cmp(id)).unwrap();
                    batch.entries.set(index, None).unwrap();
                },
        }}

        let good_responses = responses.iter().filter_map(|(identity, delivery_shard)| {
            if (&delivery_shard.amendments, delivery_shard.height) == (&amendments, height) {
                Some((*identity, delivery_shard.multisignature))
            } else {
                None
            }
        });

        let statement = BatchDelivery { height: &height, root: &batch.root() };

        let certificate = Certificate::aggregate_quorum(&membership, good_responses);

        certificate.verify_quorum(&membership, &statement).unwrap();

        println!("Obtained delivery certificate!");
    }
}
