use crate::{
    broadcast::CompressedBatch,
    crypto::{statements::BatchWitness, Certificate},
    server::{Batch, BatchError, ServerSettings},
    system::{Directory, Membership},
};

use doomstack::{here, Doom, ResultExt, Top};
use tokio::{sync::Semaphore, task};

use std::sync::Arc;

use talk::{
    crypto::{
        primitives::{hash::Hash, multi::Signature as MultiSignature},
        KeyChain,
    },
    net::{Session, SessionListener},
    sync::fuse::Fuse,
};

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
    pub fn new(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        listener: SessionListener,
        settings: ServerSettings,
    ) -> Self {
        let fuse = Fuse::new();

        fuse.spawn(async move {
            Server::listen(keychain, membership, directory, listener, settings).await;
        });

        Server { _fuse: fuse }
    }

    async fn listen(
        keychain: KeyChain,
        membership: Membership,
        directory: Directory,
        mut listener: SessionListener,
        settings: ServerSettings,
    ) {
        let membership = Arc::new(membership);
        let directory = Arc::new(directory);

        let semaphore = Semaphore::new(settings.serve_tasks);
        let semaphore = Arc::new(semaphore);

        let fuse = Fuse::new();

        loop {
            let (_, session) = listener.accept().await;

            let keychain = keychain.clone();
            let membership = membership.clone();
            let directory = directory.clone();
            let semaphore = semaphore.clone();

            fuse.spawn(async move {
                if let Err(error) =
                    Server::serve(keychain, membership, directory, semaphore, session).await
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

        let (root, witness_shard) = {
            let keychain = keychain.clone();
            let _permit = semaphore.acquire().await.unwrap();

            task::spawn_blocking(
                move || -> Result<(Hash, Option<MultiSignature>), Top<BatchError>> {
                    if verify {
                        let batch = Batch::expand_verified(&directory, compressed_batch)?;

                        let witness_shard = keychain
                            .multisign(&BatchWitness::new(batch.root()))
                            .unwrap();

                        Ok((batch.root(), Some(witness_shard)))
                    } else {
                        let batch = Batch::expand_unverified(compressed_batch)?;

                        Ok((batch.root(), None))
                    }
                },
            )
            .await
            .unwrap()
            .pot(ServeError::BatchInvalid, here!())?
        };

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

        // TODO: Interact with Total Order Broadcast

        session.end();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{Entry, crypto::statements::Reduction, broadcast::PACKING};

    use std::{collections::HashMap, iter, net::SocketAddr, time::Duration};

    use futures::stream::{FuturesUnordered, StreamExt};

    use talk::{
        crypto::{Identity},
        net::{
            test::{TestConnector, TestListener},
            SessionConnector,
        },
    };

    use tokio::time;
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

        let server_keychains = iter::repeat_with(KeyChain::random).take(servers).collect::<Vec<_>>();

        let membership =
            Membership::new(server_keychains.iter().map(|keychain| keychain.keycard()));

        let mut connector_map = HashMap::new();

        let mut servers = Vec::with_capacity(servers);
        for keychain in server_keychains {
            let (listener, address) = TestListener::new(keychain.clone()).await;
            let listener = SessionListener::new(listener);
            let server = Server::new(
                keychain.clone(),
                membership.clone(),
                directory.clone(),
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

        let multisignatures = clients.iter().take(batch_size as usize).map(|keychain| {
            let reduction_statement = Reduction { root: &root };

            keychain.multisign(&reduction_statement).unwrap()
        }).collect::<Vec<_>>();

        let multisignature = Some(MultiSignature::aggregate(multisignatures).unwrap());

        CompressedBatch {
            ids: VarCram::cram(Vec::from_iter(0..batch_size).as_slice()),
            messages: Vec::from_iter(iter::repeat([0u8; 8]).take(batch_size as usize)),
            raise: 0,
            multisignature,
            stragglers: vec!(),
        }
    }

    #[tokio::test]
    async fn server_interact() {
        let (clients, _servers, membership, _, connector_map) =
            generate_system(1000, 4).await;

        let broker = KeyChain::random();

        let connector = TestConnector::new(broker.clone(), connector_map.clone());
        let connector = SessionConnector::new(connector);

        let compressed_batch = fake_batch(&clients, 1);

        time::sleep(Duration::from_secs(1)).await;

        let mut sessions = membership
            .servers()
            .keys()
            .map(|server| async { connector.connect(server.clone()).await.unwrap() })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        sessions[0].send_raw(&compressed_batch).await.unwrap();
        sessions[0].send_raw(&true).await.unwrap();

        let response: MultiSignature = sessions[0].receive_raw().await.unwrap();

        println!("Received response: {:?}", response);
    }
}
