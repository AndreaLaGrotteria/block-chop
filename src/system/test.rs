use crate::{order::LoopBack, server::ServerSettings, Directory, Membership, Server};
use std::{collections::HashMap, iter, net::SocketAddr};
use talk::{
    crypto::{Identity, KeyChain},
    net::test::{TestConnector, TestListener},
};

pub(crate) async fn generate_system(
    clients: usize,
    servers: usize,
) -> (
    Vec<Server>,
    Membership,
    Directory,
    HashMap<Identity, SocketAddr>,
    Vec<KeyChain>,
) {
    let client_keychains = iter::repeat_with(KeyChain::random)
        .take(clients)
        .collect::<Vec<_>>();

    let mut directory = Directory::new();

    for (index, keychain) in client_keychains.iter().enumerate() {
        directory.insert(index as u64, keychain.keycard());
    }

    let server_keychains = iter::repeat_with(KeyChain::random)
        .take(servers)
        .collect::<Vec<_>>();

    let server_keycards = server_keychains.iter().map(KeyChain::keycard);
    let membership = Membership::new(server_keycards);

    let mut servers = Vec::with_capacity(servers);
    let mut broker_connector_map = HashMap::new();
    let mut totality_connector_map = HashMap::new();

    let mut listeners = Vec::new();

    for keychain in server_keychains {
        let (broker_listener, broker_address) = TestListener::new(keychain.clone()).await;

        let (totality_listener, totality_address) = TestListener::new(keychain.clone()).await;

        broker_connector_map.insert(keychain.keycard().identity(), broker_address);
        totality_connector_map.insert(keychain.keycard().identity(), totality_address);

        listeners.push((keychain, broker_listener, totality_listener));
    }

    for (keychain, broker_listener, totality_listener) in listeners {
        let broadcast = LoopBack::new();

        let totality_connector =
            TestConnector::new(keychain.clone(), totality_connector_map.clone());

        let server = Server::new(
            keychain.clone(),
            membership.clone(),
            directory.clone(),
            broadcast,
            broker_listener,
            totality_connector,
            totality_listener,
            ServerSettings {
                expand_tasks: num_cpus::get(),
                ..Default::default()
            },
        );

        servers.push(server)
    }

    (
        servers,
        membership,
        directory,
        broker_connector_map,
        client_keychains,
    )
}
