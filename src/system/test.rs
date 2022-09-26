use crate::{order::LoopBack, server::ServerSettings, Directory, Membership, Server};
use std::{collections::HashMap, iter, net::SocketAddr};
use talk::{
    crypto::{Identity, KeyChain},
    net::{
        test::{TestConnector, TestListener},
        SessionConnector, SessionListener,
    },
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
        let (listener, address_broker) = TestListener::new(keychain.clone()).await;
        let broker_listener = SessionListener::new(listener);

        let (listener, address_totality) = TestListener::new(keychain.clone()).await;
        let totality_listener = SessionListener::new(listener);

        broker_connector_map.insert(keychain.keycard().identity(), address_broker);
        totality_connector_map.insert(keychain.keycard().identity(), address_totality);

        listeners.push((keychain, broker_listener, totality_listener));
    }

    for (keychain, broker_listener, totality_listener) in listeners {
        let broadcast = LoopBack::new();

        let connector = TestConnector::new(keychain.clone(), totality_connector_map.clone());
        let totality_connector = SessionConnector::new(connector);

        let server = Server::new(
            keychain.clone(),
            membership.clone(),
            directory.clone(),
            broadcast,
            broker_listener,
            totality_connector,
            totality_listener,
            ServerSettings {
                serve_tasks: num_cpus::get(),
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
