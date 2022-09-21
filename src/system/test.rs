use crate::{order::LoopBack, server::ServerSettings, Directory, Membership, Server};
use std::{collections::HashMap, iter, net::SocketAddr};
use talk::{
    crypto::{Identity, KeyChain},
    net::{test::TestListener, SessionListener},
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
    let mut connector_map = HashMap::new();

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
            ServerSettings {
                serve_tasks: num_cpus::get(),
                ..Default::default()
            },
        );

        connector_map.insert(keychain.keycard().identity(), address);
        servers.push(server)
    }

    (
        servers,
        membership,
        directory,
        connector_map,
        client_keychains,
    )
}
