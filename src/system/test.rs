use crate::{
    broadcast::{CompressedBatch, PACKING},
    crypto::statements::Reduction,
    server::ServerSettings,
    total_order::LoopBack,
    Directory, Entry, Membership, Server,
};

use std::{collections::HashMap, iter, net::SocketAddr};

use talk::{
    crypto::{primitives::multi::Signature as MultiSignature, Identity, KeyChain},
    net::{test::TestListener, SessionListener},
};

use varcram::VarCram;
use zebra::vector::Vector;

pub(crate) async fn generate_system(
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

    let membership = Membership::new(server_keychains.iter().map(|keychain| keychain.keycard()));

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
            ServerSettings {
                serve_tasks: num_cpus::get() - 3,
                ..Default::default()
            },
        );

        connector_map.insert(keychain.keycard().identity(), address);
        servers.push(server)
    }

    (clients, servers, membership, directory, connector_map)
}

pub(crate) fn fake_batch(clients: &Vec<KeyChain>, batch_size: u64) -> CompressedBatch {
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
