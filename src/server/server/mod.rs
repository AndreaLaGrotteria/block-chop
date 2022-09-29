use crate::{
    order::Order,
    server::{Deduplicator, ServerSettings, TotalityManager},
    system::{Directory, Membership},
    Entry,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::KeyChain,
    net::{Connector, Listener, SessionListener},
    sync::fuse::Fuse,
};
use tokio::sync::{mpsc, mpsc::Receiver as MpscReceiver};

type BurstOutlet = MpscReceiver<Vec<Option<Entry>>>;

pub struct Server {
    next_batch_outlet: BurstOutlet,
    _fuse: Fuse,
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
}

mod deliver;
mod listen;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::{test::null_batch, Amendment, DeliveryShard},
        crypto::{
            statements::{
                BatchDelivery as BatchDeliveryStatement, BatchWitness as BatchWitnessStatement,
            },
            Certificate,
        },
        server::Batch,
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
            let statement = BatchWitnessStatement {
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

        let statement = BatchDeliveryStatement {
            height: &height,
            root: &batch.root(),
        };

        let certificate = Certificate::aggregate_quorum(&membership, good_responses);

        certificate.verify_quorum(&membership, &statement).unwrap();
    }
}
