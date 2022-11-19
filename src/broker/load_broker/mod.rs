use crate::{
    broker::{LoadBatch, LoadBrokerSettings, Lockstep},
    heartbeat::{self, BrokerEvent},
    system::Membership,
};
use std::{
    collections::{HashMap, VecDeque},
    iter,
    sync::Arc,
};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::PlexConnector,
    sync::{
        fuse::Fuse,
        promise::{Promise, Solver},
    },
};
use tokio::sync::mpsc::{self, UnboundedReceiver};

type UsizeOutlet = UnboundedReceiver<usize>;

pub struct LoadBroker {
    _fuse: Fuse,
}

struct Flow {
    batches: VecDeque<LoadBatch>,
    lock_solvers: Vec<Solver<()>>,
    free_outlet: UsizeOutlet,
}

impl LoadBroker {
    pub async fn new(
        membership: Membership,
        broker_identity: Identity,
        connector: PlexConnector,
        flows: Vec<Vec<(Hash, Vec<u8>)>>,
        settings: LoadBrokerSettings,
    ) -> Self {
        // Build `Arc`s

        let membership = Arc::new(membership);

        // Fill `connector`

        connector
            .fill(membership.servers().keys().copied(), settings.fill_interval)
            .await;

        // Compile multiplexes

        let mut multiplexes = HashMap::new();

        for server in membership.servers().keys().copied() {
            multiplexes.insert(server, connector.multiplexes_to(server).await);
        }

        let mut multiplexes = multiplexes
            .into_iter()
            .map(|(server, multiplexes)| (server, multiplexes.into_iter().cycle()))
            .collect::<HashMap<_, _>>();

        // Prepare batches

        let batches_per_flow = flows.first().unwrap().len();

        let mut flows = flows
            .into_iter()
            .map(|batches| {
                // Initialize lock `Promise`s and free channel

                let (lock_promises, lock_solvers): (Vec<_>, Vec<_>) =
                    iter::repeat_with(Promise::pending)
                        .take(batches.len())
                        .unzip();

                let (free_inlet, free_outlet) = mpsc::unbounded_channel();

                // Compute affinity map

                let affinities = membership
                    .servers()
                    .keys()
                    .copied()
                    .map(|server| {
                        (
                            server,
                            multiplexes.get_mut(&server).unwrap().next().unwrap(),
                        )
                    })
                    .collect::<HashMap<_, _>>();

                let affinities = Arc::new(affinities);

                // Convert `batches` into `LoadBatch`es

                let batches = batches
                    .into_iter()
                    .zip(lock_promises)
                    .enumerate()
                    .map(|(index, ((root, raw_batch), lock_promise))| {
                        let affinities = affinities.clone();

                        let lockstep = Lockstep {
                            index,
                            lock_promise,
                            free_inlet: free_inlet.clone(),
                        };

                        LoadBatch {
                            root,
                            raw_batch,
                            affinities,
                            lockstep,
                        }
                    })
                    .collect::<VecDeque<_>>();

                Flow {
                    batches,
                    lock_solvers,
                    free_outlet,
                }
            })
            .collect::<Vec<_>>();

        // Vectorize the transposition of `flows.batch`es: one batch per flow in a
        // round-robin fashion until all batches are organized in a single vector

        let mut batches = Vec::with_capacity(flows.len() * batches_per_flow);

        for _ in 0..batches_per_flow {
            for flow in flows.iter_mut() {
                batches.push(flow.batches.pop_front().unwrap());
            }
        }

        // Spawn tasks

        let fuse = Fuse::new();

        for (id, flow) in flows.into_iter().enumerate() {
            fuse.spawn(LoadBroker::lockstep(
                id,
                flow.lock_solvers,
                flow.free_outlet,
                settings.lockstep_delta,
            ));
        }

        fuse.spawn(LoadBroker::broadcast_batches(
            membership.clone(),
            broker_identity,
            connector,
            batches,
            settings.clone(),
        ));

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::Booted {
            identity: broker_identity,
        });

        LoadBroker { _fuse: fuse }
    }
}

mod broadcast_batch;
mod broadcast_batches;
mod lockstep;
mod submit_batch;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broadcast::{test::null_batch, Batch as BroadcastBatch},
        system::test::generate_system,
    };
    use std::{iter, time::Duration};
    use talk::{
        crypto::KeyChain,
        net::{test::TestConnector, PlexConnector},
    };
    use tokio::time;

    fn to_raw(val: (Hash, BroadcastBatch)) -> (Hash, Vec<u8>) {
        (val.0, bincode::serialize(&val.1).unwrap())
    }

    #[tokio::test]
    async fn broker_manage() {
        let (_servers, membership, _directory, connector_map, client_keychains) =
            generate_system(1000, 4).await;

        let keychain = KeyChain::random();
        let broker_identity = keychain.keycard().identity();
        let connector = TestConnector::new(keychain, connector_map.clone());
        let plex_connector = PlexConnector::new(connector, Default::default());

        let flows = iter::repeat(
            iter::repeat(to_raw(null_batch(&client_keychains, 30)))
                .take(5)
                .collect::<Vec<_>>(),
        )
        .take(10)
        .collect::<Vec<_>>();

        let _load_broker = LoadBroker::new(
            membership.clone(),
            broker_identity,
            plex_connector,
            flows,
            LoadBrokerSettings {
                rate: 16.,
                totality_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        );

        time::sleep(Duration::from_secs(10)).await;
    }
}
