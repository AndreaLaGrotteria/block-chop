use crate::{
    broker::{LoadBatch, LoadBrokerSettings, Lockstep},
    heartbeat::{self, BrokerEvent},
    system::Membership,
};
use std::{iter, sync::Arc};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::SessionConnector,
    sync::{fuse::Fuse, promise::Promise},
};
use tokio::sync::mpsc;

pub struct LoadBroker {
    _fuse: Fuse,
}

impl LoadBroker {
    pub fn new(
        membership: Membership,
        broker_identity: Identity,
        connector: SessionConnector,
        flows: Vec<Vec<(Hash, Vec<u8>)>>,
        settings: LoadBrokerSettings,
    ) -> Self {
        // Build `Arc`s

        let membership = Arc::new(membership);

        // Prepare batches

        let batches_per_flow = flows.first().unwrap().len();

        let (mut flows, (lock_solvers, free_outlets)): (Vec<_>, (Vec<_>, Vec<_>)) = flows
            .into_iter()
            .map(|flow| {
                let (lock_promises, lock_solvers): (Vec<_>, Vec<_>) =
                    iter::repeat_with(Promise::pending).take(flow.len()).unzip();

                let (free_inlet, free_outlet) = mpsc::unbounded_channel();

                let flow = flow
                    .into_iter()
                    .zip(lock_promises)
                    .enumerate()
                    .map(|(index, ((root, raw_batch), lock_promise))| {
                        let lockstep = Lockstep {
                            index,
                            lock_promise,
                            free_inlet: free_inlet.clone(),
                        };

                        LoadBatch {
                            root,
                            raw_batch,
                            lockstep,
                        }
                    })
                    .collect::<Vec<_>>();

                (flow.into_iter(), (lock_solvers, free_outlet))
            })
            .unzip();

        let mut batches = Vec::with_capacity(flows.len() * batches_per_flow);

        for _ in 0..batches_per_flow {
            for flow in flows.iter_mut() {
                batches.push(flow.next().unwrap())
            }
        }

        // Spawn tasks

        let fuse = Fuse::new();

        for (lock_solvers, free_outlet) in lock_solvers.into_iter().zip(free_outlets) {
            fuse.spawn(LoadBroker::lockstep(
                lock_solvers,
                free_outlet,
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
        net::{test::TestConnector, SessionConnector},
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
        let session_connector = SessionConnector::new(connector);

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
            session_connector,
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
