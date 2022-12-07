use crate::{
    applications::{create_message, Application},
    broadcast::Entry,
    broker::{Request as BrokerRequest, Response},
    crypto::statements::{
        Broadcast as BroadcastStatement, Reduction as ReductionStatement,
        ReductionAuthentication as ReductionAuthenticationStatement,
    },
    debug, info,
    system::{Directory, Passepartout},
};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{iter, net::ToSocketAddrs, ops::Range, sync::Arc, time::Duration};
use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramDispatcherSettings},
    sync::fuse::Fuse,
};
use tokio::time;

pub struct Request(BrokerRequest);

pub fn preprocess(
    directory: Directory,
    passepartout: Passepartout,
    range: Range<u64>,
    request_total: usize,
    application: Application,
) -> (Vec<KeyChain>, Vec<Request>) {
    info!("Loading keychains..");

    let keychains = range
        .clone()
        .map(|id| {
            let identity = directory.get_identity(id).unwrap();
            let keychain = passepartout.get(identity).unwrap();

            keychain
        })
        .collect::<Vec<_>>();

    info!("Generating requests..");

    info!("Directory capacity: {}", directory.capacity());

    let create = create_message(application);

    let mut broadcasts = Vec::with_capacity(request_total);

    for sequence in 0..5 { //(request_total as f64 / (range.end - range.start) as f64).ceil() as u64 {
        let new_broadcasts = keychains
            .par_iter()
            .enumerate()
            .map(|(index, keychain)| {
                let id = range.start + (index as u64);

                let message = create(id, directory.capacity() as u64);

                let entry = Entry {
                    id,
                    sequence,
                    message,
                };

                let statement = BroadcastStatement {
                    sequence: &entry.sequence,
                    message: &entry.message,
                };

                let signature = keychain.sign(&statement).unwrap();

                let request = BrokerRequest::Broadcast {
                    entry,
                    signature,
                    height_record: None,
                    authentication: None,
                };

                Request(request)
            })
            .collect::<Vec<_>>();

        broadcasts.extend(new_broadcasts);
    }

    broadcasts.truncate(request_total);

    (keychains, broadcasts)
}

pub async fn load<A>(
    directory: Directory,
    passepartout: Passepartout,
    bind_address: A,
    broker_address: A,
    range: Range<u64>,
    rate: f64,
    request_total: usize,
    application: Application,
) where
    A: Clone + ToSocketAddrs,
{
    let (keychains, broadcasts) = preprocess(
        directory,
        passepartout,
        range.clone(),
        request_total,
        application,
    );
    load_with(
        bind_address,
        broker_address,
        range,
        rate,
        broadcasts,
        keychains,
    )
    .await;
}

pub async fn load_with<A>(
    bind_address: A,
    broker_address: A,
    range: Range<u64>,
    rate: f64,
    broadcasts: Vec<Request>,
    keychains: Vec<KeyChain>,
) where
    A: Clone + ToSocketAddrs,
{
    info!("Setting up dispatcher..");

    let keychains = Arc::new(keychains);
    let broadcasts = broadcasts
        .into_iter()
        .map(|request| request.0)
        .collect::<Vec<_>>();

    let dispatcher = DatagramDispatcher::<BrokerRequest, Response>::bind(
        bind_address,
        DatagramDispatcherSettings {
            maximum_packet_rate: 393216.,
            pace_out_tasks: 10,
            process_in_tasks: 8,
            retransmission_delay: Duration::from_millis(250),
            ..Default::default()
        },
    )
    .unwrap();

    let (sender, mut receiver) = dispatcher.split();
    let sender = Arc::new(sender);

    info!("Spawning receiving task..");

    let fuse = Fuse::new();

    {
        let range = range.clone();
        let keychains = keychains.clone();
        let sender = sender.clone();

        fuse.spawn(async move {
            loop {
                let (source, response) = receiver.receive().await;

                let keychains = keychains.clone();
                let sender = sender.clone();

                tokio::spawn(async move {
                    match response {
                        Response::Inclusion { id, root, .. } => {
                            let keychain = keychains.get((id - range.start) as usize).unwrap();
                            let reduction_statement = ReductionStatement { root: &root };
                            let multisignature = keychain.multisign(&reduction_statement).unwrap();

                            let reduction_authentication_statement =
                                ReductionAuthenticationStatement {
                                    root: &root,
                                    multisignature: &multisignature,
                                };

                            let authentication =
                                keychain.sign(&reduction_authentication_statement).unwrap();

                            let request = BrokerRequest::Reduction {
                                root,
                                id,
                                multisignature,
                                authentication,
                            };

                            sender.send(source, request).await;
                        }
                        Response::Delivery { .. } => {}
                    }
                });
            }
        });
    }

    info!("Pacing requests..");

    let broker_address = broker_address.to_socket_addrs().unwrap().next().unwrap();

    let datagrams = iter::repeat(broker_address).zip(broadcasts.into_iter());

    {
        let sender = sender.clone();

        tokio::spawn(async move {
            loop {
                time::sleep(Duration::from_secs(1)).await;

                debug!("`DatagramDispatcher` statistics:\n  packets sent: {}\n  packets received: {} ({} msg, {} ack)\n  retransmissions: {}\n  pace_out chokes: {}\n  process_in drops: {}\n  route_out drops: {}\n",
                    sender.packets_sent(),
                    sender.packets_received(),
                    sender.message_packets_processed(),
                    sender.acknowledgement_packets_processed(),
                    sender.retransmissions(),
                    sender.pace_out_chokes(),
                    sender.process_in_drops(),
                    sender.route_out_drops()
                );
            }
        });
    }

    sender.pace(datagrams, rate).await;
}
