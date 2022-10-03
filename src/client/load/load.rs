use crate::{
    broadcast::{Entry, Message},
    broker::{Request, Response},
    crypto::statements::{
        Broadcast as BroadcastStatement, Reduction as ReductionStatement,
        ReductionAuthentication as ReductionAuthenticationStatement,
    },
    debug, info,
    system::{Directory, Passepartout},
};
use rayon::prelude::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{iter, net::ToSocketAddrs, ops::Range, sync::Arc, time::Duration};
use talk::{
    net::{DatagramDispatcher, DatagramDispatcherSettings},
    sync::fuse::Fuse,
};
use tokio::time;

pub async fn load<A>(
    directory: Directory,
    passepartout: Passepartout,
    bind_address: A,
    broker_address: A,
    range: Range<u64>,
    rate: f64,
) where
    A: Clone + ToSocketAddrs,
{
    info!("Setting up dispatcher..");

    let dispatcher = DatagramDispatcher::<Request, Response>::bind(
        bind_address,
        DatagramDispatcherSettings {
            maximum_packet_rate: 393216.,
            ..Default::default()
        },
    )
    .unwrap();

    let (sender, mut receiver) = dispatcher.split();
    let sender = Arc::new(sender);

    info!("Loading keychains..");

    let keychains = range
        .clone()
        .map(|id| {
            let keycard = directory.get(id).unwrap();
            let identity = keycard.identity();
            let keychain = passepartout.get(identity).unwrap();

            keychain
        })
        .collect::<Vec<_>>();

    let keychains = Arc::new(keychains);

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

                            let request = Request::Reduction {
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

    info!("Generating requests..");

    let broadcasts = keychains
        .par_iter()
        .enumerate()
        .map(|(index, keychain)| {
            let id = range.start + (index as u64);

            let mut message = Message::default();
            message[0..8].copy_from_slice(id.to_be_bytes().as_slice());

            let entry = Entry {
                id,
                sequence: 0,
                message,
            };

            let statement = BroadcastStatement {
                sequence: &entry.sequence,
                message: &entry.message,
            };

            let signature = keychain.sign(&statement).unwrap();

            Request::Broadcast {
                entry,
                signature,
                height_record: None,
                authentication: None,
            }
        })
        .collect::<Vec<_>>();

    info!("Pacing requests..");

    let broker_address = broker_address.to_socket_addrs().unwrap().next().unwrap();

    let datagrams = iter::repeat(broker_address).zip(broadcasts.into_iter());

    {
        let sender = sender.clone();

        tokio::spawn(async move {
            sender.pace(datagrams, rate).await;
        });
    }

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
}
