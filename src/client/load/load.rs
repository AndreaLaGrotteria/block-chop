use crate::{
    applications::{create_message, Application},
    broadcast::{Entry, Message},
    broker::{Request as BrokerRequest, Response},
    crypto::{
        records::Height as HeightRecord,
        statements::{
            Broadcast as BroadcastStatement,
            BroadcastAuthentication as BroadcastAuthenticationStatement,
            Reduction as ReductionStatement,
            ReductionAuthentication as ReductionAuthenticationStatement,
        },
    },
    debug, info,
    system::{Directory, Passepartout},
};
use std::{
    cmp,
    collections::VecDeque,
    net::{SocketAddr, ToSocketAddrs},
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};
use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramDispatcherSettings},
    sync::fuse::Fuse,
};
use tokio::time;

struct Top {
    height: u64,
    record: Option<HeightRecord>,
}

pub fn preprocess(
    directory: Arc<Directory>,
    passepartout: Arc<Passepartout>,
    range: Range<u64>,
    request_total: usize,
    application: Application,
) -> (Vec<KeyChain>, Vec<(u64, Message)>) {
    info!("Loading keychains..");

    let keychains = range
        .clone()
        .map(|id| {
            let identity = directory.get_identity(id).unwrap();
            let keychain = passepartout.get(identity).unwrap();

            keychain
        })
        .collect::<Vec<_>>();

    info!("Generating messages..");

    info!("Directory capacity: {}", directory.capacity());

    let create = create_message(application);

    let mut messages = Vec::with_capacity(request_total);

    for _ in 0..(request_total as f64 / (range.end - range.start) as f64).ceil() as u64 {
        let new_messages = (0..keychains.len()).map(|index| {
            let id = range.start + (index as u64);
            let message = create(id, directory.capacity() as u64);

            (id, message)
        });

        messages.extend(new_messages);
    }

    messages.truncate(request_total);

    (keychains, messages)
}

pub async fn load<A>(
    directory: Arc<Directory>,
    passepartout: Arc<Passepartout>,
    bind_address: A,
    broker_address: A,
    range: Range<u64>,
    rate: f64,
    request_total: usize,
    application: Application,
) where
    A: Clone + ToSocketAddrs,
{
    let (keychains, messages) = preprocess(
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
        messages,
        keychains,
    )
    .await;
}

pub async fn load_with<A>(
    bind_address: A,
    broker_address: A,
    range: Range<u64>,
    rate: f64,
    messages: Vec<(u64, Message)>,
    keychains: Vec<KeyChain>,
) where
    A: Clone + ToSocketAddrs,
{
    info!("Setting up dispatcher..");

    let keychains = Arc::new(keychains);

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

    let top = Arc::new(Mutex::new(Top {
        height: 0,
        record: None,
    }));

    let (sender, mut receiver) = dispatcher.split();
    let sender = Arc::new(sender);

    info!("Spawning receiving task..");

    let fuse = Fuse::new();

    {
        let range = range.clone();
        let keychains = keychains.clone();
        let sender = sender.clone();
        let top = top.clone();

        fuse.spawn(async move {
            loop {
                let (source, response) = receiver.receive().await;

                let keychains = keychains.clone();
                let sender = sender.clone();
                let top = top.clone();

                tokio::spawn(async move {
                    match response {
                        Response::Inclusion {
                            id,
                            root,
                            raise,
                            top_record,
                            ..
                        } => {
                            println!(
                                "Inclusion: {}",
                                id,
                            );
                            println!("Range: {}..{}", range.start, range.end);
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

                            let mut top = top.lock().unwrap();

                            top.height = cmp::max(top.height, raise);
                            top.record = cmp::max(top.record.clone(), top_record);
                        }
                        Response::Delivery {
                            height,
                            root,
                            certificate,
                            ..
                        } => {
                            let new_record = HeightRecord::new(height, root, certificate);

                            let mut top = top.lock().unwrap();
                            top.record = cmp::max(top.record.clone(), Some(new_record));
                        }
                    }
                });
            }
        });
    }

    info!("Pacing requests..");

    let broker_address = broker_address.to_socket_addrs().unwrap().next().unwrap();

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

    let datagrams = Datagrams {
        broker_address,
        messages: messages.into_iter().collect(),
        keychains,
        top: top.clone(),
        range,
    };

    sender.pace(datagrams, rate).await;
}

struct Datagrams {
    broker_address: SocketAddr,
    messages: VecDeque<(u64, Message)>,
    keychains: Arc<Vec<KeyChain>>,
    top: Arc<Mutex<Top>>,
    range: Range<u64>,
}

impl Iterator for Datagrams {
    type Item = (SocketAddr, BrokerRequest);

    fn next(&mut self) -> Option<Self::Item> {
        let (id, message) = if let Some(message) = self.messages.pop_front() {
            message
        } else {
            return None;
        };

        let (sequence, height_record) = {
            let top = self.top.lock().unwrap();
            (top.height + 1, top.record.clone())
        };

        let entry = Entry {
            id,
            sequence,
            message,
        };

        let broadcast_statement = BroadcastStatement {
            sequence: &entry.sequence,
            message: &entry.message,
        };

        let keychain = self
            .keychains
            .get((id - self.range.start) as usize)
            .unwrap();

        let signature = keychain.sign(&broadcast_statement).unwrap();

        let authentication = height_record.as_ref().map(|height_record| {
            let broadcast_authentication_statement =
                BroadcastAuthenticationStatement { height_record };

            keychain.sign(&broadcast_authentication_statement).unwrap()
        });

        let request = BrokerRequest::Broadcast {
            entry,
            signature,
            height_record,
            authentication,
        };

        Some((self.broker_address, request))
    }
}
