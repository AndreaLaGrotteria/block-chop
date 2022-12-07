use crate::{
    broker::{BrokerSettings, Response},
    heartbeat::{self, BrokerEvent},
    info,
    system::Membership,
    Directory,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{net::ToSocketAddrs, sync::Arc, time::Duration};
use talk::{
    crypto::Identity,
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramSender, PlexConnector},
    sync::fuse::Fuse,
};
use tokio::{sync::mpsc, time};

pub struct Broker {
    sender: Arc<DatagramSender<Response>>,
    _fuse: Fuse,
}

#[derive(Doom)]
pub enum BrokerError {
    #[doom(description("Failed to `bind` to the specified address"))]
    BindFailed,
}

impl Broker {
    pub fn new<A>(
        membership: Membership,
        directory: Directory,
        bind: A,
        broker_identity: Identity,
        connector: PlexConnector,
        settings: BrokerSettings,
    ) -> Result<Self, Top<BrokerError>>
    where
        A: Clone + ToSocketAddrs,
    {
        // Build `Arc`s

        let membership = Arc::new(membership);
        let directory = Arc::new(directory);

        // Bind `DatagramDispatcher`

        let dispatcher = DatagramDispatcher::bind(
            bind,
            DatagramDispatcherSettings {
                maximum_packet_rate: settings.maximum_packet_rate,
                pace_out_tasks: 10,
                retransmission_delay: Duration::from_millis(250),
                ..Default::default()
            },
        )
        .pot(BrokerError::BindFailed, here!())?;

        let (sender, receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        // Spawn tasks

        let fuse = Fuse::new();

        let mut authenticate_inlets = Vec::new();
        let (handle_inlet, handle_outlet) = mpsc::channel(settings.handle_channel_capacity);

        for _ in 0..settings.authenticate_tasks {
            let (authenticate_inlet, authenticate_outlet) =
                mpsc::channel(settings.authenticate_channel_capacity);

            authenticate_inlets.push(authenticate_inlet);

            fuse.spawn(Broker::authenticate_requests(
                directory.clone(),
                authenticate_outlet,
                handle_inlet.clone(),
                settings.clone(),
            ));
        }

        fuse.spawn(Broker::dispatch_requests(
            receiver,
            authenticate_inlets,
            settings.clone(),
        ));

        fuse.spawn(Broker::handle_requests(
            membership.clone(),
            directory.clone(),
            handle_outlet,
            sender.clone(),
            broker_identity,
            connector,
            settings.clone(),
        ));

        #[cfg(feature = "benchmark")]
        heartbeat::log(BrokerEvent::Booted {
            identity: broker_identity,
        });

        {
            let sender = sender.clone();

            tokio::spawn(async move {
                loop {
                    time::sleep(Duration::from_secs(1)).await;

                    info!("`DatagramDispatcher` statistics:\n  packets sent: {}\n  packets received: {} ({} msg, {} ack)\n  retransmissions: {}\n  pace_out chokes: {}\n  process_in drops: {}\n  route_out drops: {}\n",
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

        Ok(Broker {
            sender,
            _fuse: fuse,
        })
    }

    pub fn packets_sent(&self) -> usize {
        self.sender.packets_sent()
    }

    pub fn packets_received(&self) -> usize {
        self.sender.packets_received()
    }

    pub fn message_packets_processed(&self) -> usize {
        self.sender.message_packets_processed()
    }

    pub fn acknowledgement_packets_processed(&self) -> usize {
        self.sender.acknowledgement_packets_processed()
    }

    pub fn retransmissions(&self) -> usize {
        self.sender.retransmissions()
    }

    pub fn pace_out_chokes(&self) -> usize {
        self.sender.pace_out_chokes()
    }

    pub fn process_in_drops(&self) -> usize {
        self.sender.process_in_drops()
    }

    pub fn route_out_drops(&self) -> usize {
        self.sender.route_out_drops()
    }
}

mod authenticate_requests;
mod broadcast_batch;
mod dispatch_requests;
mod disseminate_deliveries;
mod handle_requests;
mod manage_batch;
mod reduce_batch;
mod setup_batch;
mod submit_batch;
