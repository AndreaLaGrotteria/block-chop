use crate::{broker::Response, system::Membership, Directory};

use doomstack::{here, Doom, ResultExt, Top};

use std::{net::ToSocketAddrs, sync::Arc};

use talk::{
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramSender, SessionConnector},
    sync::fuse::Fuse,
};

use tokio::sync::mpsc;

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
    pub async fn new<A>(
        membership: Membership,
        directory: Directory,
        bind: A,
        connector: SessionConnector,
    ) -> Result<Self, Top<BrokerError>>
    where
        A: Clone + ToSocketAddrs,
    {
        // Build `Arc`s

        let membership = Arc::new(membership);
        let directory = Arc::new(directory);
        let connector = Arc::new(connector);

        // Bind `DatagramDispatcher`

        let dispatcher = DatagramDispatcher::bind(
            bind,
            DatagramDispatcherSettings {
                maximum_packet_rate: 262144.,
                ..Default::default()
            },
        ) // TODO: Forward settings
        .pot(BrokerError::BindFailed, here!())?;

        let (sender, receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        // Spawn tasks

        let fuse = Fuse::new();

        let mut authenticate_inlets = Vec::new();
        let (handle_inlet, handle_outlet) = mpsc::channel(1024); // TODO: Add settings

        // TODO: Add settings
        for _ in 0..32 {
            let (authenticate_inlet, authenticate_outlet) = mpsc::channel(1024); // TODO: Add settings
            authenticate_inlets.push(authenticate_inlet);

            fuse.spawn(Broker::authenticate_requests(
                directory.clone(),
                authenticate_outlet,
                handle_inlet.clone(),
            ));
        }

        fuse.spawn(Broker::dispatch_requests(receiver, authenticate_inlets));

        fuse.spawn(Broker::handle_requests(
            membership.clone(),
            directory.clone(),
            handle_outlet,
            sender.clone(),
            connector,
        ));

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
mod dispatch_requests;
mod handle_requests;
mod manage_batch;
mod reduce_batch;
mod setup_batch;
