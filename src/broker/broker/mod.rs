use crate::{system::Membership, Directory};

use doomstack::{here, Doom, ResultExt, Top};

use std::sync::Arc;

use talk::{
    net::{DatagramDispatcher, SessionConnector},
    sync::fuse::Fuse,
};

use tokio::{net::ToSocketAddrs, sync::mpsc};

pub struct Broker {
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

        let dispatcher = DatagramDispatcher::bind(bind, Default::default()) // TODO: Forward settings
            .await
            .pot(BrokerError::BindFailed, here!())?;

        let (sender, receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        // Spawn tasks

        let fuse = Fuse::new();

        let mut datagram_inlets = Vec::new();
        let (request_inlet, request_outlet) = mpsc::channel(1024); // TODO: Add settings

        // TODO: Add settings
        for _ in 0..32 {
            let (datagram_inlet, datagram_outlet) = mpsc::channel(1024); // TODO: Add settings
            datagram_inlets.push(datagram_inlet);

            fuse.spawn(Broker::process(
                directory.clone(),
                datagram_outlet,
                request_inlet.clone(),
            ));
        }

        fuse.spawn(Broker::dispatch(receiver, datagram_inlets));

        fuse.spawn(Broker::handle(
            membership.clone(),
            directory.clone(),
            request_outlet,
            sender,
            connector,
        ));

        Ok(Broker { _fuse: fuse })
    }
}

mod dispatch;
mod handle;
mod process;
