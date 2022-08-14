use crate::{
    broadcast::Entry,
    broker::{Request, Response},
    crypto::statements::Broadcast as BroadcastStatement,
    system::{Directory, Passepartout},
};

use log::info;

use std::{iter, sync::Arc, time::Duration};

use talk::{net::DatagramDispatcher, sync::fuse::Fuse};

use tokio::{
    net::{self, ToSocketAddrs},
    time,
};

pub async fn load<A>(
    directory: Directory,
    passepartout: Passepartout,
    bind_address: A,
    broker_address: A,
    broadcasts: u64,
    rate: f64,
) where
    A: Clone + ToSocketAddrs,
{
    info!("Setting up dispatcher..");

    let dispatcher =
        DatagramDispatcher::<Request, Response>::bind(bind_address, Default::default())
            .await
            .unwrap();

    let (sender, mut receiver) = dispatcher.split();
    let sender = Arc::new(sender);

    let fuse = Fuse::new();

    fuse.spawn(async move {
        loop {
            let _ = receiver.receive().await;
        }
    });

    info!("Loading keychains..");

    let keychains = (0..broadcasts)
        .map(|id| {
            let keycard = directory.get(id).unwrap();
            let identity = keycard.identity();
            let keychain = passepartout.get(identity).unwrap();

            keychain
        })
        .collect::<Vec<_>>();

    info!("Generating requests..");

    let broadcasts = keychains
        .iter()
        .enumerate()
        .map(|(id, keychain)| {
            let id = id as u64;

            let entry = Entry {
                id,
                sequence: 0,
                message: id.to_be_bytes(),
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

    let broker_address = net::lookup_host(broker_address)
        .await
        .unwrap()
        .next()
        .unwrap();

    let datagrams = iter::repeat(broker_address).zip(broadcasts.into_iter());

    sender.pace(datagrams, rate).await;

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
