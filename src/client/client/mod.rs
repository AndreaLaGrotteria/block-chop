use crate::{broadcast::Message, Membership};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
};

use talk::{crypto::KeyChain, sync::fuse::Fuse};

use tokio::{
    io,
    net::{self, ToSocketAddrs},
    sync::{
        mpsc::{self, Sender as MpscSender},
        Mutex as TokioMutex,
    },
};

type BroadcastInlet = MpscSender<Message>;

pub struct Client {
    brokers: Arc<StdMutex<Vec<SocketAddr>>>,
    broadcast_inlet: TokioMutex<BroadcastInlet>,
    _fuse: Fuse,
}

#[derive(Doom)]
pub enum ClientError {
    #[doom(description("Failed to resolve host: {:?}", source))]
    #[doom(wrap(resolve_failed))]
    ResolveFailed { source: io::Error },
    #[doom(description("Host unknown"))]
    HostUnknown,
}

impl Client {
    pub fn new(id: u64, keychain: KeyChain, membership: Membership) -> Self {
        let brokers = Arc::new(StdMutex::new(Vec::new()));

        let (broadcast_inlet, broadcast_outlet) = mpsc::channel(1); // Only one broadcast is performed at a time
        let broadcast_inlet = TokioMutex::new(broadcast_inlet);

        let fuse = Fuse::new();

        {
            let brokers = brokers.clone();
            fuse.spawn(Client::run(
                id,
                keychain,
                membership,
                brokers,
                broadcast_outlet,
            ));
        }

        Client {
            brokers,
            broadcast_inlet,
            _fuse: fuse,
        }
    }

    pub async fn add_broker<A>(&self, broker: A) -> Result<(), Top<ClientError>>
    where
        A: ToSocketAddrs,
    {
        let mut addresses = net::lookup_host(broker)
            .await
            .map_err(ClientError::resolve_failed)
            .map_err(ClientError::into_top)
            .spot(here!())?;

        let address = addresses
            .next()
            .ok_or(ClientError::HostUnknown.into_top())
            .spot(here!())?;

        self.brokers.lock().unwrap().push(address);

        Ok(())
    }

    pub async fn broadcast(&self, message: Message) {
        let broadcast_inlet = self.broadcast_inlet.lock().await;

        // If the `Client` still exists, then `_fuse` has not
        // dropped, `run` is still running and `broadcast_outlet`
        // still exists
        broadcast_inlet.send(message).await.unwrap();
    }
}

mod run;
