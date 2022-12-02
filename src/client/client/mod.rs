use crate::{
    broadcast::Message,
    crypto::records::Delivery as DeliveryRecord,
    heartbeat::{self, ClientEvent},
    Membership,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};
use talk::{crypto::KeyChain, sync::fuse::Fuse};
use tokio::{
    io,
    sync::{
        mpsc::{self, Sender as MpscSender},
        oneshot::{self, Sender as OneshotSender},
    },
};

type BroadcastInlet = MpscSender<(Message, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<DeliveryRecord>;

pub struct Client {
    brokers: Arc<Mutex<Vec<SocketAddr>>>,
    broadcast_inlet: BroadcastInlet,
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
    pub fn new<A>(id: u64, keychain: KeyChain, membership: Membership, bind: A) -> Self
    where
        A: 'static + Send + Sync + Clone + ToSocketAddrs,
    {
        let brokers = Arc::new(Mutex::new(Vec::new()));
        let (broadcast_inlet, broadcast_outlet) = mpsc::channel(1); // Only one broadcast is performed at a time anyway

        let fuse = Fuse::new();

        let identity = keychain.keycard().identity();

        {
            let brokers = brokers.clone();
            fuse.spawn(Client::run(
                id,
                keychain,
                membership,
                bind,
                brokers,
                broadcast_outlet,
            ));
        }

        #[cfg(feature = "benchmark")]
        heartbeat::log(ClientEvent::Booted { identity });

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
        let mut addresses = broker
            .to_socket_addrs()
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

    pub async fn broadcast(&self, message: Message) -> DeliveryRecord {
        let (delivery_inlet, delivery_outlet) = oneshot::channel();

        // If the `Client` still exists, then `_fuse` has not
        // dropped, `run` is still running and `broadcast_outlet`
        // still exists, so this is guaranteed to succeed
        let _ = self.broadcast_inlet.send((message, delivery_inlet)).await;

        // Similarly to `broadcast_inlet`, this is guaranteed to
        // await indefinitely or eventually succeed
        delivery_outlet.await.unwrap()
    }
}

mod run;
mod submit_request;
