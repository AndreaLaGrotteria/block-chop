use crate::broadcast::Message;

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramDispatcherSettings},
    sync::fuse::Fuse,
};

use tokio::{
    io,
    net::{self, ToSocketAddrs},
    sync::{
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
        Mutex as TokioMutex,
    },
    time,
};

type BroadcastInlet = MpscSender<Message>;
type BroadcastOutlet = MpscReceiver<Message>;

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
    pub fn new(id: u64, keychain: KeyChain) -> Self {
        let brokers = Arc::new(StdMutex::new(Vec::new()));

        let (broadcast_inlet, broadcast_outlet) = mpsc::channel(1); // Only one broadcast is performed at a time
        let broadcast_inlet = TokioMutex::new(broadcast_inlet);

        let fuse = Fuse::new();

        {
            let brokers = brokers.clone();
            fuse.spawn(Client::run(id, keychain, brokers, broadcast_outlet));
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

    async fn run(
        _id: u64,
        _keychain: KeyChain,
        brokers: Arc<StdMutex<Vec<SocketAddr>>>,
        mut broadcast_outlet: BroadcastOutlet,
    ) {
        let dispatcher = DatagramDispatcher::bind(
            "0.0.0.0:0",
            DatagramDispatcherSettings {
                workers: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap(); // TODO: Determine if this error should be handled

        let (sender, _receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        let _sequence = 0..=0;

        loop {
            // Wait for the next message to broadcast

            let _message = match broadcast_outlet.recv().await {
                Some(message) => message,
                None => return, // `Client` has dropped, shutdown
            };

            // Spawn requesting task

            let fuse = Fuse::new();

            {
                let brokers = brokers.clone();
                let _sender = sender.clone();

                fuse.spawn(async move {
                    for index in 0.. {
                        // Fetch next broker

                        let _broker = loop {
                            if let Some(broker) = brokers.lock().unwrap().get(index).cloned() {
                                break broker;
                            }

                            time::sleep(Duration::from_secs(1)).await;
                        };
                    }
                });
            }
        }
    }
}
