use crate::broadcast::Message;

use talk::{crypto::KeyChain, sync::fuse::Fuse};

use tokio::sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender};

type BroadcastInlet = MpscSender<Message>;
type BroadcastOutlet = MpscReceiver<Message>;

pub struct Client {
    broadcast_inlet: BroadcastInlet,
    _fuse: Fuse,
}

impl Client {
    pub fn new(id: u64, keychain: KeyChain) -> Self {
        let (broadcast_inlet, broadcast_outlet) = mpsc::channel(1); // Only one broadcast is performed at a time

        let fuse = Fuse::new();

        fuse.spawn(async move {
            let _ = Client::run(id, keychain, broadcast_outlet).await;
        });

        Client {
            broadcast_inlet,
            _fuse: fuse,
        }
    }

    pub async fn broadcast(&mut self, message: Message) {
        // If the `Client` still exists, then `_fuse` has not
        // dropped, `run` is still running and `broadcast_outlet`
        // still exists
        self.broadcast_inlet.send(message).await.unwrap();
    }

    async fn run(_id: u64, _keychain: KeyChain, _broadcast_outlet: BroadcastOutlet) {}
}
