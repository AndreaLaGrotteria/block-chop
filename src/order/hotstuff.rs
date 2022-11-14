use crate::order::Order;
use async_trait::async_trait;
use sha1::{Digest, Sha1};
use std::{error::Error, net::SocketAddr};
use talk::sync::fuse::Fuse;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};

type MessageInlet = UnboundedSender<Vec<u8>>;
type MessageOutlet = UnboundedReceiver<Vec<u8>>;

pub struct HotStuff {
    send_inlet: MessageInlet,
    deliver_outlet: Mutex<MessageOutlet>,
    _fuse: Fuse,
}

impl HotStuff {
    pub async fn connect(addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        let (read, write) = stream.into_split();

        let (send_inlet, send_outlet) = mpsc::unbounded_channel();
        let (deliver_inlet, deliver_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        fuse.spawn(HotStuff::send(write, send_outlet));
        fuse.spawn(HotStuff::receive(read, deliver_inlet));

        let deliver_outlet = Mutex::new(deliver_outlet);

        Ok(HotStuff {
            send_inlet,
            deliver_outlet,
            _fuse: fuse,
        })
    }

    async fn send(mut write: OwnedWriteHalf, mut send_outlet: MessageOutlet) {
        loop {
            let payload = if let Some(payload) = send_outlet.recv().await {
                payload
            } else {
                // `HotStuff` has dropped, shutdown
                return;
            };

            let magic: u32 = 0;
            let opcode: u8 = 100;
            let length: u32 = payload.len().try_into().unwrap();
            let mut hasher = Sha1::new();

            hasher.update(payload.as_slice());

            let hash = hasher.finalize();

            write.write(&magic.to_le_bytes()).await.unwrap();
            write.write(&opcode.to_le_bytes()).await.unwrap();
            write.write(&length.to_le_bytes()).await.unwrap();
            write.write(&hash[0..4]).await.unwrap();
            write.write(payload.as_slice()).await.unwrap();
        }
    }

    async fn receive(mut read: OwnedReadHalf, deliver_inlet: MessageInlet) {
        loop {
            let mut buf = vec![0; 13];

            read.read_exact(&mut buf).await.unwrap();

            let length = u32::from_le_bytes(buf[5..9].try_into().unwrap());
            let mut msg = vec![0; length.try_into().unwrap()];

            read.read_exact(&mut msg).await.unwrap();

            deliver_inlet.send(msg).unwrap();
        }
    }
}

#[async_trait]
impl Order for HotStuff {
    async fn order(&self, payload: &[u8]) {
        let _ = self.send_inlet.send(payload.to_vec());
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut deliver_outlet = self.deliver_outlet.lock().await;

        // `deliver_inlet` is held by `HotStuff::receive`, whose `Fuse`
        // is held alive by self: as a result, this call cannot fail
        deliver_outlet.recv().await.unwrap()
    }
}
