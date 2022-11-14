use crate::order::Order;
use async_trait::async_trait;
use rand::Rng;
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

pub struct BftSmart {
    send_inlet: MessageInlet,
    deliver_outlet: Mutex<MessageOutlet>,
    _fuse: Fuse,
}

struct BftSmartState {
    write: OwnedWriteHalf,
    sequence: u32,
}

impl BftSmart {
    pub async fn connect(id: u32, addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        let (read, mut write) = stream.into_split();
        let session: u32 = rand::thread_rng().gen();

        Self::subscribe(id, session, &mut write).await;

        let sequence: u32 = 1;
        let state = BftSmartState { write, sequence };

        let (send_inlet, send_outlet) = mpsc::unbounded_channel();
        let (deliver_inlet, deliver_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        fuse.spawn(BftSmart::send(id, session, state, send_outlet));
        fuse.spawn(BftSmart::receive(read, deliver_inlet));

        let deliver_outlet = Mutex::new(deliver_outlet);

        Ok(Self {
            send_inlet,
            deliver_outlet,
            _fuse: fuse,
        })
    }

    async fn subscribe(id: u32, session: u32, write: &mut OwnedWriteHalf) {
        let totlen: u32 = 40;
        let msglen: u32 = 32;
        let view: u32 = 0;
        let rtype: u32 = 0;
        let sequence: u32 = 0;
        let opid: u32 = 0;
        let reply: u32 = u32::MAX;
        let contlen: u32 = 0;
        let padding: u32 = 0;

        write.write(&totlen.to_be_bytes()).await.unwrap();
        write.write(&msglen.to_be_bytes()).await.unwrap();
        write.write(&id.to_be_bytes()).await.unwrap();
        write.write(&view.to_be_bytes()).await.unwrap();
        write.write(&rtype.to_be_bytes()).await.unwrap();
        write.write(&session.to_be_bytes()).await.unwrap();
        write.write(&sequence.to_be_bytes()).await.unwrap();
        write.write(&opid.to_be_bytes()).await.unwrap();
        write.write(&reply.to_be_bytes()).await.unwrap();
        write.write(&contlen.to_be_bytes()).await.unwrap();
        write.write(&padding.to_be_bytes()).await.unwrap();
    }

    async fn send(id: u32, session: u32, mut state: BftSmartState, mut send_outlet: MessageOutlet) {
        loop {
            let payload = if let Some(payload) = send_outlet.recv().await {
                payload
            } else {
                // `BftSmart` has dropped, shutdown
                return;
            };

            let sequence: u32 = state.sequence;
            let view: u32 = 0;
            let rtype: u32 = 0;
            let opid: u32 = 0;
            let reply: u32 = u32::MAX;
            let contlen: u32 = payload.len().try_into().unwrap();
            let msglen: u32 = 32 + contlen;
            let padding: u32 = 0;
            let totlen: u32 = msglen + 8;

            let write = &mut state.write;

            write.write(&totlen.to_be_bytes()).await.unwrap();
            write.write(&msglen.to_be_bytes()).await.unwrap();
            write.write(&id.to_be_bytes()).await.unwrap();
            write.write(&view.to_be_bytes()).await.unwrap();
            write.write(&rtype.to_be_bytes()).await.unwrap();
            write.write(&session.to_be_bytes()).await.unwrap();
            write.write(&sequence.to_be_bytes()).await.unwrap();
            write.write(&opid.to_be_bytes()).await.unwrap();
            write.write(&reply.to_be_bytes()).await.unwrap();
            write.write(&contlen.to_be_bytes()).await.unwrap();
            write.write(payload.as_slice()).await.unwrap();
            write.write(&padding.to_be_bytes()).await.unwrap();

            state.sequence += 1;
        }
    }

    async fn receive(mut read: OwnedReadHalf, deliver_inlet: MessageInlet) {
        loop {
            let mut buf = vec![0; 40];

            read.read_exact(&mut buf).await.unwrap();

            let contlen = u32::from_be_bytes(buf[36..40].try_into().unwrap());
            let mut msg = vec![0; contlen.try_into().unwrap()];

            read.read_exact(&mut msg).await.unwrap();

            let mut _padding = vec![0; 4];
            read.read_exact(&mut _padding).await.unwrap();

            let _ = deliver_inlet.send(msg);
        }
    }
}

#[async_trait]
impl Order for BftSmart {
    async fn order(&self, payload: &[u8]) {
        let _ = self.send_inlet.send(payload.to_vec());
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut deliver_outlet = self.deliver_outlet.lock().await;

        // `deliver_inlet` is held by `BftSmart::receive`, whose `Fuse`
        // is held alive by self: as a result, this call cannot fail
        deliver_outlet.recv().await.unwrap()
    }
}
