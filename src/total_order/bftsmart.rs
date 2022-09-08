use async_trait::async_trait;

use crate::total_order::Broadcast;

use rand::Rng;

use std::{error::Error, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Mutex,
};

struct BftSmartState {
    write: OwnedWriteHalf,
    sequence: u32,
}

pub struct BftSmart {
    read: Mutex<OwnedReadHalf>,
    state: Mutex<BftSmartState>,
    id: u32,
    session: u32,
}

impl BftSmart {
    pub async fn connect(id: u32, addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        let (read, mut write) = stream.into_split();
        let session: u32 = rand::thread_rng().gen();

        Self::subscribe(id, session, &mut write).await;

        let read = Mutex::new(read);
        let sequence: u32 = 1;
        let state = Mutex::new(BftSmartState { write, sequence });

        Ok(Self {
            read,
            state,
            id,
            session,
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
}

#[async_trait]
impl Broadcast for BftSmart {
    async fn order(&self, payload: &[u8]) {
        let mut state = self.state.lock().await;

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
        write.write(&self.id.to_be_bytes()).await.unwrap();
        write.write(&view.to_be_bytes()).await.unwrap();
        write.write(&rtype.to_be_bytes()).await.unwrap();
        write.write(&self.session.to_be_bytes()).await.unwrap();
        write.write(&sequence.to_be_bytes()).await.unwrap();
        write.write(&opid.to_be_bytes()).await.unwrap();
        write.write(&reply.to_be_bytes()).await.unwrap();
        write.write(&contlen.to_be_bytes()).await.unwrap();
        write.write(payload).await.unwrap();
        write.write(&padding.to_be_bytes()).await.unwrap();

        state.sequence += 1;
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut read = self.read.lock().await;

        let mut buf = vec![0; 40];

        read.read_exact(&mut buf).await.unwrap();

        let contlen = u32::from_be_bytes(buf[36..40].try_into().unwrap());
        let mut msg = vec![0; contlen.try_into().unwrap()];

        read.read_exact(&mut msg).await.unwrap();

        let mut _padding = vec![0; 4];
        read.read_exact(&mut _padding).await.unwrap();

        return msg;
    }
}
