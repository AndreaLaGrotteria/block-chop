use async_trait::async_trait;

use crate::total_order::Broadcast;

use sha1::{Digest, Sha1};

use std::{error::Error, net::SocketAddr};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Mutex,
};

pub struct HotStuff {
    read: Mutex<OwnedReadHalf>,
    write: Mutex<OwnedWriteHalf>,
}

impl HotStuff {
    pub async fn connect(addr: &SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr).await?;
        let (read, write) = stream.into_split();

        let read = Mutex::new(read);
        let write = Mutex::new(write);

        Ok(HotStuff { read, write })
    }
}

#[async_trait]
impl Broadcast for HotStuff {
    async fn order(&self, payload: &[u8]) {
        let mut write = self.write.lock().await;

        let magic: u32 = 0;
        let opcode: u8 = 100;
        let length: u32 = payload.len().try_into().unwrap();
        let mut hasher = Sha1::new();

        hasher.update(payload);

        let hash = hasher.finalize();

        write.write(&magic.to_le_bytes()).await.unwrap();
        write.write(&opcode.to_le_bytes()).await.unwrap();
        write.write(&length.to_le_bytes()).await.unwrap();
        write.write(&hash[0..4]).await.unwrap();
        write.write(payload).await.unwrap();
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut read = self.read.lock().await;

        let mut buf = vec![0; 13];

        read.read_exact(&mut buf).await.unwrap();

        let length = u32::from_le_bytes(buf[5..9].try_into().unwrap());
        let mut msg = vec![0; length.try_into().unwrap()];

        read.read_exact(&mut msg).await.unwrap();

        return msg;
    }
}
