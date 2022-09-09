use async_trait::async_trait;

use crate::order::Order;

use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Mutex,
};

pub struct LoopBack {
    sender: UnboundedSender<Vec<u8>>,
    receiver: Mutex<UnboundedReceiver<Vec<u8>>>,
}

impl LoopBack {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let receiver = Mutex::new(receiver);

        LoopBack { sender, receiver }
    }
}

#[async_trait]
impl Order for LoopBack {
    async fn order(&self, payload: &[u8]) {
        let _ = self.sender.send(payload.to_vec());
    }

    async fn deliver(&self) -> Vec<u8> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await.unwrap()
    }
}
