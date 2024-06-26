use crate::order::Order;
use async_trait::async_trait;
use std::{error::Error, sync::Arc, path::Path};
use talk::sync::fuse::Fuse;
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};
use ethers::{
    contract::abigen,
    core::types::Address,
    types::U256,
    providers::{Provider, StreamExt, Http, Ipc}, signers::{Wallet, LocalWallet}, prelude::{SignerMiddleware, k256::{ecdsa::{self, SigningKey}, Secp256k1}, EthError},
};
use std::convert::TryFrom;

type MessageInlet = UnboundedSender<Vec<u8>>;
type MessageOutlet = UnboundedReceiver<Vec<u8>>;

abigen!(Consensus,"./src/order/Consensus.json");

pub struct Blockchain {
    send_inlet: MessageInlet,
    deliver_outlet: Mutex<MessageOutlet>,
    _fuse: Fuse,
}

impl Blockchain{
    pub async fn connect(addr: &str, from: &str, path: &str) -> Result<Self, Box<dyn Error>> {
        let contract_addr = addr.parse::<Address>().expect("Wrong address");
        let from_addr = from.parse::<Address>().expect("Wrong address");
        let path_ipc = Path::new(path);

        let provider = Provider::connect_ipc(path_ipc).await.unwrap().with_sender(from_addr);
        let client = Arc::new(provider);
        
        let contract = Consensus::new(contract_addr,client);

        let (send_inlet, send_outlet) = mpsc::unbounded_channel();
        let (deliver_inlet, deliver_outlet) = mpsc::unbounded_channel();

        let fuse = Fuse::new();

        fuse.spawn(Blockchain::send(contract.to_owned(),send_outlet));
        fuse.spawn(Blockchain::receive(contract.to_owned(), deliver_inlet));

        let deliver_outlet = Mutex::new(deliver_outlet);

        Ok(Blockchain {
            send_inlet,
            deliver_outlet,
            _fuse: fuse,
        })
    }

    async fn send(contract: Consensus<Provider<Ipc>>, mut send_outlet: MessageOutlet){
        loop {
            let payload = if let Some(payload) = send_outlet.recv().await {
                payload
            } else {
                // `Blockchain connection` has dropped, shutdown
                return;
            };

            contract.method::<_,U256>("submitTest",payload).expect("Wrong submit").legacy().send().await.ok();

        }

    }

    async fn receive(contract: Consensus<Provider<Ipc>>, deliver_inlet: MessageInlet){
        let inlet = &deliver_inlet;

        let events = contract.events();        
        let stream = events.stream().await.expect("error stream");

        stream.for_each_concurrent(None, |event_result| async move{
            let event = event_result.expect("event result error");
            inlet.send(event.payload.to_owned()).unwrap();
        }).await;
    }
}

#[async_trait]
impl Order for Blockchain {
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