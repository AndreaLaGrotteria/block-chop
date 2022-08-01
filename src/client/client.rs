use talk::{crypto::KeyChain, sync::fuse::Fuse};

pub struct Client {
    _fuse: Fuse,
}

impl Client {
    pub fn new(id: u64, keychain: KeyChain) -> Self {
        let fuse = Fuse::new();

        fuse.spawn(async move {
            let _ = Client::run(id, keychain).await;
        });

        Client { _fuse: fuse }
    }

    async fn run(_id: u64, _keychain: KeyChain) {}
}
