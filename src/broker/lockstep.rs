use talk::sync::promise::Promise;
use tokio::sync::mpsc::UnboundedSender;

type UsizeInlet = UnboundedSender<usize>;

pub(in crate::broker) struct Lockstep {
    index: usize,
    lock_promise: Promise<()>,
    free_inlet: UsizeInlet,
}

impl Lockstep {
    pub async fn lock(&mut self) {
        self.lock_promise.wait().await;
    }

    pub fn free(&self) {
        let _ = self.free_inlet.send(self.index);
    }
}
