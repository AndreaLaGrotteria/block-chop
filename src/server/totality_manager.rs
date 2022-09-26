use crate::{server::Batch, system::Membership};
use doomstack::{here, Doom, ResultExt, Top};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use talk::{
    crypto::{primitives::hash::Hash, Identity},
    net::{Session, SessionConnector, SessionListener},
    sync::fuse::Fuse,
};
use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
    task, time,
};

type CallInlet = MpscSender<Call>;
type CallOutlet = MpscReceiver<Call>;

type BatchInlet = MpscSender<Batch>;
type BatchOutlet = MpscReceiver<Batch>;

type EntryInlet = MpscSender<(u64, Batch)>;
type EntryOutlet = MpscReceiver<(u64, Batch)>;

// TODO: Refactor constants into settings

const PIPELINE: usize = 8192;
const EXTEND_TIMEOUT: Duration = Duration::from_secs(2);

const SYNC_INTERVAL: Duration = Duration::from_secs(1);
const COLLECT_INTERVAL: Duration = Duration::from_millis(500);
const WAKE_INTERVAL: Duration = Duration::from_millis(100);

pub(in crate::server) struct TotalityManager {
    run_call_inlet: CallInlet,
    pull_outlet: BatchOutlet,
    _fuse: Fuse,
}

enum Call {
    Hit(Vec<u8>, Batch),
    Miss(Hash),
}

struct DeliveryQueue {
    offset: u64,
    entries: VecDeque<Option<Batch>>,
}

struct TotalityQueue {
    offset: u64,
    entries: VecDeque<Option<Arc<Vec<u8>>>>,
}

#[derive(Serialize, Deserialize)]
enum Request {
    Retrieve(u64),
    Collect(u64),
}

#[derive(Doom)]
enum AskError {
    #[doom(description("Failed to connect"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Batch miss"))]
    Miss,
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Failed to expand batch"))]
    ExpandFailed,
    #[doom(description("Root mismatch"))]
    RootMismatch,
}

#[derive(Doom)]
enum UpdateError {
    #[doom(description("Failed to connect"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

#[derive(Doom)]
enum ServeError {
    #[doom(description("Connnection error"))]
    ConnectionError,
}

impl TotalityManager {
    pub fn new(
        membership: Membership,
        connector: SessionConnector,
        listener: SessionListener,
    ) -> Self {
        let membership = Arc::new(membership);
        let connector = Arc::new(connector);

        let totality_queue = TotalityQueue {
            offset: 0,
            entries: VecDeque::new(),
        };

        let totality_queue = Arc::new(Mutex::new(totality_queue));

        let vector_clock = membership
            .servers()
            .keys()
            .copied()
            .map(|identity| (identity, AtomicU64::new(0)))
            .collect::<HashMap<_, _>>();

        let vector_clock = Arc::new(vector_clock);

        let (run_call_inlet, run_call_outlet) = mpsc::channel(PIPELINE);
        let (pull_inlet, pull_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        fuse.spawn(TotalityManager::run(
            membership,
            totality_queue.clone(),
            vector_clock.clone(),
            connector.clone(),
            run_call_outlet,
            pull_inlet,
        ));

        fuse.spawn(TotalityManager::listen(
            totality_queue.clone(),
            vector_clock.clone(),
            listener,
        ));

        TotalityManager {
            run_call_inlet,
            pull_outlet,
            _fuse: fuse,
        }
    }

    pub async fn hit(&self, compressed_batch: Vec<u8>, batch: Batch) {
        let _ = self
            .run_call_inlet
            .send(Call::Hit(compressed_batch, batch))
            .await;
    }

    pub async fn miss(&self, root: Hash) {
        let _ = self.run_call_inlet.send(Call::Miss(root)).await;
    }

    pub async fn pull(&mut self) -> Batch {
        // The `Fuse` to `TotalityManager::run` is owned by
        // `self`, so `pull_inlet` cannot have been dropped
        self.pull_outlet.recv().await.unwrap()
    }

    async fn run(
        membership: Arc<Membership>,
        totality_queue: Arc<Mutex<TotalityQueue>>,
        vector_clock: Arc<HashMap<Identity, AtomicU64>>,
        connector: Arc<SessionConnector>,
        mut run_call_outlet: CallOutlet,
        pull_inlet: BatchInlet,
    ) {
        let mut delivery_queue = DeliveryQueue {
            offset: 0,
            entries: VecDeque::new(),
        };

        let (run_entry_inlet, mut run_entry_outlet) = mpsc::channel(PIPELINE);

        let fuse = Fuse::new();

        let mut last_sync = Instant::now();
        let mut last_collect = Instant::now();

        loop {
            tokio::select! {
                call = run_call_outlet.recv() => {
                    let call = if let Some(call) = call {
                        call
                    } else {
                        // `TotalityManager` has dropped, shutdown
                        return;
                    };

                    match call {
                        Call::Hit(compressed_batch, batch) => {
                            delivery_queue.entries.push_back(Some(batch));

                            totality_queue
                                .lock()
                                .unwrap()
                                .entries
                                .push_back(Some(Arc::new(compressed_batch)));
                        }

                        Call::Miss(root) => {
                            delivery_queue.entries.push_back(None);
                            totality_queue.lock().unwrap().entries.push_back(None);

                            let height =
                                delivery_queue.offset + ((delivery_queue.entries.len() - 1) as u64);

                            fuse.spawn(TotalityManager::retrieve(
                                membership.clone(),
                                height,
                                root,
                                connector.clone(),
                                run_entry_inlet.clone(),
                            ));
                        }
                    }
                }

                entry = run_entry_outlet.recv() => {
                    let (height, batch) = if let Some(entry) = entry {
                        entry
                    } else {
                        // `TotalityManager` has dropped, shutdown
                        return;
                    };

                    *delivery_queue.entries.get_mut((height - delivery_queue.offset) as usize).unwrap() = Some(batch);
                }

                _ = time::sleep(WAKE_INTERVAL) => {}
            }

            while delivery_queue.entries.front().is_some()
                && delivery_queue.entries.front().unwrap().is_some()
            {
                let batch = delivery_queue.entries.pop_front().unwrap().unwrap();
                delivery_queue.offset += 1;

                let _ = pull_inlet.send(batch).await;
            }

            if last_collect.elapsed() >= COLLECT_INTERVAL {
                let max_clock = vector_clock
                    .values()
                    .map(|clock| clock.load(Ordering::Relaxed))
                    .max()
                    .unwrap();

                let mut totality_queue = totality_queue.lock().unwrap();

                while totality_queue.offset > max_clock {
                    totality_queue.entries.pop_front();
                    totality_queue.offset += 1;
                }
            }

            if last_sync.elapsed() >= SYNC_INTERVAL {
                fuse.spawn(TotalityManager::sync(
                    membership.clone(),
                    delivery_queue.offset,
                    connector.clone(),
                ));

                last_sync = Instant::now();
            }
        }
    }

    async fn retrieve(
        membership: Arc<Membership>,
        height: u64,
        root: Hash,
        connector: Arc<SessionConnector>,
        run_entry_inlet: EntryInlet,
    ) {
        let mut servers = membership
            .servers()
            .keys()
            .copied()
            .choose_multiple(&mut rand::thread_rng(), membership.servers().len())
            .into_iter();

        let mut ask_tasks = FuturesUnordered::new();

        loop {
            if !ask_tasks.is_empty() {
                tokio::select! {
                    Some(batch) = ask_tasks.next() => {
                        if let Ok(batch) = batch {
                            let _ = run_entry_inlet.send((height, batch)).await;
                            return;
                        } else {
                            continue;
                        }
                    },
                    _ = time::sleep(EXTEND_TIMEOUT) => {}
                }
            }

            if let Some(server) = servers.next() {
                ask_tasks.push(TotalityManager::ask(
                    height,
                    root,
                    server,
                    connector.clone(),
                ));
            }
        }
    }

    async fn ask(
        height: u64,
        root: Hash,
        server: Identity,
        connector: Arc<SessionConnector>,
    ) -> Result<Batch, Top<AskError>> {
        let mut session = connector
            .connect(server)
            .await
            .pot(AskError::ConnectFailed, here!())?;

        session
            .send_plain(&Request::Retrieve(height))
            .await
            .pot(AskError::ConnectionError, here!())?;

        let hit = session
            .receive_plain::<bool>()
            .await
            .pot(AskError::ConnectionError, here!())?;

        if !hit {
            session.end();
            return AskError::Miss.fail().spot(here!());
        }

        let batch = session
            .receive_raw_bytes()
            .await
            .pot(AskError::ConnectionError, here!())?;

        let batch = task::spawn_blocking(move || -> Result<Batch, Top<AskError>> {
            let batch = bincode::deserialize(batch.as_slice())
                .map_err(AskError::deserialize_failed)
                .map_err(Doom::into_top)
                .spot(here!())?;

            Batch::expand_unverified(batch).pot(AskError::ExpandFailed, here!())
        })
        .await
        .unwrap()?;

        session.end();

        if batch.root() == root {
            Ok(batch)
        } else {
            AskError::RootMismatch.fail()
        }
    }

    async fn sync(membership: Arc<Membership>, height: u64, connector: Arc<SessionConnector>) {
        let fuse = Fuse::new();

        for server in membership.servers().keys().copied() {
            fuse.spawn(TotalityManager::update(server, height, connector.clone()));
        }
    }

    async fn update(
        server: Identity,
        height: u64,
        connector: Arc<SessionConnector>,
    ) -> Result<(), Top<UpdateError>> {
        let mut session = connector
            .connect(server)
            .await
            .pot(UpdateError::ConnectFailed, here!())?;

        session
            .send_plain(&Request::Collect(height))
            .await
            .pot(UpdateError::ConnectionError, here!())?;

        session.end();

        Ok(())
    }

    async fn listen(
        totality_queue: Arc<Mutex<TotalityQueue>>,
        vector_clock: Arc<HashMap<Identity, AtomicU64>>,
        mut listener: SessionListener,
    ) {
        let fuse = Fuse::new();

        loop {
            let (server, session) = listener.accept().await;

            fuse.spawn(TotalityManager::serve(
                totality_queue.clone(),
                vector_clock.clone(),
                server,
                session,
            ));
        }
    }

    async fn serve(
        totality_queue: Arc<Mutex<TotalityQueue>>,
        vector_clock: Arc<HashMap<Identity, AtomicU64>>,
        server: Identity,
        mut session: Session,
    ) -> Result<(), Top<ServeError>> {
        let request = session
            .receive_plain::<Request>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        match request {
            Request::Retrieve(height) => {
                let batch = {
                    let totality_queue = totality_queue.lock().unwrap();

                    if height >= totality_queue.offset {
                        totality_queue
                            .entries
                            .get((height - totality_queue.offset) as usize)
                            .cloned()
                            .flatten()
                    } else {
                        None
                    }
                };

                if let Some(batch) = batch {
                    session
                        .send_plain(&true)
                        .await
                        .pot(ServeError::ConnectionError, here!())?;

                    session
                        .send_raw_bytes(batch.as_slice())
                        .await
                        .pot(ServeError::ConnectionError, here!())?;
                } else {
                    session
                        .send_plain(&false)
                        .await
                        .pot(ServeError::ConnectionError, here!())?;
                }
            }

            Request::Collect(height) => {
                if let Some(clock) = vector_clock.get(&server) {
                    clock.fetch_max(height, Ordering::Relaxed);
                }
            }
        }

        session.end();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast::test::random_unauthenticated_batch;
    use std::collections::HashMap;
    use talk::{
        crypto::KeyChain,
        net::test::{TestConnector, TestListener},
    };

    #[tokio::test]
    async fn all_hits() {
        let membership = Membership::new([]);

        let connector =
            SessionConnector::new(TestConnector::new(KeyChain::random(), HashMap::new()));

        let (listener, _) = TestListener::new(KeyChain::random()).await;
        let listener = SessionListener::new(listener);

        let mut totality_manager = TotalityManager::new(membership, connector, listener);

        for _ in 0..128 {
            let compressed_batch = random_unauthenticated_batch(128, 32);
            let serialized_compressed_batch = bincode::serialize(&compressed_batch).unwrap();

            let batch = Batch::expand_unverified(compressed_batch).unwrap();
            let root = batch.root();

            totality_manager
                .hit(serialized_compressed_batch, batch)
                .await;

            let batch = totality_manager.pull().await;

            assert_eq!(batch.root(), root);
        }
    }
}
