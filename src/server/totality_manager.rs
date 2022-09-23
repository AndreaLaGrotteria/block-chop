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
    net::{Connector, Listener, Session, SessionConnector, SessionListener},
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

const UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const COLLECT_INTERVAL: Duration = Duration::from_millis(500);
const WAKE_INTERVAL: Duration = Duration::from_millis(100);

pub(in crate::server) struct TotalityManager {
    run_call_inlet: CallInlet,
    pull_outlet: BatchOutlet,
    _fuse: Fuse,
}

struct DeliveryQueue {
    offset: u64,
    entries: VecDeque<Option<Batch>>,
}

struct TotalityQueue {
    offset: u64,
    entries: VecDeque<Option<Arc<Vec<u8>>>>,
}

enum Call {
    Hit(Vec<u8>, Batch),
    Miss(Hash),
}

#[derive(Serialize, Deserialize)]
enum Request {
    Query(u64),
    Release(u64),
}

#[derive(Doom)]
enum QueryError {
    #[doom(description("Failed to connect"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
    #[doom(description("Batch unavailable"))]
    BatchUnavailable,
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Failed to expand batch"))]
    ExpandFailed,
    #[doom(description("Root mismatch"))]
    RootMismatch,
}

#[derive(Doom)]
enum ReleaseError {
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
    pub fn new<C, L>(membership: Membership, connector: C, listener: L) -> Self
    where
        C: Connector,
        L: Listener,
    {
        // Preprocess arguments

        let membership = Arc::new(membership);

        let connector = SessionConnector::new(connector);
        let connector = Arc::new(connector);

        let listener = SessionListener::new(listener);

        // Initialize common state

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

        // Initialize channels

        let (run_call_inlet, run_call_outlet) = mpsc::channel(PIPELINE);
        let (pull_inlet, pull_outlet) = mpsc::channel(PIPELINE);

        // Spawn tasks

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

        // Assemble `TotalityManager`

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

        let mut last_collect = Instant::now();
        let mut last_update = Instant::now();

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
                            // Make `compressed_batch` available to other servers,
                            // stage `batch` for delivery

                            delivery_queue.entries.push_back(Some(batch));

                            totality_queue
                                .lock()
                                .unwrap()
                                .entries
                                .push_back(Some(Arc::new(compressed_batch)));
                        }

                        Call::Miss(root) => {
                            // Compute the height of the current batch

                            let height = delivery_queue.offset + (delivery_queue.entries.len() as u64);

                            // Indicate that the current batch is unavailable to other
                            // servers, allocate an empty slot for its retrieval

                            delivery_queue.entries.push_back(None);
                            totality_queue.lock().unwrap().entries.push_back(None);

                            // Spawn `TotalityManager::retrieve` task

                            fuse.spawn(TotalityManager::retrieve(
                                height,
                                root,
                                membership.clone(),
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

                    // Only `Some` elements are `pop_front()`ed from `delivery_queue`,
                    // and `None` elements in `delivery_queue` are set to `Some` only
                    // upon receiving an entry from `run_entry_outlet`. Each entry in
                    // `run_entry_outlet` pertains to an initially `None` element in
                    // `delivery_queue`, and no entry in `run_entry_outlet` is duplicated.
                    // As a result, the `height`-th element of `delivery_queue` is
                    // guaranteed to still be available (and `None`).
                    *delivery_queue.entries.get_mut((height - delivery_queue.offset) as usize).unwrap() = Some(batch);
                }

                _ = time::sleep(WAKE_INTERVAL) => {}
            }

            // Deliver all `Some` elements at the beginning of `delivery_queue`
            // (block on the first `None` element to preserve batch ordering)

            while let Some(Some(_)) = delivery_queue.entries.front() {
                let batch = delivery_queue.entries.pop_front().unwrap().unwrap();
                delivery_queue.offset += 1;

                let _ = pull_inlet.send(batch).await;
            }

            // Every `COLLECT_INTERVAL`, garbage collect `totality_queue`

            if last_collect.elapsed() >= COLLECT_INTERVAL {
                // Compute minimum height below which all batches can be garbage
                // collected, i.e., the minimum value in `vector_clock` (remark:
                // each entry of `vector_clock` lower-bounds the value of
                // `delivery_queue.offset` at the corresponding server).

                let min_clock = vector_clock
                    .values()
                    .map(|clock| clock.load(Ordering::Relaxed))
                    .min()
                    .unwrap();

                // Garbage-collect all elements of `totality_queue` whose height
                // is smaller than `min_clock`, i.e., pop elements from the front of
                // `totality_queue` until `totality_queue.offset` matches `min_clock`

                let mut totality_queue = totality_queue.lock().unwrap();

                while totality_queue.offset < min_clock {
                    totality_queue.entries.pop_front();
                    totality_queue.offset += 1;
                }

                last_collect = Instant::now();
            }

            // Every `UPDATE_INTERVAL`, update all servers on the below which
            // all batches have been delivered, i.e., `delivery_queue.offset`

            if last_update.elapsed() >= UPDATE_INTERVAL {
                fuse.spawn(TotalityManager::update(
                    delivery_queue.offset,
                    membership.clone(),
                    connector.clone(),
                ));

                last_update = Instant::now();
            }
        }
    }

    async fn retrieve(
        height: u64,
        root: Hash,
        membership: Arc<Membership>,
        connector: Arc<SessionConnector>,
        run_entry_inlet: EntryInlet,
    ) {
        // Randomize query order (compute a random permutation of `membership` keys)

        let mut servers = membership
            .servers()
            .keys()
            .copied()
            .choose_multiple(&mut rand::thread_rng(), membership.servers().len())
            .into_iter();

        // Progressively query all elements of `servers` until the batch is retrieved

        let mut ask_tasks = FuturesUnordered::new();

        loop {
            if !ask_tasks.is_empty() {
                // Wait for an element of `ask_tasks` to complete (or `EXTEND TIMEOUT`)

                tokio::select! {
                    Some(batch) = ask_tasks.next() => {
                        if let Ok(batch) = batch {
                            let _ = run_entry_inlet.send((height, batch)).await;
                            return;
                        } else {
                            continue; // Back to waiting (if `ask_tasks` is still non-empty)
                        }
                    },
                    _ = time::sleep(EXTEND_TIMEOUT) => {}
                }
            }

            // Extend query to the next element of `servers`

            if let Some(server) = servers.next() {
                ask_tasks.push(TotalityManager::query(
                    height,
                    root,
                    server,
                    connector.clone(),
                ));
            }
        }
    }

    async fn query(
        height: u64,
        root: Hash,
        server: Identity,
        connector: Arc<SessionConnector>,
    ) -> Result<Batch, Top<QueryError>> {
        let mut session = connector
            .connect(server)
            .await
            .pot(QueryError::ConnectFailed, here!())?;

        session
            .send_plain(&Request::Query(height))
            .await
            .pot(QueryError::ConnectionError, here!())?;

        let hit = session
            .receive_plain::<bool>()
            .await
            .pot(QueryError::ConnectionError, here!())?;

        if !hit {
            session.end();
            return QueryError::BatchUnavailable.fail().spot(here!());
        }

        let batch = session
            .receive_raw_bytes()
            .await
            .pot(QueryError::ConnectionError, here!())?;

        // Expand using a blocking task to avoid clogging the event loop
        let batch = task::spawn_blocking(move || -> Result<Batch, Top<QueryError>> {
            let batch = bincode::deserialize(batch.as_slice())
                .map_err(QueryError::deserialize_failed)
                .map_err(Doom::into_top)
                .spot(here!())?;

            Batch::expand_unverified(batch).pot(QueryError::ExpandFailed, here!())
        })
        .await
        .unwrap()?;

        session.end();

        if batch.root() == root {
            Ok(batch)
        } else {
            QueryError::RootMismatch.fail()
        }
    }

    async fn update(height: u64, membership: Arc<Membership>, connector: Arc<SessionConnector>) {
        let fuse = Fuse::new();

        for server in membership.servers().keys().copied() {
            fuse.spawn(TotalityManager::release(server, height, connector.clone()));
        }
    }

    async fn release(
        server: Identity,
        height: u64,
        connector: Arc<SessionConnector>,
    ) -> Result<(), Top<ReleaseError>> {
        let mut session = connector
            .connect(server)
            .await
            .pot(ReleaseError::ConnectFailed, here!())?;

        session
            .send_plain(&Request::Release(height))
            .await
            .pot(ReleaseError::ConnectionError, here!())?;

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
                server,
                session,
                totality_queue.clone(),
                vector_clock.clone(),
            ));
        }
    }

    async fn serve(
        server: Identity,
        mut session: Session,
        totality_queue: Arc<Mutex<TotalityQueue>>,
        vector_clock: Arc<HashMap<Identity, AtomicU64>>,
    ) -> Result<(), Top<ServeError>> {
        let request = session
            .receive_plain::<Request>()
            .await
            .pot(ServeError::ConnectionError, here!())?;

        match request {
            Request::Query(height) => {
                let batch = {
                    let totality_queue = totality_queue.lock().unwrap();

                    if height >= totality_queue.offset {
                        totality_queue
                            .entries
                            .get((height - totality_queue.offset) as usize)
                            .cloned()
                            .flatten()
                    } else {
                        // Remark: in this case, `server` is probably Byzantine,
                        // as `TotalityManager` previously received its release
                        // to garbage collect below `height` (contrived delays
                        // could still have caused this out-of-order request).
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

            Request::Release(height) => {
                if let Some(clock) = vector_clock.get(&server) {
                    clock.fetch_max(height, Ordering::Relaxed); // Clocks can only move forward
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
    use std::{collections::HashMap, iter};
    use talk::{
        crypto::{KeyCard, KeyChain},
        net::test::{System, TestConnector, TestListener},
    };

    #[tokio::test]
    async fn all_hits() {
        let membership = Membership::new([]);

        let connector = TestConnector::new(KeyChain::random(), HashMap::new());
        let (listener, _) = TestListener::new(KeyChain::random()).await;

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

    #[tokio::test]
    async fn all_hits_all_misses() {
        let keychains = iter::repeat_with(KeyChain::random)
            .take(2)
            .collect::<Vec<_>>();

        let keycards = keychains.iter().map(KeyChain::keycard).collect::<Vec<_>>();

        let membership = Membership::new(keycards);
        let system = System::setup_with_keychains(keychains).await;

        let mut connectors = system.connectors.into_iter();
        let mut listeners = system.listeners.into_iter();

        let mut hitter = TotalityManager::new(
            membership.clone(),
            connectors.next().unwrap(),
            listeners.next().unwrap(),
        );

        let mut misser = TotalityManager::new(
            membership.clone(),
            connectors.next().unwrap(),
            listeners.next().unwrap(),
        );

        for _ in 0..128 {
            let compressed_batch = random_unauthenticated_batch(128, 32);
            let serialized_compressed_batch = bincode::serialize(&compressed_batch).unwrap();

            let batch = Batch::expand_unverified(compressed_batch).unwrap();
            let root = batch.root();

            hitter.hit(serialized_compressed_batch, batch).await;
            misser.miss(root).await;

            let hitter_batch = hitter.pull().await;
            let misser_batch = misser.pull().await;

            assert_eq!(hitter_batch.root(), root);
            assert_eq!(misser_batch.root(), root);
        }
    }
}
