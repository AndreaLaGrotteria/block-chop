use chop_chop::{
    applications::{
        auctions::Processor as AuctionsProcessor, payments::Processor as PaymentsProcessor,
        pixel_war::Processor as PixelWarProcessor,
    },
    heartbeat, BftSmart, Blockchain, Directory, HotStuff, LoopBack, Membership, Order, Passepartout, Server,
    ServerSettings,
};
// use chrono::{Timelike, Utc};
use futures::stream::StreamExt;
use log::info;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use talk::link::{
    context::{ConnectDispatcher, ListenDispatcher},
    rendezvous::{
        Client as RendezvousClient, Connector as RendezvousConnector,
        Listener as RendezvousListener,
    },
};
use tokio::time;

const AVERAGING_INTERVAL: usize = 5;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `Server` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <server_index> (integer) index of this server (in `0..membership_size`)
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
          --gc_exclude (default 0) number of servers to exclude for garbage collection
        
        Underlying Total Order Broadcast (choose one):
          --loopback use `LoopBack` order (warning: does not actually guarantee Total Order!)
          --hotstuff (string) address to `HotStuff`'s endpoint
          --bftsmart (string) address to `BftSmart`'s endpoint
          --blockchain (string) address of `Blockchain`'s smart contract, unlock account and ipc path (semi-column separated)

        Application messages (choose one):
          --random
          --payments
          --auction
          --pixel_war

        Heartbeat:
          --heartbeat-path (string) path to save `heartbeat` data
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let server_index = args.get_integer("server_index") as usize;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");
    let gc_exclude = args.get_integer("gc_exclude") as usize;
    let heartbeat_path = args.get_string_result("heartbeat-path").ok();

    let loopback = args.get_bool("loopback");
    let hotstuff = args.get_string_result("hotstuff").ok();
    let bftsmart = args.get_string_result("bftsmart").ok();
    let blockchain = args.get_string_result("blockchain").ok();

    let random = args.get_bool("random");
    let payments = args.get_bool("payments");
    let auction = args.get_bool("auction");
    let pixel_war = args.get_bool("pixel_war");

    let orders_selected = if loopback { 1 } else { 0 }
        + if blockchain.is_some() { 1 } else { 0 }
        + if hotstuff.is_some() { 1 } else { 0 }
        + if bftsmart.is_some() { 1 } else { 0 };

    if orders_selected == 0 {
        println!("Please select the underlying Total Order Broadcast.");
        return;
    } else if orders_selected > 1 {
        println!("Please select only one underlying Total Order Broadcast.");
        return;
    }

    let applications_selected = if random { 1 } else { 0 }
        + if payments { 1 } else { 0 }
        + if auction { 1 } else { 0 }
        + if pixel_war { 1 } else { 0 };

    if applications_selected == 0 {
        println!("Please select the underlying application message type.");
        return;
    } else if applications_selected > 1 {
        println!("Please select only one underlying application message type.");
        return;
    }

    // Load `Membership`

    info!("Loading `Membership`..");

    let membership = Membership::load_exact(membership_path, membership_size).unwrap();

    // Load own `KeyChain`

    info!("Loading `KeyChain`..");

    let identity = membership
        .servers()
        .keys()
        .copied()
        .collect::<Vec<_>>()
        .get(server_index)
        .cloned()
        .unwrap();

    let keychain = Passepartout::fetch(passepartout_path, identity)
        .unwrap()
        .unwrap();

    // Load `Directory`

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    // Setup `Order`

    info!("Setting up `Order`..");

    let order: Arc<dyn Order> = if loopback {
        Arc::new(LoopBack::new())
    } else if let Some(blockchain) = blockchain {
        let splits: Vec<&str> = blockchain.split(";").collect();
        Arc::new(Blockchain::connect(splits[0], splits[1], splits[2]).await.unwrap())
    } else if let Some(hotstuff) = hotstuff {
        Arc::new(HotStuff::connect(&hotstuff.parse().unwrap()).await.unwrap())
    } else if let Some(bftsmart) = bftsmart {
        Arc::new(
            BftSmart::connect(server_index as u32, &bftsmart.parse().unwrap())
                .await
                .unwrap(),
        )
    } else {
        unreachable!()
    };

    // Setup `ConnectDispatcher` and `ListenDispatcher`

    info!("Setting up `ConnectDispatcher` and `ListenDispatcher`..");

    let connector = RendezvousConnector::new(
        rendezvous_address.clone(),
        keychain.clone(),
        Default::default(),
    );

    let connect_dispatcher = ConnectDispatcher::new(connector);

    let listener = RendezvousListener::new(
        rendezvous_address.clone(),
        keychain.clone(),
        Default::default(),
    )
    .await;

    let listen_dispatcher = ListenDispatcher::new(listener, Default::default());

    // Setup context `Connector`s and `Listener`s

    info!("Setting up `Connector`s and `Listener`s..");

    let broker_listener = listen_dispatcher.register("broker".to_string());
    let totality_connector = connect_dispatcher.register("totality".to_string());
    let totality_listener = listen_dispatcher.register("totality".to_string());

    // Start `Server`

    info!("Starting `Server`..");

    let num_clients = directory.capacity() as u64;

    info!("Directory capacity {}", num_clients);

    let mut server = Server::new(
        keychain.clone(),
        membership,
        directory,
        order,
        broker_listener,
        totality_connector,
        totality_listener,
        ServerSettings {
            garbage_collect_excluded: gc_exclude,
            ..Default::default()
        },
    );

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers and brokers..");

    let rendezvous_client = RendezvousClient::new(rendezvous_address.clone(), Default::default());

    rendezvous_client
        .publish_card(keychain.keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    // Pull batches

    let counter = Arc::new(AtomicUsize::new(0));

    {
        let counter = counter.clone();

        tokio::spawn(async move {
            if random {
                loop {
                    let batch = server.next_batch().await;
                    counter.fetch_add(batch.count(), Ordering::Relaxed);
                }
            } else if payments {
                let processor = Arc::new(PaymentsProcessor::new(
                    num_clients,
                    100_000,
                    Default::default(),
                ));

                {
                    let processor = processor.clone();

                    tokio::spawn(async move {
                        loop {
                            time::sleep(Duration::from_millis(100)).await;

                            counter.store(
                                processor.operations_processed() as usize,
                                Ordering::Relaxed,
                            );
                        }
                    });
                }

                loop {
                    let batch = server.next_batch().await;
                    processor.push(batch).await;
                }
            } else if auction {
                let owners = (0..num_clients / 65536)
                    .map(|num| num * 65536)
                    .collect::<Vec<_>>();

                let processor = Arc::new(AuctionsProcessor::new(
                    num_clients,
                    100_000,
                    owners,
                    Default::default(),
                ));

                {
                    let processor = processor.clone();

                    tokio::spawn(async move {
                        loop {
                            time::sleep(Duration::from_millis(100)).await;

                            counter.store(
                                processor.operations_processed() as usize,
                                Ordering::Relaxed,
                            );
                        }
                    });
                }

                loop {
                    let batch = server.next_batch().await;
                    processor.push(batch).await;
                }
            } else if pixel_war {
                let processor =
                    Arc::new(PixelWarProcessor::new(num_clients, 5, Default::default()));

                {
                    let processor = processor.clone();

                    tokio::spawn(async move {
                        loop {
                            time::sleep(Duration::from_millis(100)).await;

                            counter.store(
                                processor.operations_processed() as usize,
                                Ordering::Relaxed,
                            );
                        }
                    });
                }

                loop {
                    let batch = server.next_batch().await;
                    processor.push(batch).await;
                }
            } else {
                unreachable!()
            }
        });
    }

    // Log progress

    tokio::spawn(async move {
        let mut rates = VecDeque::with_capacity(120);

        let mut last_count = 0;
        let mut last_time = Instant::now();

        loop {
            time::sleep(Duration::from_secs(1)).await;

            let total = counter.load(Ordering::Relaxed);
            let rate = (total - last_count) as f64 / last_time.elapsed().as_secs_f64();

            last_count = total;
            last_time = Instant::now();

            rates.push_front(rate);

            info!(
                "{:.04} MOPps ({:.04} MOPps average, {} MOPs total).",
                rate / 1e6,
                (rates.iter().copied().take(AVERAGING_INTERVAL).sum::<f64>()
                    / std::cmp::min(rates.len(), AVERAGING_INTERVAL) as f64)
                    / 1e6,
                total / 1000000
            );
        }
    });

    // Wait for `Ctrl + C`

    let mut signals = Signals::new(&[SIGTERM, SIGINT]).unwrap();
    signals.next().await;

    info!("`Ctrl + C` detected, shutting down..");

    if let Some(heartbeat_path) = heartbeat_path {
        // let time = Utc::now();

        let heartbeat_path = PathBuf::from(heartbeat_path);

        // heartbeat_path.push(format!(
        //     "heartbeat-server{}-{}h{}m{}s.bin",
        //     server_index,
        //     time.hour(),
        //     time.minute(),
        //     time.second()
        // ));

        println!("Saving heartbeat data to {}", heartbeat_path.display());

        let entries = heartbeat::flush();

        let file = File::create(heartbeat_path).unwrap();
        let mut file = BufWriter::new(file);

        bincode::serialize_into(&mut file, &entries).unwrap();

        file.flush().unwrap();
    }

    info!("All done! Chop CHOP!");
}
