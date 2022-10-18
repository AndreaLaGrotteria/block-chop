use chop_chop::{Directory, HotStuff, LoopBack, Membership, Order, Passepartout, Server};
use log::info;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
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
        
        Underlying Total Order Broadcast (choose one):
          --loopback use `LoopBack` order (warning: does not actually guarantee Total Order!)
          --hotstuff (string) address to `HotStuff`'s endpoint
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let server_index = args.get_integer("server_index") as usize;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

    let loopback = args.get_bool("loopback");
    let hotstuff = args.get_string_result("hotstuff").ok();

    let orders_selected = if loopback { 1 } else { 0 } + if hotstuff.is_some() { 1 } else { 0 };

    if orders_selected == 0 {
        println!("Please select the underlying Total Order Broadcast.");
        return;
    } else if orders_selected > 1 {
        println!("Please select only one underlying Total Order Broadcast.");
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
    } else if let Some(hotstuff) = hotstuff {
        Arc::new(HotStuff::connect(&hotstuff.parse().unwrap()).await.unwrap())
    } else {
        unreachable!();
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

    // Start `Server`

    info!("Starting `Server`..");

    let mut server = Server::new(
        keychain,
        membership,
        directory,
        order,
        broker_listener,
        totality_connector,
        totality_listener,
        Default::default(),
    );

    // Pull batches

    let counter = Arc::new(AtomicUsize::new(0));

    {
        let counter = counter.clone();

        tokio::spawn(async move {
            loop {
                let batch = server.next_batch().await;
                counter.fetch_add(batch.count(), Ordering::Relaxed);
            }
        });
    }

    // Log progress

    let mut last_count = 0;
    let mut operations_performed: VecDeque<usize> = VecDeque::with_capacity(120);

    loop {
        time::sleep(Duration::from_secs(1)).await;

        let total = counter.load(Ordering::Relaxed);
        operations_performed.push_front(total - last_count);

        info!(
            "{:.02} MOPps ({:.02} MOPps average, {} MOPs total).",
            ((total - last_count) as f64) / 1e6,
            (operations_performed
                .iter()
                .copied()
                .take(AVERAGING_INTERVAL)
                .sum::<usize>() as f64
                / std::cmp::min(operations_performed.len(), AVERAGING_INTERVAL) as f64)
                / 1e6,
            total / 1000000
        );

        last_count = total;
    }
}
