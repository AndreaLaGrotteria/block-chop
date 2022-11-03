use chop_chop::{LoadBroker, LoadBrokerSettings, Membership};
use log::info;
use std::{
    cmp,
    fs::File,
    future,
    io::{self, Read, Write},
    path::PathBuf,
    time::Duration,
};
use talk::{
    crypto::{
        primitives::hash::{Hash, HASH_LENGTH},
        KeyChain,
    },
    link::{
        context::ConnectDispatcher,
        rendezvous::{Client as RendezvousClient, Connector as RendezvousConnector},
    },
    net::SessionConnector,
};
use tokio::time;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `LoadBroker` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <rate> (float) number of batches per second to submit
          <flows_path> (string) path to pre-generated flows
          <flow_range_start> (integer) start of the range of flows to broadcast
          <flow_range_end> (integer) end of the range of flows to broadcast
          --batches-per-flow (default 1920) number of batches per flow
          
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let rate = args.get_float("rate") as f64;
    let flows_path = args.get_string("flows_path");
    let flow_range_start = args.get_integer("flow_range_start") as usize;
    let flow_range_end = args.get_integer("flow_range_end") as usize;
    let batches_per_flow = args.get_integer("batches-per-flow") as usize;

    // Load `Membership`

    info!("Loading `Membership`..");

    let membership = Membership::load_exact(membership_path, membership_size).unwrap();

    // Setup `Connector`

    info!("Setting up `Connector`s..");

    let broker_keychain = KeyChain::random();
    let broker_identity = broker_keychain.keycard().identity();

    let connector = RendezvousConnector::new(
        rendezvous_address.clone(),
        broker_keychain.clone(),
        Default::default(),
    );

    let connect_dispatcher = ConnectDispatcher::new(connector);
    let broker_connector = connect_dispatcher.register("broker".to_string());
    let broker_connector = SessionConnector::new(broker_connector);

    // Load batches

    info!("Loading batches..");

    let batches = (0..batches_per_flow)
        .map(|batch_index| {
            let flows_path = flows_path.clone();

            (flow_range_start..flow_range_end).map(move |flow_index| {
                let mut path = PathBuf::from(flows_path.clone());
                path.push(format!("flow-{flow_index:02}"));
                path.push(format!("batch-{batch_index:05}.raw"));

                let mut file = File::open(path).unwrap();

                let mut root = [0u8; HASH_LENGTH];
                file.read_exact(&mut root).unwrap();

                let root = Hash::from_bytes(root);

                let mut batch = Vec::new();
                file.read_to_end(&mut batch).unwrap();

                (root, batch)
            })
        })
        .flatten()
        .collect::<Vec<_>>();

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers and brokers..");

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    // Start `LoadBroker`

    time::sleep(Duration::from_secs(5)).await;

    info!("Starting `LoadBroker`..");

    let _broker = LoadBroker::new(
        membership,
        broker_identity,
        broker_connector,
        batches,
        LoadBrokerSettings {
            rate,
            workers: cmp::max((rate * 5.) as u16, 1),
            ..Default::default()
        },
    );

    println!(" .. done! `LoadBroker` running!");

    // Wait indefinitely

    print!("\n    [Hit Ctrl + C to stop this daemon]  ");
    io::stdout().flush().unwrap();
    future::pending::<()>().await;
}
