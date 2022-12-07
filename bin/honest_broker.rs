use chop_chop::{heartbeat, Broker, BrokerSettings, Directory, Membership};
// use chrono::{Timelike, Utc};
use futures::StreamExt;
use log::info;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook_tokio::Signals;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::Duration,
};
use talk::{
    crypto::KeyChain,
    link::{
        context::ConnectDispatcher,
        rendezvous::{Client as RendezvousClient, Connector as RendezvousConnector},
    },
    net::{PlexConnector, PlexConnectorSettings},
};
use tokio::time;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `Broker` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <client_port> (integer) `Broker` port to which clients connect
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
          --margin (default 0) number of extra servers to request immediate witnessing

        Heartbeat:
          --heartbeat-path (string) path to save `heartbeat` data
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let port = args.get_integer("client_port") as u16;
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");
    let heartbeat_path = args.get_string_result("heartbeat-path").ok();
    let margin = args.get_integer("margin") as usize;

    // Load `Membership`

    info!("Loading `Membership`..");

    let membership = Membership::load_exact(membership_path, membership_size).unwrap();

    // Load `Directory`

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

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

    let plex_connector_settings = PlexConnectorSettings {
        connections_per_remote: 32,
        ..Default::default()
    };

    let broker_connector = PlexConnector::new(broker_connector, plex_connector_settings);

    // Fill `connector`

    info!("Filling `PlexConnector`..");

    broker_connector
        .fill(
            membership.servers().keys().copied(),
            Duration::from_millis(100),
        )
        .await;

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers, load brokers..");

    let keychain = KeyChain::random();

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    rendezvous_client
        .advertise_port(keychain.keycard().identity(), port)
        .await;

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    // Start `Broker`

    info!("Starting `Broker`..");

    let bind_address = "0.0.0.0:".to_owned() + &format!("{}", port);

    let _broker = Broker::new(
        membership,
        directory,
        bind_address,
        broker_identity,
        broker_connector,
        BrokerSettings {
            workers: 32,
            maximum_packet_rate: 1.5 * 262144.,
            optimistic_margin: margin,
            ..Default::default()
        },
    );

    println!(" .. done! `HonestBroker` running!");

    // Wait for `Ctrl + C`

    let mut signals = Signals::new(&[SIGTERM, SIGINT]).unwrap();
    signals.next().await;

    info!("`Ctrl + C` detected, shutting down..");

    // Save heartbeat data (if necessary)

    if let Some(heartbeat_path) = heartbeat_path {
        // let time = Utc::now();

        let heartbeat_path = PathBuf::from(heartbeat_path);

        // heartbeat_path.push(format!(
        //     "heartbeat-broker-{}h{}m{}s.bin",
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
