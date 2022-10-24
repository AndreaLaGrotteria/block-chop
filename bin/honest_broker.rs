use chop_chop::{Broker, BrokerSettings, Directory, Membership};
use log::info;
use std::{
    future,
    io::{self, Write},
    time::Duration,
};
use talk::{
    crypto::KeyChain,
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
        Welcome to `chop-chop`'s `Broker` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <client_port> (integer) `Broker` port to which clients connect
          <honest_broker_index> (integer) index of this honest broker (`0..#honest_brokers`)
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let port = args.get_integer("client_port") as u16;
    let honest_broker_index = args.get_integer("honest_broker_index") as usize;
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

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
    let broker_connector = SessionConnector::new(broker_connector);

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers, load brokers..");

    let keychain = KeyChain::random();

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    rendezvous_client
        .advertise_port(keychain.keycard().identity(), port).await;

    rendezvous_client
        .publish_card(
            keychain.keycard(),
            Some(1 + honest_broker_index as u32),
        )
        .await
        .unwrap();

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
            ..Default::default()
        },
    );

    println!(" .. done! `LoadBroker` running!");

    // Wait indefinitely

    println!("\n    [Hit Ctrl + C to stop this daemon]  ");
    io::stdout().flush().unwrap();
    future::pending::<()>().await;
}
