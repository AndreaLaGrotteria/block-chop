use chop_chop::{heartbeat, LoadBroker, LoadBrokerSettings, Membership};
use chrono::{Timelike, Utc};
use futures::stream::StreamExt;
use log::info;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::{
    cmp,
    fs::File,
    io::{BufWriter, Read, Write},
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
    net::{PlexConnector, PlexConnectorSettings},
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
          --margin (default 0) number of extra servers to request immediate witnessing
          --gc_exclude (default 0) number of servers to exclude for garbage collection
          
        Heartbeat:
          --heartbeat-path (string) path to save `heartbeat` data
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
    let heartbeat_path = args.get_string_result("heartbeat-path").ok();
    let margin = args.get_integer("margin") as usize;
    let gc_exclude = args.get_integer("gc_exclude") as usize;

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

    let plex_connector_settings = PlexConnectorSettings {
        connections_per_remote: 32,
        ..Default::default()
    };

    let broker_connector = PlexConnector::new(broker_connector, plex_connector_settings);

    // Load batches

    info!("Loading batches..");

    let flows = (flow_range_start..flow_range_end)
        .map(|flow_index| {
            let flows_path = flows_path.clone();

            (0..batches_per_flow)
                .map(move |batch_index| {
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
                .collect::<Vec<_>>()
        })
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
        flows,
        LoadBrokerSettings {
            rate,
            workers: cmp::max((rate * 30.) as u16, 1),
            optimistic_margin: margin,
            garbage_collect_exclude: gc_exclude,
            ..Default::default()
        },
    )
    .await;

    info!(" .. done! `LoadBroker` running!");

    // Wait for `Ctrl + C`

    let mut signals = Signals::new(&[SIGTERM, SIGINT]).unwrap();
    signals.next().await;

    info!("`Ctrl + C` detected, shutting down..");

    // Save heartbeat data (if necessary)

    if let Some(heartbeat_path) = heartbeat_path {
        // let time = Utc::now();

        let mut heartbeat_path = PathBuf::from(heartbeat_path);

        // heartbeat_path.push(format!(
        //     "heartbeat-loadbroker-{}Bs-{}h{}m{}s.bin",
        //     rate as usize,
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
