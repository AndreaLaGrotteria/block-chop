use chop_chop::{
    applications::{create_message, Application},
    client::Client,
    heartbeat, Directory, Membership, Passepartout,
};
// use chrono::{Timelike, Utc};
use futures::StreamExt;
use log::info;
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook_tokio::Signals;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant},
};
use talk::{crypto::KeyChain, link::rendezvous::Client as RendezvousClient};
use tokio::time;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `LoadClient` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <client_id> (integer) id of the client
          <broker_address> (string) address of honest `Broker`
          <duration> (float) number of seconds to submit for
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw

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
    let client_id = args.get_integer("client_id") as u64;
    let broker_address = args.get_string("broker_address");
    let duration = args.get_float("duration") as f64;
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");
    let heartbeat_path = args.get_string_result("heartbeat-path").ok();

    let random = args.get_bool("random");
    let payments = args.get_bool("payments");
    let auction = args.get_bool("auction");
    let pixel_war = args.get_bool("pixel_war");

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

    info!("Client id: {}", client_id);

    info!("Loading `Membership`..");

    let membership = Membership::load_exact(membership_path, membership_size).unwrap();

    info!("Loading `Passepartout`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    // Load `Directory`

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers, load brokers..");

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    let identity = directory.get_identity(client_id).unwrap();
    let keychain = passepartout.get(identity).unwrap();
    let client = Client::new(
        client_id,
        keychain,
        membership,
        format!("0.0.0.0:{}", 11000 + client_id),
    );
    client.add_broker(broker_address).await.unwrap();

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    time::sleep(Duration::from_secs(3)).await;

    // Start `Client`

    info!("Starting honest client..");

    let application = if random {
        Application::Random
    } else if auction {
        Application::Auction
    } else if payments {
        Application::Payments
    } else if pixel_war {
        Application::PixelWar
    } else {
        unreachable!()
    };

    let create = create_message(application);

    let start = Instant::now();

    let mut latencies = Vec::new();
    for i in 0u64.. {
        let message = create(client_id, directory.capacity() as u64);

        let time = Instant::now();
        info!("Broadcasting message {}. Contents: {:?}", i, message);
        client.broadcast(message).await;
        let latency = time.elapsed().as_millis();
        info!("Message delivered! Took {} ms", latency);
        latencies.push(latency as f64);

        if start.elapsed() > Duration::from_secs_f64(duration) {
            break;
        }
    }

    // Wait for `Ctrl + C`

    let mut signals = Signals::new(&[SIGTERM, SIGINT]).unwrap();
    signals.next().await;

    info!("`Ctrl + C` detected, shutting down..");

    // Save heartbeat data (if necessary)

    if let Some(heartbeat_path) = heartbeat_path {
        // let time = Utc::now();

        let heartbeat_path = PathBuf::from(heartbeat_path);

        // heartbeat_path.push(format!(
        //     "heartbeat-client-{}h{}m{}s.bin",
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

    info!(
        "Honest client finished. Avg: {:.02} +- {:.02} ms",
        statistical::mean(&latencies),
        statistical::standard_deviation(&latencies, None)
    );
}
