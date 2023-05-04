use chop_chop::{applications::Application, client, Directory, Passepartout};
use log::info;
use std::{future, sync::Arc, time::Duration};
use talk::{crypto::KeyChain, link::rendezvous::Client as RendezvousClient};
use tokio::time;

const CLIENTS_PER_LOAD_CLIENT: u64 = 500_000;
// const CLIENTS_PER_LOAD_CLIENT: u64 = 500;
const WORKERS: u64 = 32;
// const WORKERS: u64 = 8;
const ID_START: u64 = 1_000_000;
// const ID_START: u64 = 500;
const ID_END: u64 = 10_000_000; // exclusive
// const ID_END: u64 = 10_000; // exclusive


#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `LoadClient` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <load_client_index> (integer) index of this load client
          <broker_address> (string) address of honest `Broker`
          <rate> (float) number of operations per second to submit
          <duration> (float) number of seconds to submit for
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw

        Application messages (choose one):
          --random
          --payments
          --auction
          --pixel_war
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let load_client_index = args.get_integer("load_client_index") as u64;
    let broker_address = args.get_string("broker_address");
    let rate = args.get_float("rate") as f64;
    let duration = args.get_float("duration") as f64;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

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

    info!("Loading `Passepartout`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    // Load `Directory`

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    // `Client` preprocessing

    let total_requests = (rate * duration) as usize;

    info!("Total requests: {}", total_requests);

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

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers, load brokers..");

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    // Start `Client`

    time::sleep(Duration::from_secs(7)).await;

    info!("Starting load client..");

    let directory = Arc::new(directory);
    let passepartout = Arc::new(passepartout);

    for i in 0..WORKERS { //0..32
        let machine_offset = ID_START + load_client_index * CLIENTS_PER_LOAD_CLIENT; // 1_000 + 0 * 500
        let worker_start = machine_offset + i * CLIENTS_PER_LOAD_CLIENT / WORKERS;
        let worker_end = machine_offset + (i + 1) * CLIENTS_PER_LOAD_CLIENT / WORKERS;
        let range = worker_start..worker_end;

        if range.end > ID_END {
            panic!("Range out of bounds. Check that the load_client_index is in in [0, 64) ..");
        }

        {
            let directory = directory.clone();
            let passepartout = passepartout.clone();
            let broker_address = broker_address.clone();
            let application = application.clone();

            tokio::spawn(async move {
                client::load(
                    directory,
                    passepartout,
                    format!("0.0.0.0:{}", 10000 + i),
                    broker_address,
                    range,
                    rate / (WORKERS as f64),
                    total_requests / WORKERS as usize,
                    application,
                )
                .await;
            });
        }
    }

    info!("Load client finished.");

    future::pending::<()>().await;
}
