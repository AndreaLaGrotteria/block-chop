use chop_chop::{client, Directory, Passepartout};
use log::info;
use std::time::Duration;
use talk::{crypto::KeyChain, link::rendezvous::Client as RendezvousClient};
use tokio::time;

const CLIENTS_PER_LOAD_CLIENT: usize = 1_000_000;
const ID_START: u64 = 1_000_000;
const ID_END: u64 = 65_000_000; // exclusive

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `LoadClient` bootstrapper.

        Required arguments:
          <rendezvous_address> (string) address of `Rendezvous` server
          <honest_broker_index> (integer) index of the honest broker to connect to
          <rate> (float) number of operations per second to submit
          <duration> (float) number of seconds to submit for
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let honest_broker_index = args.get_integer("honest_broker_index") as usize;
    let rate = args.get_float("rate") as f64;
    let duration = args.get_float("duration") as f64;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

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

    let range = (ID_START + (honest_broker_index * CLIENTS_PER_LOAD_CLIENT) as u64)
        ..(ID_START + ((honest_broker_index + 1) * CLIENTS_PER_LOAD_CLIENT) as u64);

    if range.end > ID_END {
        panic!("Range out of bounds. Check that the honest_broker_index is in in [0, 64) ..");
    }

    let total_requests = (rate * duration) as usize;

    info!("Total requests: {}", total_requests);

    let (keychains, broadcasts) =
        client::preprocess(directory, passepartout, range.clone(), total_requests);

    // Rendezvous with servers and brokers

    info!("Rendezvous-ing with servers, load brokers..");

    let rendezvous_client = RendezvousClient::new(rendezvous_address, Default::default());

    let broker_address = loop {
        if let Ok(shard) = rendezvous_client
            .get_shard(1 + honest_broker_index as u32)
            .await
        {
            let broker_address = rendezvous_client
                .get_address(shard[0].identity())
                .await
                .unwrap();

            break broker_address.to_string();
        }

        time::sleep(Duration::from_millis(500)).await;
    };

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    // Start `Client`

    time::sleep(Duration::from_secs(5)).await;

    info!("Starting load client..");

    client::load_with(
        "0.0.0.0:10000",
        &broker_address,
        range,
        rate,
        broadcasts,
        keychains,
    )
    .await;

    info!("Load client finished.");
}
