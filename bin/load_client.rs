use chop_chop::{client, Directory, Passepartout};
use log::info;
use std::time::Duration;
use talk::{crypto::KeyChain, link::rendezvous::Client as RendezvousClient};
use tokio::time;

const CLIENTS_PER_BROKER: usize = 4_000_000;

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
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let honest_broker_index = args.get_integer("honest_broker_index") as usize;
    let rate = args.get_float("rate") as f64;
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

    let range = ((honest_broker_index+1) * CLIENTS_PER_BROKER) as u64
        ..((honest_broker_index + 2) * CLIENTS_PER_BROKER) as u64;

    let (keychains, broadcasts) = client::preprocess(directory, passepartout, range.clone());

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
