use chop_chop::{client::Client, Directory, Membership, Message, Passepartout};
use log::info;
use std::time::{Duration, Instant};
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
          <honest_broker_index> (integer) index of the honest broker to connect to
          <duration> (float) number of seconds to submit for
          <membership_path> (string) path to system `Membership` 
          <membership_size> (integer) size of system `Membership`
          <passepartout_path> (string) path to system `Passepartout`
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw

          Logging:
          --latency-path (string) path to save latency data to
        ",
    );

    let rendezvous_address = args.get_string("rendezvous_address");
    let client_id = args.get_integer("client_id") as u64;
    let honest_broker_index = args.get_integer("honest_broker_index") as usize;
    let duration = args.get_float("duration") as f64;
    let membership_path = args.get_string("membership_path");
    let membership_size = args.get_integer("membership_size") as usize;
    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

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

    let identity = directory.get_identity(client_id).unwrap();
    let keychain = passepartout.get(identity).unwrap();
    let client = Client::new(client_id, keychain, membership, "0.0.0.0:9999");
    client.add_broker(broker_address).await.unwrap();

    rendezvous_client
        .publish_card(KeyChain::random().keycard(), Some(0))
        .await
        .unwrap();

    while rendezvous_client.get_shard(0).await.is_err() {
        time::sleep(Duration::from_millis(500)).await;
    }

    time::sleep(Duration::from_secs(5)).await;

    // Start `Client`

    info!("Starting honest client..");

    let start = Instant::now();

    let mut latencies = Vec::new();
    for i in 0u64.. {
        let mut message = Message::default();
        message.bytes[0..8].copy_from_slice(i.to_be_bytes().as_slice());
        let time = Instant::now();
        info!("Broadcasting message {}", i);
        client.broadcast(message).await;
        let latency = time.elapsed().as_millis();
        info!("Message delivered! Took {} ms", latency);
        latencies.push(latency as f64);

        if start.elapsed() > Duration::from_secs_f64(duration) {
            break;
        }
    }

    info!(
        "Honest client finished. Avg: {:.0} +- {:.0} ms",
        statistical::mean(&latencies),
        statistical::standard_deviation(&latencies, None)
    );
}
