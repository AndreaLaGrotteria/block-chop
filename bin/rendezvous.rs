use std::{
    future,
    io::{self, Write},
};
use talk::link::rendezvous::{Server, ServerSettings};

#[tokio::main]
async fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `Rendezvous` server bootstrapper.

        Required arguments:
          <port> (integer) port to which to bind the `Rendezvous` server
          <expected_participants> (integer) total number of `Server`s, `Broker`s (honest + load), and `Client`s (honest + load) to wait for
        ",
    );

    let port = args.get_integer("port") as u16;
    let expected_participants = args.get_integer("expected_participants") as usize;

    println!("Starting `Rendezvous` server..");

    let shard_sizes = vec![expected_participants];

    let _server = Server::new(("0.0.0.0", port), ServerSettings { shard_sizes })
        .await
        .unwrap();

    println!(" .. done! `Rendezvous` server running!");

    print!("\n    [Hit Ctrl + C to stop this daemon]  ");
    io::stdout().flush().unwrap();
    future::pending::<()>().await;
}
