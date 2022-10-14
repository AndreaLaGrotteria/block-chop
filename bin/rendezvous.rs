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
          <expected_participants> (integer) number of `Server`s and `Broker`s to wait for
        ",
    );

    let port = args.get_integer("port") as u16;
    let expected_participants = args.get_integer("port") as usize;

    println!("Starting `Rendezvous` server..");

    let _server = Server::new(
        ("0.0.0.0", port),
        ServerSettings {
            shard_sizes: vec![expected_participants],
        },
    )
    .await
    .unwrap();

    println!(" .. done! `Rendezvous` server running!");

    print!("\n    [Hit Ctrl + C to stop this daemon]  ");
    io::stdout().flush().unwrap();
    future::pending::<()>().await;
}
