use chop_chop::{Directory, Membership, Passepartout};
use log::info;
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::{
    io::{self, prelude::*},
    iter,
    sync::atomic::{AtomicUsize, Ordering},
};
use talk::crypto::KeyChain;

fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s system-generation utility.

        Required arguments:
          <servers> (integer) number of servers in the system
          <clients> (integer) number of clients in the system
          <passepartout_path> (string) path to store the new `Passepartout`
          <membership_path> (string) path to store the new `Membership`
          <directory_path> (string) path to store the new `Directory`
        ",
    );

    let servers = args.get_integer("servers") as usize;
    let clients = args.get_integer("clients") as usize;

    let passepartout_path = args.get_string("passepartout_path");
    let membership_path = args.get_string("membership_path");
    let directory_path = args.get_string("directory_path");

    info!("Initializing `Passepartout`..");

    let mut passepartout = Passepartout::new();

    info!("\nGenerating `Membership`..");

    let membership = Membership::new(iter::repeat_with(KeyChain::random).take(servers).map(
        |keychain| {
            let keycard = keychain.keycard();
            passepartout.insert(keycard.identity(), keychain);

            keycard
        },
    ));

    info!(" .. done!");

    info!("\nGenerating keypairs..");

    let counter = AtomicUsize::new(0);

    let keypairs = (0..clients)
        .into_par_iter()
        .map(|_| {
            let index = counter.fetch_add(1, Ordering::Relaxed);

            if index % 1000 == 0 {
                print!("\r  -> Generating keypair {}", (index as u64));
                io::stdout().flush().unwrap();
            }

            let keychain = KeyChain::random();
            let keycard = keychain.keycard();

            (keychain, keycard)
        })
        .collect::<Vec<_>>();

    info!("\r .. done!                             ");

    info!("\nBuilding `Passepartout` and `Directory`..");

    let mut directory = Directory::new();

    for (index, (keychain, keycard)) in keypairs.into_iter().enumerate() {
        if index % 1000 == 0 {
            print!("\r  -> Inserting id {}", (index as u64));
            io::stdout().flush().unwrap();
        }

        passepartout.insert(keycard.identity(), keychain);
        directory.insert(index as u64, keycard);
    }

    info!("\r .. done!                    ");

    info!("\nSaving `Passepartout`, `Membership` and `Directory`..");

    passepartout.save(passepartout_path).unwrap();
    membership.save(membership_path).unwrap();
    directory.save(directory_path).unwrap();

    info!("All done! Chop CHOP!");
}
