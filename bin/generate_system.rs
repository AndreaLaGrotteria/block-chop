use chop_chop::{Directory, Membership, Passepartout};
use rayon::{iter::ParallelIterator, prelude::IntoParallelIterator};
use std::{
    io::{self, prelude::*},
    iter,
    sync::atomic::{AtomicUsize, Ordering},
};
use talk::crypto::KeyChain;

fn main() {
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

    println!("Initializing `Passepartout`..");

    let mut passepartout = Passepartout::new();

    println!("\nGenerating `Membership`..");

    let membership = Membership::new(iter::repeat_with(KeyChain::random).take(servers).map(
        |keychain| {
            let keycard = keychain.keycard();
            passepartout.insert(keycard.identity(), keychain);

            keycard
        },
    ));

    println!(" .. done!");

    println!("\nGenerating keypairs..");

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

    println!("\r .. done!                             ");

    println!("\nBuilding `Passepartout` and `Directory`..");

    let mut directory = Directory::new();

    for (index, (keychain, keycard)) in keypairs.into_iter().enumerate() {
        if index % 1000 == 0 {
            print!("\r  -> Inserting id {}", (index as u64));
            io::stdout().flush().unwrap();
        }

        passepartout.insert(keycard.identity(), keychain);
        directory.insert(index as u64, keycard);
    }

    println!("\r .. done!                    ");

    println!("\nSaving `Passepartout`, `Membership` and `Directory`..");

    passepartout.save(passepartout_path).unwrap();
    membership.save(membership_path).unwrap();
    directory.save(directory_path).unwrap();

    println!("All done! Chop CHOP!");
}
