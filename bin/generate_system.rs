use chop_chop::{Directory, Membership, Passepartout};

use std::iter;

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

    let passepartout = Passepartout::open(passepartout_path).unwrap();

    println!("Generating `Membership`..");

    let membership = Membership::new(iter::repeat_with(KeyChain::random).take(servers).map(
        |keychain| {
            let keycard = keychain.keycard();
            passepartout.insert(keycard.identity(), keychain).unwrap();

            keycard
        },
    ));

    println!("Generating `Directory`..");

    let mut directory = Directory::new();

    for index in 0..clients {
        let keychain = KeyChain::random();
        let keycard = keychain.keycard();

        passepartout.insert(keycard.identity(), keychain).unwrap();
        directory.insert(index as u64, keycard);
    }

    println!("Saving `Membership` and `Directory`..");

    membership.save(membership_path).unwrap();
    directory.save(directory_path).unwrap();

    println!("All done! Chop CHOP!");
}
