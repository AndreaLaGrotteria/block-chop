use chop_chop::{Directory, Passepartout};
use log::info;
use talk::crypto::KeyCard;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `Passepartout` exporter.

        Required arguments:
          <passepartout_path> (string) path to system `Passepartout`
          <new_passepartout_path> (string) path to system `Passepartout`
          <subset-size> (integer) number of identities
          <directory_path> (path) path to system `Directory`
          --raw-directory load `Directory` as raw
          --new-directory create a new directory for the corresponding subset.
          <new_directory_path> (path default ~/new_directory.db) path to new system `Directory`
        ",
    );

    let passepartout_path = args.get_string("passepartout_path");
    let new_passepartout_path = args.get_string("new_passepartout_path");
    let subset_size = args.get_integer("subset-size") as usize;
    let directory_path = args.get_path("directory_path");
    let raw_directory = args.get_bool("raw-directory");
    let new_directory = args.get_bool("new-directory");
    let new_directory_path = args.get_path("new_directory_path");

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    info!("Loading `Passepartout`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    info!("Taking subset of size {}..", subset_size);

    let passepartout = passepartout
        .export((0u64..subset_size as u64).map(|id| directory.get_identity(id).unwrap()));

    if new_directory {
        let mut new_directory = Directory::new();
        for id in 0u64..subset_size as u64 {
            let multi_public_key = directory.get_multi_public_key(id).unwrap().clone();
            let sign_public_key = directory.get_public_key(id).unwrap().clone();
            let keycard = KeyCard::from_public_keys(sign_public_key, multi_public_key);
            new_directory.insert(id, keycard)
        }
        new_directory.save(new_directory_path).unwrap();
    }

    info!("Saving passepartout to {}..", new_passepartout_path);

    passepartout.save(new_passepartout_path).unwrap();

    info!("Passepartout exporter finished.");
}
