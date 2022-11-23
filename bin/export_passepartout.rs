use chop_chop::{Passepartout, Directory};
use log::info;

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
          <directory_path> (string) path to system `Directory`
          --raw-directory load `Directory` as raw
        ",
    );

    let passepartout_path = args.get_string("passepartout_path");
    let new_passepartout_path = args.get_string("new_passepartout_path");
    let subset_size = args.get_integer("subset-size") as usize;
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");

    info!("Loading `Directory`..");

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    info!("Loading `Passepartout`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    info!("Taking subset of size {}..", subset_size);

    let passepartout =
        passepartout.export((0u64..subset_size as u64).map(|id| directory.get_identity(id).unwrap()));

    info!("Saving passepartout to {}..", new_passepartout_path);

    passepartout.save(new_passepartout_path).unwrap();

    info!("Passepartout exporter finished.");
}
