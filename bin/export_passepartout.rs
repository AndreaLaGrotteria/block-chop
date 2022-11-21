use chop_chop::Passepartout;
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
        ",
    );

    let passepartout_path = args.get_string("passepartout_path");
    let new_passepartout_path = args.get_string("new_passepartout_path");
    let subset_size = args.get_integer("subset-size") as usize;

    info!("Loading `Passepartout`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    info!("Taking subset of size {}..", subset_size);

    let passepartout =
        passepartout.export(passepartout.keychains.keys().cloned().take(subset_size));

    info!("Saving passepartout to {}..", new_passepartout_path);

    passepartout.save(new_passepartout_path).unwrap();

    info!("Passepartout exporter finished.");
}
