use chop_chop::Directory;

fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `Directory` raw-ifcation utility.

        Required arguments:
          <directory_path> (string) path to the (pre-existing) `Directory` to raw
          <raw_directory_path> (string) path to store the raw `Directory`
        ",
    );

    let directory_path = args.get_string("directory_path");
    let raw_directory_path = args.get_string("raw_directory_path");

    println!("Loading `Directory`..");

    let directory = Directory::load(directory_path).unwrap();

    println!(" .. done!");

    println!("\nRaw-ing `Directory`..");

    unsafe {
        directory.save_raw(raw_directory_path);
    }

    println!("All done! Chop CHOP!");
}
