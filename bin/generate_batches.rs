use chop_chop::{
    applications::{auctions::Request, payments::Payment, pixel_war::Paint},
    Batch, Directory, Entry, Message, Passepartout,
};
use log::info;
use rand::{
    distributions::{Distribution, WeightedIndex},
    seq::SliceRandom,
};
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::{self, prelude::*},
    iter,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use talk::crypto::KeyChain;

struct Client {
    id: u64,
    keychain: KeyChain,
    state: RefCell<State>,
}

struct State {
    next_sequence: u64,
    last_broadcast: Option<usize>,
}

fn main() {
    env_logger::init();
    // Parse command-line arguments

    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s batch-generation utility.

        Required arguments:
          <passepartout_path> (string) path to the `Passepartout`
          <directory_path> (string) path to the `Directory`
          --raw-directory load `Directory` as raw
          <range_start> (integer) beginning of the id range of broadcasting clients
          <range_end> (integer) end of the id range of broadcasting clients
          <batch_size> (integer) number of messages in each batch
          <flows> (integer) number of flows
          <batches_per_flow> (integer) number of batches per flow
          <cooldown> (integer) number of batches to wait before re-broadcasting
          <reduction_probability> (float) probability that any broadcast will be reduced
          <output_path> (string) path to output

        Application messages (choose one):
          --random
          --payments
          --auction
          --pixel_war
        ",
    );

    let passepartout_path = args.get_string("passepartout_path");
    let directory_path = args.get_string("directory_path");
    let raw_directory = args.get_bool("raw-directory");
    let range_start = args.get_integer("range_start") as u64;
    let range_end = args.get_integer("range_end") as u64;
    let batch_size = args.get_integer("batch_size") as usize;
    let flows = args.get_integer("flows") as usize;
    let batches_per_flow = args.get_integer("batches_per_flow") as usize;
    let cooldown = args.get_integer("cooldown") as usize;
    let reduction_probability = args.get_float("reduction_probability") as f64;
    let output_path = PathBuf::from(args.get_string("output_path"));

    let total_batches = flows * batches_per_flow;

    let random = args.get_bool("random");
    let payments = args.get_bool("payments");
    let auction = args.get_bool("auction");
    let pixel_war = args.get_bool("pixel_war");

    let applications_selected = if random { 1 } else { 0 }
        + if payments { 1 } else { 0 }
        + if auction { 1 } else { 0 }
        + if pixel_war { 1 } else { 0 };

    if applications_selected == 0 {
        info!("Please select the underlying application message type.");
        return;
    } else if applications_selected > 1 {
        info!("Please select only one underlying application message type.");
        return;
    }

    // Load `Passepartout` and `Directory`

    info!("Loading `Passepartout` and `Directory`..");

    let passepartout = Passepartout::load(passepartout_path).unwrap();

    let directory = if raw_directory {
        unsafe { Directory::load_raw(directory_path) }
    } else {
        Directory::load(directory_path).unwrap()
    };

    let directory_size = directory.capacity() as u64;

    let create_message: fn(u64, u64) -> Message = if random {
        |_, _| -> Message { rand::random() }
    } else if payments {
        |source: u64, directory_size: u64| -> Message {
            Payment::generate(source, directory_size, 10).to_message().1
        }
    } else if auction {
        |source: u64, _: u64| -> Message {
            Request::generate(source, source / 65536, 10).to_message().1
        }
    } else if pixel_war {
        |source, _| -> Message { Paint::random(source).to_message().1 }
    } else {
        unreachable!()
    };

    info!(" .. done!");

    // Partition id range among flows

    info!("Partitioning id range in flows..");

    let mut ids = (range_start..range_end).into_iter().collect::<Vec<_>>();
    ids.shuffle(&mut rand::thread_rng());

    // Each flow runs `ceil(ids.len() / flows)` accounts
    // (TODO: replace with `div_ceil` when `int_roundings` is stabilized)
    let ids_per_flow = (ids.len() + flows - 1) / flows;

    let flows = ids
        .chunks(ids_per_flow)
        .map(|flow| flow.to_vec())
        .collect::<Vec<_>>();

    info!(" .. done!");

    // Generate flows

    info!("\nGenerating flows..");

    let batch_count = AtomicU64::new(0);

    flows
        .into_par_iter()
        .enumerate()
        .for_each(|(flow_index, flow)| {
            // Setup output folder

            let mut flow_path = output_path.clone();
            flow_path.push(format!("flow-{flow_index:02}"));

            fs::create_dir_all(flow_path.as_path()).unwrap();

            info!("\nA..");

            // Setup `Client`s

            let clients = flow
                .iter()
                .copied()
                .map(|id| {
                    let identity = directory.get_identity(id).unwrap();
                    let keychain = passepartout.get(identity).unwrap();

                    let state = State {
                        next_sequence: 0,
                        last_broadcast: None,
                    };

                    Client {
                        id,
                        keychain,
                        state: RefCell::new(state),
                    }
                })
                .collect::<Vec<_>>();

            info!("\nB..");

            // Generate `Batch`es

            for batch_index in 0..batches_per_flow {
                // Select broadcasters

                let mut broadcasters = HashMap::new();

                while broadcasters.len() < batch_size {
                    let client = clients.choose(&mut rand::thread_rng()).unwrap();

                    if broadcasters.contains_key(&client.id) {
                        continue;
                    }

                    if let Some(last_broadcast) = client.state.borrow().last_broadcast {
                        if batch_index - last_broadcast < cooldown {
                            continue;
                        }
                    }

                    broadcasters.insert(client.id, client);
                }

                info!("\nC..");

                let broadcasters = broadcasters.into_values().collect::<Vec<_>>();

                // Generate entries

                let entries = broadcasters
                    .iter()
                    .map(|broadcaster| Entry {
                        id: broadcaster.id,
                        sequence: broadcaster.state.borrow().next_sequence,
                        message: create_message(broadcaster.id, directory_size),
                    })
                    .collect::<Vec<_>>();

                // Randomize reductions

                let choices = [true, false];
                let weights = [reduction_probability, (1. - reduction_probability)];

                let distribution = WeightedIndex::new(&weights).unwrap();

                let reductions =
                    iter::repeat_with(|| choices[distribution.sample(&mut rand::thread_rng())])
                        .take(broadcasters.len())
                        .collect::<Vec<_>>();

                info!("\nD..");

                // Assemble `Batch`

                let requests = broadcasters
                    .iter()
                    .zip(entries)
                    .zip(reductions.iter().copied())
                    .map(|((broadcaster, entry), reduce)| {
                        (entry, broadcaster.keychain.clone(), reduce)
                    });

                let (root, batch) = Batch::assemble(requests);

                info!("\nE..");

                // Update `next_sequence`s in `broadcasters`

                for (broadcaster, reduced) in broadcasters.into_iter().zip(reductions) {
                    let mut state = broadcaster.state.borrow_mut();
                    state.last_broadcast = Some(batch_index);

                    state.next_sequence = if reduced {
                        batch.raise + 1
                    } else {
                        state.next_sequence + 1
                    };
                }

                info!("\nF..");

                // Save `root` and `batch`

                let mut batch_path = flow_path.clone();
                batch_path.push(format!("batch-{batch_index:05}.raw"));

                let root = root.to_bytes();
                let batch = bincode::serialize(&batch).unwrap();

                let mut file = File::create(batch_path).unwrap();
                file.write_all(root.as_slice()).unwrap();
                file.write_all(batch.as_slice()).unwrap();

                info!("\nG..");
                // Log progress

                let batch_count = batch_count.fetch_add(1, Ordering::Relaxed);

                info!("\r    Generated {batch_count} / {total_batches} batches");
                io::stdout().flush().unwrap();
            }
        });

    info!(" .. done!                                           \n");
    info!("All done! Chop CHOP!");
}
