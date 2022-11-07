use crate::ServerSubmission;
use chop_chop::heartbeat::Entry;
use rayon::slice::ParallelSliceMut;
use std::{fs::File, io::Read, time::Duration};

pub fn shallow_server(path: String, drop_front: f32) {
    // Load, deserialize and sort `Entry`ies by time

    let mut file = File::open(path).unwrap();
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    let mut entries = bincode::deserialize::<Vec<Entry>>(buffer.as_slice()).unwrap();
    entries.par_sort_unstable_by_key(|entry| entry.time);

    // Crop entries from front

    let heartbeat_start = entries.first().unwrap().time;

    let entries = entries
        .into_iter()
        .filter_map(|entry| {
            if entry.time.duration_since(heartbeat_start).unwrap()
                >= Duration::from_secs_f64(drop_front as f64)
            {
                Some(entry)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Parse `ServerSubmission`s

    let submissions = ServerSubmission::parse(entries.iter());
}
