use crate::{observable::Observable, utils, ServerSubmission};
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

    // Reception

    let reception = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_announced, submission.batch_received)
    });

    println!("Reception times: {reception:#?}");

    // Deserialization

    let deserialization = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_received, submission.batch_deserialized)
    });

    println!("Deserialization times: {deserialization:#?}");

    // Verification request

    let verification_request =
        Observable::from_samples(submissions.values().flatten(), |submission| {
            utils::option_delta_f64(
                submission.batch_deserialized,
                submission.batch_expansion_started,
            )
        });

    println!("Verification request times: {verification_request:#?}");

    // Expansion

    let expansion = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(
            submission.batch_expansion_started,
            submission.batch_expansion_completed,
        )
    });

    println!("Expansion times: {expansion:#?}");

    // Witnessing

    let witnessing = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(
            submission.batch_expansion_completed,
            submission.batch_witnessed,
        )
    });

    println!("Witnessing times: {witnessing:#?}");

    // Submission

    let submission = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_witnessed, submission.batch_submitted)
    });

    println!("Submission times: {submission:#?}");

    // Ordering

    let ordering = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_submitted, submission.batch_ordered)
    });

    println!("Ordering times: {ordering:#?}");

    // Delivery

    let delivery = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_ordered, submission.batch_delivered)
    });

    println!("Delivery times: {delivery:#?}");

    // Serve

    let serve = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_ordered, submission.batch_served)
    });

    println!("Serve times: {serve:#?}");
}
