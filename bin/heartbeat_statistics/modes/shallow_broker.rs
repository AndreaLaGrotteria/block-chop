use crate::{BrokerSubmission, Observable};
use chop_chop::heartbeat::Entry;
use rayon::slice::ParallelSliceMut;
use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    time::{Duration, SystemTime},
};

pub fn shallow_broker(path: String, drop_front: f32) {
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

    // Parse `BrokerSubmission`s

    let submissions = BrokerSubmission::parse(entries.iter());

    // Derive membership

    let membership = submissions
        .keys()
        .copied()
        .map(|(_, server)| server)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    // Extract observables

    fn conditional_delta(from: Option<SystemTime>, to: Option<SystemTime>) -> Option<f64> {
        match (from, to) {
            (Some(from), Some(to)) => Some(to.duration_since(from).unwrap().as_secs_f64()),
            _ => None,
        }
    }

    // Completion

    let completion = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(
            Some(submission.submission_started),
            submission.submission_completed,
        )
    });

    println!("Completion times: {completion:#?}");

    // Connection

    let connection = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(
            Some(submission.submission_started),
            submission.server_connected,
        )
    });

    println!("Connection times: {connection:#?}");

    // Send

    let send = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(submission.server_connected, submission.batch_sent)
    });

    println!("Send times: {send:#?}");

    // Witness shard

    let witness_shard = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
    });

    println!("Witness shard times (all): {witness_shard:#?}");

    // Witness shard (verifier)

    let witness_shard_verifiers =
        Observable::from_samples(submissions.values().flatten(), |submission| {
            if submission.witness_shard_requested.is_some() {
                conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
            } else {
                None
            }
        });

    println!("Witness shard times (verifiers): {witness_shard_verifiers:#?}");

    // Witness shard (verifier, per server)

    for (index, server) in membership.iter().copied().enumerate() {
        let witness_shard_verifiers =
            Observable::from_samples(submissions.values().flatten(), |submission| {
                if submission.witness_shard_requested.is_some() && submission.server == server {
                    conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
                } else {
                    None
                }
            });

        println!("Witness shard times (verifiers, server {index}): {witness_shard_verifiers:#?}");
    }

    // Witness

    let witness = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(
            submission.witness_shard_concluded,
            submission.witness_acquired,
        )
    });

    println!("Witness times: {witness:#?}");

    // Delivery shard

    let delivery_shard = Observable::from_samples(submissions.values().flatten(), |submission| {
        conditional_delta(
            submission.witness_acquired,
            submission.delivery_shard_received,
        )
    });

    println!("Delivery shard times: {delivery_shard:#?}");
}
