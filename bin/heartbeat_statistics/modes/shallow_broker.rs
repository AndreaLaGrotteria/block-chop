use crate::{utils, BrokerSubmission, Observable};
use chop_chop::heartbeat::Entry;
use rayon::slice::ParallelSliceMut;
use std::{collections::BTreeSet, fs::File, io::Read, time::Duration};

pub fn shallow_broker(path: String, start: f32, duration: f32) {
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
            let time = entry.time.duration_since(heartbeat_start).unwrap();

            if time >= Duration::from_secs_f64(start as f64)
                && time <= Duration::from_secs_f64((start + duration) as f64)
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

    // Completion

    let completion = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(
            submission.submission_started,
            submission.submission_completed,
        )
    });

    println!("Completion times: {completion:#?}");

    // Connection

    let connection = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.submission_started, submission.server_connected)
    });

    println!("Connection times: {connection:#?}");

    // Send

    let send = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.server_connected, submission.batch_sent)
    });

    println!("Send times: {send:#?}");
    println!();

    // Witness shard

    let witness_shard = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.batch_sent, submission.witness_shard_concluded)
    });

    println!("Witness shard times (all): {witness_shard:#?}");

    // Witness shard (verifier)

    let witness_shard_verifiers =
        Observable::from_samples(submissions.values().flatten(), |submission| {
            if submission.witness_shard_requested.is_some() {
                utils::option_delta_f64(submission.batch_sent, submission.witness_shard_concluded)
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
                    utils::option_delta_f64(
                        submission.batch_sent,
                        submission.witness_shard_concluded,
                    )
                } else {
                    None
                }
            });

        println!("Witness shard times (verifiers, server {index}): {witness_shard_verifiers:#?}");
    }

    println!();

    // Witness

    let witness = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(submission.submission_started, submission.witness_acquired)
    });

    println!("Witness times: {witness:#?}");
    println!();

    // Delivery shard

    let delivery_shard = Observable::from_samples(submissions.values().flatten(), |submission| {
        utils::option_delta_f64(
            submission.witness_acquired,
            submission.delivery_shard_received,
        )
    });

    println!("Delivery shard times: {delivery_shard:#?}");

    for (index, server) in membership.iter().copied().enumerate() {
        let delivery_shard =
            Observable::from_samples(submissions.values().flatten(), |submission| {
                if submission.server == server {
                    utils::option_delta_f64(
                        submission.witness_acquired,
                        submission.delivery_shard_received,
                    )
                } else {
                    None
                }
            });

        println!("Delivery shard times (server {index}): {delivery_shard:#?}");
    }
}
