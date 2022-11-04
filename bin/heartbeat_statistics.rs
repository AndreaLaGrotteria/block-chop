fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `heartbeat` data statistics tool

        Choose one of the following:
          --shallow-broker (string) path to `Broker` / `LoadBroker` `heartbeat` data

        Options:
          --drop-front (default 0) number of seconds to drop from the beginning of the `heartbeat` data
        ",
    );

    let shallow_broker = args.get_string_result("shallow-broker").ok();

    let drop_front = args.get_float("drop-front");

    if [&shallow_broker].into_iter().flatten().count() != 1 {
        println!("Please select one of the available statistical modes.");
        return;
    }

    if let Some(path) = shallow_broker {
        modes::shallow_broker(path, drop_front);
    }
}

mod modes {
    use chop_chop::heartbeat::{BrokerEvent, Entry, Event};
    use rayon::slice::ParallelSliceMut;
    use std::{
        collections::HashMap,
        fs::File,
        io::Read,
        time::{Duration, SystemTime},
    };
    use talk::crypto::{primitives::hash::Hash, Identity};

    #[allow(dead_code)]
    struct BrokerSubmission {
        root: Hash,
        server: Identity,
        submission_started: SystemTime,
        server_connected: Option<SystemTime>,
        batch_sent: Option<SystemTime>,
        witness_shard_requested: Option<SystemTime>,
        witness_shard_received: Option<SystemTime>,
        witness_shard_verified: Option<SystemTime>,
        witness_shard_waived: Option<SystemTime>,
        witness_shard_concluded: Option<SystemTime>,
        witness_acquired: Option<SystemTime>,
        witness_sent: Option<SystemTime>,
        delivery_shard_received: Option<SystemTime>,
        submission_completed: Option<SystemTime>,
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    struct Observable {
        applicability: f64,
        average: f64,
        standard_deviation: f64,
        median: f64,
        min: f64,
        max: f64,
    }

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

        // Initialize table of `BrokerSubmission`s

        let mut submissions: HashMap<Identity, HashMap<Hash, Vec<BrokerSubmission>>> =
            HashMap::new();

        fn last_submission(
            submissions: &mut HashMap<Identity, HashMap<Hash, Vec<BrokerSubmission>>>,
            root: Hash,
            server: Identity,
        ) -> Option<&mut BrokerSubmission> {
            submissions
                .get_mut(&server)
                .and_then(|submissions| submissions.get_mut(&root))
                .and_then(|submissions| submissions.last_mut())
        }

        // Loop through `entries` to fill `submissions`

        for entry in entries {
            let time = entry.time;

            let broker_event = match entry.event {
                Event::Broker(event) => event,
                _ => unreachable!(),
            };

            match broker_event {
                BrokerEvent::SubmissionStarted { root, server } => {
                    submissions
                        .entry(server)
                        .or_default()
                        .entry(root)
                        .or_default()
                        .push(BrokerSubmission {
                            root,
                            server,
                            submission_started: time,
                            server_connected: None,
                            batch_sent: None,
                            witness_shard_requested: None,
                            witness_shard_received: None,
                            witness_shard_verified: None,
                            witness_shard_waived: None,
                            witness_shard_concluded: None,
                            witness_acquired: None,
                            witness_sent: None,
                            delivery_shard_received: None,
                            submission_completed: None,
                        });
                }
                BrokerEvent::ServerConnected { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.server_connected = Some(time);
                    }
                }
                BrokerEvent::BatchSent { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.batch_sent = Some(time);
                    }
                }
                BrokerEvent::WitnessShardRequested { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_shard_requested = Some(time);
                    }
                }
                BrokerEvent::WitnessShardReceived { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_shard_received = Some(time);
                    }
                }
                BrokerEvent::WitnessShardVerified { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_shard_verified = Some(time);
                    }
                }
                BrokerEvent::WitnessShardWaived { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_shard_waived = Some(time);
                    }
                }
                BrokerEvent::WitnessShardConcluded { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_shard_concluded = Some(time);
                    }
                }
                BrokerEvent::WitnessAcquired { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_acquired = Some(time);
                    }
                }
                BrokerEvent::WitnessSent { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.witness_sent = Some(time);
                    }
                }
                BrokerEvent::DeliveryShardReceived { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.delivery_shard_received = Some(time);
                    }
                }
                BrokerEvent::SubmissionCompleted { root, server } => {
                    if let Some(submission) = last_submission(&mut submissions, root, server) {
                        submission.submission_completed = Some(time);
                    }
                }
            }
        }

        let mut membership = submissions.keys().copied().collect::<Vec<_>>();
        membership.sort();

        // Extract observables

        fn observe<O>(
            submissions: &HashMap<Identity, HashMap<Hash, Vec<BrokerSubmission>>>,
            observable: O,
        ) -> Observable
        where
            O: Fn(&BrokerSubmission) -> Option<f64>,
        {
            let submissions = submissions
                .values()
                .map(HashMap::values)
                .flatten()
                .map(|submissions| submissions.iter())
                .flatten();

            let mut non_applicable = 0;

            let mut values = submissions
                .map(observable)
                .filter_map(|value| {
                    if value.is_none() {
                        non_applicable += 1;
                    }

                    value
                })
                .collect::<Vec<_>>();

            let applicability = (values.len() as f64) / ((values.len() + non_applicable) as f64);

            let average = statistical::mean(values.as_slice());
            let standard_deviation = statistical::standard_deviation(values.as_slice(), None);
            let median = statistical::median(values.as_slice());

            values.par_sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

            let min = *values.first().unwrap();
            let max = *values.last().unwrap();

            Observable {
                applicability,
                average,
                standard_deviation,
                median,
                min,
                max,
            }
        }

        fn conditional_delta(from: Option<SystemTime>, to: Option<SystemTime>) -> Option<f64> {
            match (from, to) {
                (Some(from), Some(to)) => Some(to.duration_since(from).unwrap().as_secs_f64()),
                _ => None,
            }
        }

        fn format_time(mut time: f64) -> String {
            if time >= 1. {
                return format!("{time:.02} s");
            }

            time *= 1000.;

            if time >= 1. {
                return format!("{time:.02} ms");
            }

            time *= 1000.;

            if time >= 1. {
                return format!("{time:.02} us");
            }

            time *= 1000.;

            format!("{time:.02} ns")
        }

        fn print_times(observable: Observable) {
            println!("Applicability: {:.03}", observable.applicability);
            println!("Average: {}", format_time(observable.average));
            println!(
                "Standard deviation: {}",
                format_time(observable.standard_deviation)
            );
            println!("Median: {}", format_time(observable.median));
            println!("Min: {}", format_time(observable.min));
            println!("Max: {}", format_time(observable.max));
        }

        // Completion

        let completion = observe(&submissions, |submission| {
            conditional_delta(
                Some(submission.submission_started),
                submission.submission_completed,
            )
        });

        println!("  --------------------- Completion times ---------------------  ");
        print_times(completion);
        println!("  ------------------------------------------------------------  ");
        println!();

        // Connection

        let connection = observe(&submissions, |submission| {
            conditional_delta(
                Some(submission.submission_started),
                submission.server_connected,
            )
        });

        println!("  --------------------- Connection times ---------------------  ");
        print_times(connection);
        println!("  ------------------------------------------------------------  ");
        println!();

        // Send

        let send = observe(&submissions, |submission| {
            conditional_delta(submission.server_connected, submission.batch_sent)
        });

        println!("  --------------------- Send times ---------------------  ");
        print_times(send);
        println!("  ------------------------------------------------------  ");
        println!();

        // Witness shard

        let witness_shard = observe(&submissions, |submission| {
            conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
        });

        println!("  --------------------- Witness shard times (all) ---------------------  ");
        print_times(witness_shard);
        println!("  ---------------------------------------------------------------------  ");
        println!();

        // Witness shard (verifier)

        let witness_shard_verifiers = observe(&submissions, |submission| {
            if submission.witness_shard_requested.is_some() {
                conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
            } else {
                None
            }
        });

        println!("  --------------------- Witness shard times (verifiers) ---------------------  ");
        print_times(witness_shard_verifiers);
        println!("  ---------------------------------------------------------------------------  ");
        println!();

        // Witness shard (verifier, per server)

        for (index, server) in membership.iter().copied().enumerate() {
            let witness_shard_verifiers = observe(&submissions, |submission| {
                if submission.witness_shard_requested.is_some() && submission.server == server {
                    conditional_delta(submission.batch_sent, submission.witness_shard_concluded)
                } else {
                    None
                }
            });

            println!("  --------------------- Witness shard times (verifiers, server {index}) ---------------------  ");
            print_times(witness_shard_verifiers);
            println!("  -------------------------------------------------------------------------------------  ");
            println!();
        }

        // Witness

        let witness = observe(&submissions, |submission| {
            conditional_delta(
                submission.witness_shard_concluded,
                submission.witness_acquired,
            )
        });

        println!("  --------------------- Witness times ---------------------  ");
        print_times(witness);
        println!("  ---------------------------------------------------------  ");
        println!();

        // Delivery shard

        let delivery_shard = observe(&submissions, |submission| {
            conditional_delta(
                submission.witness_acquired,
                submission.delivery_shard_received,
            )
        });

        println!("  --------------------- Delivery shard times ---------------------  ");
        print_times(delivery_shard);
        println!("  ----------------------------------------------------------------  ");
        println!();
    }
}
