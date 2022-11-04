fn main() {
    let args = lapp::parse_args(
        "
        Welcome to `chop-chop`'s `heartbeat` data statistics tool

        Choose one of the following:
          --shallow-broker (string) path to `Broker` / `LoadBroker` `heartbeat` data
        ",
    );

    let shallow_broker = args.get_string_result("shallow-broker").ok();

    if [&shallow_broker].into_iter().flatten().count() != 1 {
        println!("Please select one of the available statistical modes.");
        return;
    }

    if let Some(path) = shallow_broker {
        modes::shallow_broker(path);
    }
}

mod modes {
    use chop_chop::heartbeat::{BrokerEvent, Entry, Event};
    use rayon::slice::ParallelSliceMut;
    use std::{collections::HashMap, fs::File, io::Read, time::SystemTime};
    use talk::crypto::{primitives::hash::Hash, Identity};

    struct BrokerSubmission {
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

    pub fn shallow_broker(path: String) {
        // Load, deserialize and sort `Entry`ies by time

        let mut file = File::open(path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();

        let mut entries = bincode::deserialize::<Vec<Entry>>(buffer.as_slice()).unwrap();
        entries.par_sort_unstable_by_key(|entry| entry.time);

        // Initialize table of `BrokerSubmission`s

        let mut submissions: HashMap<Identity, HashMap<Hash, Vec<BrokerSubmission>>> =
            HashMap::new();

        fn last_submission(
            submissions: &mut HashMap<Identity, HashMap<Hash, Vec<BrokerSubmission>>>,
            root: Hash,
            server: Identity,
        ) -> &mut BrokerSubmission {
            submissions
                .get_mut(&server)
                .unwrap()
                .get_mut(&root)
                .unwrap()
                .last_mut()
                .unwrap()
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
                    last_submission(&mut submissions, root, server).server_connected = Some(time);
                }
                BrokerEvent::BatchSent { root, server } => {
                    last_submission(&mut submissions, root, server).batch_sent = Some(time);
                }
                BrokerEvent::WitnessShardRequested { root, server } => {
                    last_submission(&mut submissions, root, server).witness_shard_requested =
                        Some(time);
                }
                BrokerEvent::WitnessShardReceived { root, server } => {
                    last_submission(&mut submissions, root, server).witness_shard_received =
                        Some(time);
                }
                BrokerEvent::WitnessShardVerified { root, server } => {
                    last_submission(&mut submissions, root, server).witness_shard_verified =
                        Some(time);
                }
                BrokerEvent::WitnessShardWaived { root, server } => {
                    last_submission(&mut submissions, root, server).witness_shard_waived =
                        Some(time);
                }
                BrokerEvent::WitnessShardConcluded { root, server } => {
                    last_submission(&mut submissions, root, server).witness_shard_concluded =
                        Some(time);
                }
                BrokerEvent::WitnessAcquired { root, server } => {
                    last_submission(&mut submissions, root, server).witness_acquired = Some(time);
                }
                BrokerEvent::WitnessSent { root, server } => {
                    last_submission(&mut submissions, root, server).witness_sent = Some(time);
                }
                BrokerEvent::DeliveryShardReceived { root, server } => {
                    last_submission(&mut submissions, root, server).delivery_shard_received =
                        Some(time);
                }
                BrokerEvent::SubmissionCompleted { root, server } => {
                    last_submission(&mut submissions, root, server).submission_completed =
                        Some(time);
                }
            }
        }

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

        let completion = observe(&submissions, |submission| {
            conditional_delta(
                Some(submission.submission_started),
                submission.submission_completed,
            )
        });

        println!("Completion times (s): {completion:?}");
    }
}
