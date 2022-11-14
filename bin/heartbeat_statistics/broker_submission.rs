use chop_chop::heartbeat::{BrokerEvent, Entry, Event};
use std::{collections::HashMap, time::SystemTime};
use talk::crypto::{primitives::hash::Hash, Identity};

#[allow(dead_code)]
pub(crate) struct BrokerSubmission {
    pub root: Hash,
    pub server: Identity,
    pub submission_started: SystemTime,
    pub server_connected: Option<SystemTime>,
    pub batch_sent: Option<SystemTime>,
    pub witness_shard_requested: Option<SystemTime>,
    pub witness_shard_received: Option<SystemTime>,
    pub witness_shard_verified: Option<SystemTime>,
    pub witness_shard_waived: Option<SystemTime>,
    pub witness_shard_concluded: Option<SystemTime>,
    pub witness_acquired: Option<SystemTime>,
    pub witness_sent: Option<SystemTime>,
    pub delivery_shard_received: Option<SystemTime>,
    pub submission_completed: Option<SystemTime>,
}

impl BrokerSubmission {
    pub fn parse<'e, E>(entries: E) -> HashMap<(Hash, Identity), Vec<BrokerSubmission>>
    where
        E: IntoIterator<Item = &'e Entry>,
    {
        let mut submissions: HashMap<(Hash, Identity), Vec<BrokerSubmission>> = HashMap::new();

        fn last_submission(
            submissions: &mut HashMap<(Hash, Identity), Vec<BrokerSubmission>>,
            root: Hash,
            server: Identity,
        ) -> Option<&mut BrokerSubmission> {
            submissions
                .get_mut(&(root, server))
                .and_then(|submissions| submissions.last_mut())
        }

        for entry in entries.into_iter().cloned() {
            let time = entry.time;

            let broker_event = match entry.event {
                Event::Broker(event) => event,
                _ => {
                    continue;
                }
            };

            match broker_event {
                BrokerEvent::SubmissionStarted { root, server } => {
                    submissions
                        .entry((root, server))
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
                _ => (),
            }
        }

        submissions
    }
}
