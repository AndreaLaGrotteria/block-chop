use chop_chop::heartbeat::{Entry, Event};
use std::{collections::HashMap, time::SystemTime};
use talk::crypto::primitives::hash::Hash;

#[allow(dead_code)]
pub(crate) struct ServerSubmission {
    root: Hash,
    batch_announced: SystemTime,
    batch_received: Option<SystemTime>,
    batch_deserialized: Option<SystemTime>,
    batch_expansion_started: Option<SystemTime>,
    batch_expansion_completed: Option<SystemTime>,
    batch_witnessed: Option<SystemTime>,
    batch_submitted: Option<SystemTime>,
    batch_ordered: Option<SystemTime>,
    batch_delivered: Option<SystemTime>,
    batch_served: Option<SystemTime>,
}

impl ServerSubmission {
    pub fn parse<'e, E>(entries: E) -> HashMap<Hash, Vec<ServerSubmission>>
    where
        E: IntoIterator<Item = &'e Entry>,
    {
        let mut submissions: HashMap<Hash, Vec<ServerSubmission>> = HashMap::new();

        fn last_submission(
            submissions: &mut HashMap<Hash, Vec<ServerSubmission>>,
            root: Hash,
        ) -> Option<&mut ServerSubmission> {
            submissions
                .get_mut(&root)
                .and_then(|submissions| submissions.last_mut())
        }

        for entry in entries.into_iter().cloned() {
            let time = entry.time;

            let server_event = match entry.event {
                Event::Server(event) => event,
                _ => {
                    continue;
                }
            };

            match server_event {
                chop_chop::heartbeat::ServerEvent::BatchAnnounced { root } => {
                    submissions.entry(root).or_default().push(ServerSubmission {
                        root,
                        batch_announced: time,
                        batch_received: None,
                        batch_deserialized: None,
                        batch_expansion_started: None,
                        batch_expansion_completed: None,
                        batch_witnessed: None,
                        batch_submitted: None,
                        batch_ordered: None,
                        batch_delivered: None,
                        batch_served: None,
                    });
                }
                chop_chop::heartbeat::ServerEvent::BatchReceived { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_received = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchDeserialized { root, .. } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_deserialized = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchExpansionStarted { root, .. } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_expansion_started = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchExpansionCompleted { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_expansion_completed = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchWitnessed { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_witnessed = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchSubmitted { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_submitted = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchOrdered { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_ordered = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchDelivered { root, .. } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_delivered = Some(time);
                    }
                }
                chop_chop::heartbeat::ServerEvent::BatchServed { root } => {
                    if let Some(submission) = last_submission(&mut submissions, root) {
                        submission.batch_served = Some(time);
                    }
                }
                _ => (),
            }
        }

        submissions
    }
}
