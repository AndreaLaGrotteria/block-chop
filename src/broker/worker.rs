use std::sync::Arc;
use talk::net::SessionConnector;

pub(in crate::broker) struct Worker {
    pub connector: Arc<SessionConnector>,
    pub next_sequence: u64,
}
