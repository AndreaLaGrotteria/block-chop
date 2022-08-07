use crate::broker::{Batch, Broker, Response, Submission};

use std::collections::HashMap;

use talk::net::DatagramSender;

impl Broker {
    pub(in crate::broker::broker) async fn setup_batch(
        pool: HashMap<u64, Submission>,
        sender: &DatagramSender<Response>,
    ) -> Batch {
        todo!()
    }
}
