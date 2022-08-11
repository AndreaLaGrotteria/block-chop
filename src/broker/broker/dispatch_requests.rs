use crate::broker::{Broker, Request};

use std::{
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};

use talk::net::DatagramReceiver;

use tokio::{sync::mpsc::Sender as MpscSender, time};

type BurstInlet = MpscSender<Vec<(SocketAddr, Request)>>;

impl Broker {
    pub(in crate::broker::broker) async fn dispatch_requests(
        mut receiver: DatagramReceiver<Request>,
        authenticate_inlets: Vec<BurstInlet>,
    ) {
        let mut robin = 0;

        let mut burst_buffer = Vec::new();
        let mut last_burst = Instant::now();

        loop {
            if let Some(datagram) = tokio::select! {
                datagram = receiver.receive() => Some(datagram),
                _ = time::sleep(Duration::from_millis(10)) => None
            } {
                burst_buffer.push(datagram);
            }

            // TODO: Add settings
            if burst_buffer.len() >= 2000 || last_burst.elapsed() >= Duration::from_millis(100) {
                // This fails only if the `Broker` is shutting down
                let _ = authenticate_inlets
                    .get(robin % authenticate_inlets.len())
                    .unwrap()
                    .send(mem::take(&mut burst_buffer))
                    .await;

                robin += 1;
                last_burst = Instant::now();
            }
        }
    }
}
