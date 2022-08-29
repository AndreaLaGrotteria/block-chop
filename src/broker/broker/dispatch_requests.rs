use crate::broker::{Broker, BrokerSettings, Request};

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
        settings: BrokerSettings,
    ) {
        let mut burst_buffer = Vec::with_capacity(settings.authentication_burst_size);
        let mut last_flush = Instant::now();

        let mut authenticate_inlets = authenticate_inlets.iter().cycle();

        loop {
            // Receive next `Request` (with a timeout to ensure timely flushing)

            if let Some(datagram) = tokio::select! {
                datagram = receiver.receive() => Some(datagram),
                _ = time::sleep(Duration::from_millis(10)) => None
            } {
                burst_buffer.push(datagram);
            }

            if burst_buffer.len() >= settings.authentication_burst_size
                || last_flush.elapsed() >= settings.authentication_burst_timeout
            {
                // Flush `burst_buffer` into the next element of `authenticate_inlets`

                let mut burst = Vec::with_capacity(settings.authentication_burst_size); // Better performance than `mem::take`
                mem::swap(&mut burst, &mut burst_buffer);

                // This fails only if the `Broker` is shutting down
                let _ = authenticate_inlets.next().unwrap().send(burst).await;

                last_flush = Instant::now();
            }
        }
    }
}
