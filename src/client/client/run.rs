use crate::{
    broadcast::{Entry, Message},
    broker::{Request, Response},
    client::Client,
    crypto::{
        records::Delivery as DeliveryRecord,
        statements::{
            Reduction as ReductionStatement,
            ReductionAuthentication as ReductionAuthenticationStatement,
        },
    },
    debug,
    heartbeat::{self, ClientEvent},
    info, warn, Membership,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    cmp,
    net::{SocketAddr, ToSocketAddrs},
    ops::RangeInclusive,
    sync::{Arc, Mutex},
};
use talk::{
    crypto::KeyChain,
    net::{DatagramDispatcher, DatagramSender},
    sync::fuse::Fuse,
};
use tokio::sync::{mpsc::Receiver as MpscReceiver, oneshot::Sender as OneshotSender};

type BroadcastOutlet = MpscReceiver<(Message, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<DeliveryRecord>;

#[derive(Doom)]
enum HandleError {
    #[doom(description("Misdirected response"))]
    MisdirectedResponse,
    #[doom(description("Invalid proof of inclusion"))]
    InvalidInclusion,
    #[doom(description("Raise rewinds the sequence range"))]
    RewindingRaise,
    #[doom(description("Invalid height record"))]
    InvalidHeightRecord,
    #[doom(description("Raise is not justified by height record"))]
    UnjustifiedRaise,
    #[doom(description("Replayed delivery record"))]
    ReplayedDelivery,
    #[doom(description("Invalid delivery record"))]
    InvalidDeliveryRecord,
}

impl Client {
    pub(in crate::client::client) async fn run<A>(
        id: u64,
        keychain: KeyChain,
        membership: Membership,
        bind: A,
        brokers: Arc<Mutex<Vec<SocketAddr>>>,
        mut broadcast_outlet: BroadcastOutlet,
    ) where
        A: 'static + Send + Sync + Clone + ToSocketAddrs,
    {
        let dispatcher = DatagramDispatcher::bind(
            bind,
            Default::default(), // TODO: Forward settings?
        )
        .unwrap(); // TODO: Determine if this error should be handled

        let (sender, mut receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        let mut sequence_range = 0..=0;
        let mut height_record = None;

        loop {
            // Wait for the next message to broadcast

            info!("Waiting for next message to broadcast..");

            let (message, delivery_inlet) = match broadcast_outlet.recv().await {
                Some(broadcast) => broadcast,
                None => return, // `Client` has dropped, shutdown
            };

            #[cfg(feature = "benchmark")]
            heartbeat::log(ClientEvent::StartingBroadcast {
                message,
                sequence: *sequence_range.start(),
            });

            info!("Broadcasting a new message.");
            debug!("Message to broadcast: {:?}", message);

            // Spawn requesting task

            info!("Spawning requesting task.");

            let fuse = Fuse::new();

            fuse.spawn(Client::submit_request(
                id,
                keychain.clone(),
                brokers.clone(),
                sender.clone(),
                *sequence_range.start(),
                message,
                height_record.clone(),
            ));

            // Handle `Response`s until `message` is delivered

            let delivery_record = loop {
                let (source, response) = receiver.receive().await;

                debug!("Received new datagram.");

                if let Ok(Some(delivery_record)) = Client::handle_response(
                    id,
                    &keychain,
                    &membership,
                    &mut sequence_range,
                    message,
                    source,
                    response,
                    sender.as_ref(),
                )
                .await
                {
                    break delivery_record;
                }
            };

            // Shift `sequence_range`, upgrade `height_record`

            sequence_range = (sequence_range.end() + 1)..=(sequence_range.end() + 1);
            height_record = Some(delivery_record.height());

            // Send `record` back the invoking `broadcast` method

            let _ = delivery_inlet.send(delivery_record);
        }
    }

    async fn handle_response(
        id: u64,
        keychain: &KeyChain,
        membership: &Membership,
        sequence_range: &mut RangeInclusive<u64>,
        message: Message,
        source: SocketAddr,
        response: Response,
        sender: &DatagramSender<Request>,
    ) -> Result<Option<DeliveryRecord>, Top<HandleError>> {
        match response {
            Response::Inclusion {
                id: rid,
                root,
                proof,
                raise,
                top_record: height_record,
            } => {
                info!("Handling inclusion.");

                // Verify that the `Response` concerns the local `Client`

                if rid != id {
                    return HandleError::MisdirectedResponse.fail().spot(here!());
                }

                #[cfg(feature = "benchmark")]
                heartbeat::log(ClientEvent::ReceivedInclusion {
                    message: message.clone(),
                    root,
                });

                // Verify that `message` is included in `root`

                let entry = Some(Entry {
                    id,
                    sequence: raise,
                    message,
                });

                proof
                    .verify(root, &entry)
                    .pot(HandleError::InvalidInclusion, here!())?;

                // Verify that `raise` does not rewind `sequence_range`

                if raise < *sequence_range.start() {
                    return HandleError::RewindingRaise.fail().spot(here!());
                }

                // Verify that `raise` is justified by `height_record`

                let height = if let Some(height_record) = height_record {
                    height_record
                        .verify(&membership)
                        .pot(HandleError::InvalidHeightRecord, here!())?;

                    height_record.height()
                } else {
                    0 // No `Height` record is necessary for height 0
                };

                if raise > height {
                    return HandleError::UnjustifiedRaise.fail().spot(here!());
                }

                info!("All inclusion checks successfully completed.");

                // Extend `sequence_range`

                *sequence_range =
                    (*sequence_range.start())..=(cmp::max(*sequence_range.end(), raise));

                // Multi-sign and authenticate `Reduction` statement

                #[cfg(feature = "benchmark")]
                heartbeat::log(ClientEvent::SigningReduction {
                    message: message.clone(),
                    root,
                    raise,
                });

                let reduction_statement = ReductionStatement { root: &root };
                let multisignature = keychain.multisign(&reduction_statement).unwrap();

                let authentication_statement = ReductionAuthenticationStatement {
                    root: &root,
                    multisignature: &multisignature,
                };

                let authentication = keychain.sign(&authentication_statement).unwrap();

                // Send `Reduction` back to `source`

                info!("Sending reduction.");

                let request = Request::Reduction {
                    root,
                    id,
                    multisignature,
                    authentication,
                };

                #[cfg(feature = "benchmark")]
                heartbeat::log(ClientEvent::SendingReduction {
                    message: message.clone(),
                    root,
                });

                sender.send(source, request).await;

                Ok(None)
            }

            Response::Delivery {
                height,
                root,
                certificate,
                sequence,
                proof,
            } => {
                info!("Handling delivery. Sequence: {}", sequence);

                #[cfg(feature = "benchmark")]
                heartbeat::log(ClientEvent::ReceivedDelivery { sequence });

                // Verify that the delivered sequence is within the current sequence range

                if !sequence_range.contains(&sequence) {
                    warn!("Sequence outside of sequence range!");
                    return HandleError::ReplayedDelivery.fail().spot(here!());
                }

                // Build and verify `DeliveryRecord`

                let entry = Entry {
                    id,
                    sequence,
                    message,
                };

                let record = DeliveryRecord::new(height, root, certificate, entry, proof);

                if record.verify(&membership).is_err() {
                    warn!(
                        "Invalid record! {:?}",
                        record.verify(&membership).unwrap_err()
                    );
                    return HandleError::InvalidDeliveryRecord.fail().spot(here!());
                }

                #[cfg(feature = "benchmark")]
                heartbeat::log(ClientEvent::BroadcastComplete { sequence });

                info!("All delivery checks completed successfully.");

                // Message delivered!

                Ok(Some(record))
            }
        }
    }
}
