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
    Membership,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{cmp, net::SocketAddr, ops::RangeInclusive};

use talk::{crypto::KeyChain, net::DatagramSender};

#[derive(Doom)]
pub(in crate::client::client) enum HandleError {
    #[doom(description("Failed to deserialize response: {:?}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: bincode::Error },
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
    pub(in crate::client::client) async fn handle(
        id: u64,
        keychain: &KeyChain,
        membership: &Membership,
        sequence_range: &mut RangeInclusive<u64>,
        message: Message,
        source: SocketAddr,
        response: Vec<u8>,
        sender: &DatagramSender,
    ) -> Result<Option<DeliveryRecord>, Top<HandleError>> {
        let response = bincode::deserialize(response.as_slice())
            .map_err(HandleError::deserialize_failed)
            .map_err(HandleError::into_top)
            .spot(here!())?;

        match response {
            Response::Inclusion {
                id: rid,
                root,
                proof,
                raise,
                height_record,
            } => {
                // Verify that the `Response` concerns the local `Client`

                if rid != id {
                    return HandleError::MisdirectedResponse.fail().spot(here!());
                }

                // Verify that `message` is included in `root`

                let entry = Entry {
                    id,
                    sequence: raise,
                    message,
                };

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

                // Extend `sequence_range`

                *sequence_range =
                    (*sequence_range.start())..=(cmp::max(*sequence_range.end(), raise));

                // Multi-sign and authenticate `Reduction` statement

                let reduction_statement = ReductionStatement { root: &root };
                let multisignature = keychain.multisign(&reduction_statement).unwrap();

                let authentication_statement = ReductionAuthenticationStatement {
                    root: &root,
                    multisignature: &multisignature,
                };

                let authentication = keychain.sign(&authentication_statement).unwrap();

                // Send `Reduction` back to `source`

                let request = Request::Reduction {
                    root,
                    id,
                    multisignature,
                    authentication,
                };

                let request = bincode::serialize(&request).unwrap();
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
                // Verify that the delivered sequence is within the current sequence range

                if !sequence_range.contains(&sequence) {
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
                    return HandleError::InvalidDeliveryRecord.fail().spot(here!());
                }

                // Message delivered!

                Ok(Some(record))
            }
        }
    }
}
