use crate::{
    broker::{Broker, Request},
    crypto::statements::{
        Broadcast as BroadcastStatement,
        BroadcastAuthentication as BroadcastAuthenticationStatement,
        ReductionAuthentication as ReductionAuthenticationStatement,
    },
    system::Directory,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{net::SocketAddr, sync::Arc};

use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

type RequestInlet = MpscSender<(SocketAddr, Request)>;
type RequestOutlet = MpscReceiver<(SocketAddr, Request)>;

#[derive(Doom)]
enum FilterError {
    #[doom(description("Id unknown"))]
    IdUnknown,
    #[doom(description("Invalid signature"))]
    InvalidSignature,
    #[doom(description("Missing authentication"))]
    MissingAuthentication,
    #[doom(description("Invalid authentication"))]
    InvalidAuthentication,
}

impl Broker {
    pub(in crate::broker::broker) async fn authenticate_requests(
        directory: Arc<Directory>,
        mut authenticate_outlet: RequestOutlet,
        handle_inlet: RequestInlet,
    ) {
        loop {
            let (source, request) = if let Some(datagram) = authenticate_outlet.recv().await {
                datagram
            } else {
                // `Broker` has dropped, shutdown
                return;
            };

            if let Ok(request) = Broker::filter_request(directory.as_ref(), request) {
                // This fails only if the `Broker` is shutting down
                let _ = handle_inlet.send((source, request)).await;
            }
        }
    }

    fn filter_request(
        directory: &Directory,
        request: Request,
    ) -> Result<Request, Top<FilterError>> {
        match &request {
            Request::Broadcast {
                entry,
                signature,
                height_record,
                authentication,
            } => {
                // Fetch `KeyCard` from `directory`

                let keycard = directory
                    .get(entry.id)
                    .ok_or(FilterError::IdUnknown.into_top())
                    .spot(here!())?;

                // Verify `signature`

                let broadcast_statement = BroadcastStatement {
                    sequence: &entry.sequence,
                    message: &entry.message,
                };

                signature
                    .verify(keycard, &broadcast_statement)
                    .pot(FilterError::InvalidSignature, here!())?;

                // Verify `authentication`

                if let Some(height_record) = height_record {
                    let authentication = authentication
                        .ok_or(FilterError::MissingAuthentication.into_top())
                        .spot(here!())?;

                    let authentication_statement =
                        BroadcastAuthenticationStatement { height_record };

                    authentication
                        .verify(keycard, &authentication_statement)
                        .pot(FilterError::InvalidAuthentication, here!())?;
                }
            }
            Request::Reduction {
                root,
                id,
                multisignature,
                authentication,
            } => {
                // Fetch `KeyCard` from `directory`

                let keycard = directory
                    .get(*id)
                    .ok_or(FilterError::IdUnknown.into_top())
                    .spot(here!())?;

                // Verify `authentication`

                let authentication_statement = ReductionAuthenticationStatement {
                    root,
                    multisignature,
                };

                authentication
                    .verify(keycard, &authentication_statement)
                    .pot(FilterError::InvalidAuthentication, here!())?;
            }
        }

        Ok(request)
    }
}
