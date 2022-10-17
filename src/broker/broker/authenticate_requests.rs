use crate::{
    broker::{Broker, BrokerSettings, Request},
    crypto::statements::{
        Broadcast as BroadcastStatement,
        BroadcastAuthentication as BroadcastAuthenticationStatement,
        ReductionAuthentication as ReductionAuthenticationStatement,
    },
    system::Directory,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{net::SocketAddr, sync::Arc};
use talk::crypto::primitives::sign::{PublicKey, Signature};
use tokio::sync::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};

type BurstOutlet = MpscReceiver<Vec<(SocketAddr, Request)>>;
type RequestInlet = MpscSender<(SocketAddr, Request)>;

struct BurstItem<'a> {
    source: SocketAddr,
    request: Request,
    public_key: &'a PublicKey,
}

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
        mut authenticate_outlet: BurstOutlet,
        handle_inlet: RequestInlet,
        settings: BrokerSettings,
    ) {
        loop {
            // Receive next burst of `Request`s (from `dispatch_requests`)

            let burst = if let Some(burst) = authenticate_outlet.recv().await {
                burst
            } else {
                // `Broker` has dropped, shutdown
                return;
            };

            // Remove from `burst` all `Request`s whose fields are trivially invalid
            // (i.e., unknown id or missing authentication). To each `Request` attach
            // a reference to the relevant `PublicKey`.

            let burst = burst
                .into_iter()
                .filter_map(|(source, request)| {
                    Broker::filter_fields(directory.as_ref(), source, request).ok()
                })
                .collect::<Vec<_>>();

            // Remove from `burst` all `Request`s that are incorrectly authenticated.
            // First, optimistically check if all `Request`s in `burst` are correctly
            // authenticated (this enables `Signature::batch_verify`). If not, loop
            // through each `Request` and filter out the incorrectly authenticated ones.

            let burst = if Broker::batch_authenticate(burst.iter(), &settings).is_ok() {
                burst
            } else {
                burst
                    .into_iter()
                    .filter_map(|item| Broker::filter_authentication(item).ok())
                    .collect::<Vec<_>>()
            };

            // Forward all `Request`s in `burst` to `handle_requests`

            for item in burst {
                let _ = handle_inlet.send((item.source, item.request)).await;
            }
        }
    }

    fn filter_fields<'a>(
        directory: &'a Directory,
        source: SocketAddr,
        request: Request,
    ) -> Result<BurstItem<'a>, Top<FilterError>> {
        let public_key = match &request {
            Request::Broadcast {
                entry,
                height_record,
                authentication,
                ..
            } => {
                // Fetch relevant `PublicKey` from `directory`

                let public_key = directory
                    .get_public_key(entry.id)
                    .ok_or(FilterError::IdUnknown.into_top())
                    .spot(here!())?;

                // Verify that `height_record` is justified by `authentication`

                if height_record.is_some() && authentication.is_none() {
                    return FilterError::MissingAuthentication.fail().spot(here!());
                }

                public_key
            }
            Request::Reduction { id, .. } => {
                // Fetch relevant `PublicKey` from `directory`

                let public_key = directory
                    .get_public_key(*id)
                    .ok_or(FilterError::IdUnknown.into_top())
                    .spot(here!())?;

                public_key
            }
        };

        Ok(BurstItem {
            source,
            request,
            public_key,
        })
    }

    fn batch_authenticate<'a, I>(
        burst: I,
        settings: &BrokerSettings,
    ) -> Result<(), Top<FilterError>>
    where
        I: IntoIterator<Item = &'a BurstItem<'a>>,
    {
        // Initialize buffers to `Signature::batch_verify` `BroadcastStatement`s,
        // `BroadcastAuthenticationStatement`s and `ReductionAuthenticationStatement`s

        let mut broadcast_public_keys = Vec::with_capacity(settings.authentication_burst_size);
        let mut broadcast_statements = Vec::with_capacity(settings.authentication_burst_size);
        let mut broadcast_signatures = Vec::with_capacity(settings.authentication_burst_size);

        let mut broadcast_authentication_public_keys =
            Vec::with_capacity(settings.authentication_burst_size);
        let mut broadcast_authentication_statements =
            Vec::with_capacity(settings.authentication_burst_size);
        let mut broadcast_authentication_signatures =
            Vec::with_capacity(settings.authentication_burst_size);

        let mut reduction_authentication_public_keys =
            Vec::with_capacity(settings.authentication_burst_size);
        let mut reduction_authentication_statements =
            Vec::with_capacity(settings.authentication_burst_size);
        let mut reduction_authentication_signatures =
            Vec::with_capacity(settings.authentication_burst_size);

        // Fill `Signature::batch_verify` buffers

        for item in burst {
            match &item.request {
                Request::Broadcast {
                    entry,
                    signature,
                    height_record,
                    authentication,
                    ..
                } => {
                    // `BroadcastStatement`

                    broadcast_public_keys.push(item.public_key);

                    broadcast_statements.push(BroadcastStatement {
                        sequence: &entry.sequence,
                        message: &entry.message,
                    });

                    broadcast_signatures.push(signature);

                    // `BroadcastAuthenticationStatement` (optional)

                    if let Some(height_record) = height_record {
                        broadcast_authentication_public_keys.push(item.public_key);

                        broadcast_authentication_statements
                            .push(BroadcastAuthenticationStatement { height_record });

                        // We previously checked that `authentication` is `Some`
                        broadcast_authentication_signatures.push(authentication.as_ref().unwrap());
                    }
                }
                Request::Reduction {
                    root,
                    multisignature,
                    authentication,
                    ..
                } => {
                    // `ReductionAuthenticationStatement`

                    reduction_authentication_public_keys.push(item.public_key);

                    reduction_authentication_statements.push(ReductionAuthenticationStatement {
                        root,
                        multisignature,
                    });

                    reduction_authentication_signatures.push(authentication);
                }
            }
        }

        // `Signature::batch_verify` all three types of statements.
        // Return `Ok` only if all verifications are successful.

        Signature::batch_verify(
            broadcast_public_keys,
            broadcast_statements.iter(),
            broadcast_signatures,
        )
        .pot(FilterError::InvalidSignature, here!())?;

        Signature::batch_verify(
            broadcast_authentication_public_keys,
            broadcast_authentication_statements.iter(),
            broadcast_authentication_signatures,
        )
        .pot(FilterError::InvalidAuthentication, here!())?;

        Signature::batch_verify(
            reduction_authentication_public_keys,
            reduction_authentication_statements.iter(),
            reduction_authentication_signatures,
        )
        .pot(FilterError::InvalidAuthentication, here!())?;

        Ok(())
    }

    fn filter_authentication(item: BurstItem) -> Result<BurstItem, Top<FilterError>> {
        // Individually verify the authentication of `item`

        match &item.request {
            Request::Broadcast {
                entry,
                signature,
                height_record,
                authentication,
                ..
            } => {
                // `BroadcastStatement`

                let broadcast_statement = BroadcastStatement {
                    sequence: &entry.sequence,
                    message: &entry.message,
                };

                signature
                    .verify(item.public_key, &broadcast_statement)
                    .pot(FilterError::InvalidSignature, here!())?;

                // `BroadcastAuthenticationStatement` (optional)

                if let Some(height_record) = height_record {
                    let broadcast_authentication_statement =
                        BroadcastAuthenticationStatement { height_record };

                    authentication
                        .as_ref()
                        .unwrap() // We previously checked that `authentication` is `Some`
                        .verify(item.public_key, &broadcast_authentication_statement)
                        .pot(FilterError::InvalidAuthentication, here!())?;
                }
            }
            Request::Reduction {
                root,
                multisignature,
                authentication,
                ..
            } => {
                // `ReductionAuthenticationStatement`

                let reduction_authentication_statement = ReductionAuthenticationStatement {
                    root,
                    multisignature,
                };

                authentication
                    .verify(item.public_key, &reduction_authentication_statement)
                    .pot(FilterError::InvalidAuthentication, here!())?;
            }
        }

        Ok(item)
    }
}
