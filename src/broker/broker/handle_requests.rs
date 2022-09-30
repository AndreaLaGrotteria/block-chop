use crate::{
    broadcast::Entry,
    broker::{Broker, BrokerSettings, Reduction, Request, Response, Submission, Worker},
    crypto::records::Height as HeightRecord,
    debug, info,
    system::{Directory, Membership},
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{collections::HashMap, mem, net::SocketAddr, sync::Arc, time::Instant};
use talk::{
    crypto::{primitives::sign::Signature, Identity},
    net::DatagramSender,
    sync::fuse::Fuse,
};
use tokio::{
    sync::{
        broadcast,
        mpsc::{self, Receiver as MpscReceiver},
    },
    time,
};

type RequestOutlet = MpscReceiver<(SocketAddr, Request)>;

#[derive(Doom)]
enum FilterError {
    #[doom(description("Invalid height record"))]
    InvalidHeightRecord,
    #[doom(description("Unjustified height"))]
    UnjustifiedHeight,
}

impl Broker {
    pub(in crate::broker::broker) async fn handle_requests(
        membership: Arc<Membership>,
        directory: Arc<Directory>,
        mut handle_outlet: RequestOutlet,
        sender: Arc<DatagramSender<Response>>,
        mut workers: HashMap<Identity, Worker>,
        settings: BrokerSettings,
    ) {
        let (worker_recycler, mut available_workers) = mpsc::channel(workers.len());

        for identity in workers.keys().copied() {
            worker_recycler.send(identity).await.unwrap();
        }

        let mut top_record = None;
        let mut next_flush = None;
        let mut pool = HashMap::new();

        let (reduction_inlet, _) = broadcast::channel(settings.reduction_channel_capacity);

        let fuse = Fuse::new();

        loop {
            if let Some((source, request)) = tokio::select! {
                request = handle_outlet.recv() => {
                    if request.is_some() {
                        request
                    } else {
                        // `Broker` has dropped, shutdown
                        return;
                    }
                }
                _ = time::sleep(settings.pool_interval) => None,
            } {
                match request {
                    Request::Broadcast {
                        entry,
                        signature,
                        height_record,
                        ..
                    } => {
                        debug!("Handling broadcast request.");

                        if let Ok(submission) = Broker::filter_broadcast(
                            membership.as_ref(),
                            &mut top_record,
                            source,
                            entry,
                            signature,
                            height_record,
                        ) {
                            pool.insert(submission.entry.id, submission);

                            next_flush =
                                next_flush.or(Some(Instant::now() + settings.pool_timeout));
                        }
                    }
                    Request::Reduction {
                        root,
                        id,
                        multisignature,
                        ..
                    } => {
                        debug!("Forwarding reduction request.");

                        let reduction = Reduction {
                            root,
                            id,
                            multisignature,
                        };

                        let _ = reduction_inlet.send(reduction);
                    }
                }
            }

            if pool.len() >= settings.pool_capacity
                || (next_flush.is_some() && Instant::now() > next_flush.unwrap())
            {
                if let Ok(identity) = available_workers.try_recv() {
                    info!("Flushing pool into a batch ({} entries).", pool.len());

                    next_flush = None;

                    let worker = workers.get_mut(&identity).unwrap();

                    fuse.spawn(Broker::manage_batch(
                        identity,
                        worker.next_sequence,
                        membership.clone(),
                        directory.clone(),
                        mem::take(&mut pool),
                        top_record.clone(),
                        reduction_inlet.subscribe(),
                        sender.clone(),
                        worker.connector.clone(),
                        worker_recycler.clone(),
                        settings.clone(),
                    ));

                    worker.next_sequence += 1;
                }
            }
        }
    }

    fn filter_broadcast(
        membership: &Membership,
        top_record: &mut Option<HeightRecord>,
        source: SocketAddr,
        entry: Entry,
        signature: Signature,
        height_record: Option<HeightRecord>,
    ) -> Result<Submission, Top<FilterError>> {
        let height = height_record
            .as_ref()
            .map(HeightRecord::height)
            .unwrap_or(0);

        let mut top = top_record.as_ref().map(HeightRecord::height).unwrap_or(0);

        if height > top {
            height_record
                .as_ref()
                .unwrap() // `height > 0`, hence `height_record` is `Some`
                .verify(&membership)
                .pot(FilterError::InvalidHeightRecord, here!())?;

            top = height;
            *top_record = height_record;
        }

        if entry.sequence > top {
            return FilterError::UnjustifiedHeight.fail().spot(here!())?;
        }

        debug!("All broadcast checks completed successfully.");

        Ok(Submission {
            address: source,
            entry,
            signature,
        })
    }
}
