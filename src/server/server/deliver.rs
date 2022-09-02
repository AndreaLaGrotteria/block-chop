use crate::{
    broadcast::Amendment,
    crypto::{statements::BatchWitness, Certificate},
    server::{batch::Batch, deduplicator::Deduplicator, Server},
    system::Membership,
    total_order::Broadcast,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use talk::crypto::primitives::hash::Hash;

use tokio::sync::{mpsc::Receiver as MpscReceiver, oneshot::Sender as OneshotSender};

pub(in crate::server::server) struct AmendedDelivery {
    pub height: u64,
    pub amended_root: Hash,
    pub amendments: Vec<Amendment>,
}

type BatchOutlet = MpscReceiver<(Batch, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<AmendedDelivery>;

#[derive(Doom)]
enum ProcessError {
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Witness invalid"))]
    WitnessInvalid,
}

impl Server {
    pub(in crate::server::server) async fn deliver(
        membership: Membership,
        broadcast: Arc<dyn Broadcast>,
        mut batches_outlet: BatchOutlet,
        mut deduplicator: Deduplicator,
    ) {
        let mut height: u64 = 0;

        let mut pending_batches: HashMap<Hash, Vec<(Batch, DeliveryInlet)>> = HashMap::new();
        let mut pending_deliveries: Vec<(u64, Hash)> = Vec::new();

        loop {
            tokio::select! {
                out = batches_outlet.recv() => {
                    match out {
                        Some((batch, delivery_inlet)) => {
                            Self::add_pending_batch(&mut pending_batches, batch, delivery_inlet);
                            Self::flush_deliveries(&mut pending_batches, &mut pending_deliveries, &mut deduplicator).await;
                        }
                        None => return, // `Server` has dropped, shutdown
                    }
                }
                submission = broadcast.deliver() => {
                    if let Ok(delivery) = Self::validate(&membership, &submission) {
                        pending_deliveries.push((height, delivery));
                        Self::flush_deliveries(&mut pending_batches, &mut pending_deliveries, &mut deduplicator).await;
                    }

                    height += 1;
                }
            }
        }
    }

    fn add_pending_batch(
        pending_batches: &mut HashMap<Hash, Vec<(Batch, DeliveryInlet)>>,
        batch: Batch,
        delivery_inlet: DeliveryInlet,
    ) {
        match pending_batches.entry(batch.entries.root()) {
            Entry::Vacant(entry) => {
                entry.insert(vec![(batch, delivery_inlet)]);
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().push((batch, delivery_inlet));
            }
        }
    }

    fn remove_pending_batch(
        pending_batches: &mut HashMap<Hash, Vec<(Batch, DeliveryInlet)>>,
        batch_root: &Hash,
    ) -> Option<(Batch, DeliveryInlet)> {
        match pending_batches.entry(batch_root.clone()) {
            Entry::Vacant(_) => None,
            Entry::Occupied(mut entry) => {
                let pending = entry.get_mut();

                let result = pending.pop();

                if pending.is_empty() {
                    entry.remove();
                }

                result
            }
        }
    }

    async fn flush_deliveries(
        pending_batches: &mut HashMap<Hash, Vec<(Batch, DeliveryInlet)>>,
        pending_deliveries: &mut Vec<(u64, Hash)>,
        deduplicator: &mut Deduplicator,
    ) {
        while !pending_deliveries.is_empty() {
            let (height, batch_root) = *pending_deliveries.last().unwrap();

            match Self::remove_pending_batch(pending_batches, &batch_root) {
                Some((batch, delivery_inlet)) => {
                    pending_deliveries.pop().unwrap();

                    let (amended_root, amendments) = Self::process(batch, deduplicator);

                    let amended_delivery = AmendedDelivery {
                        height,
                        amended_root,
                        amendments,
                    };

                    let _ = delivery_inlet.send(amended_delivery);
                }
                None => break,
            }
        }
    }

    fn validate(membership: &Membership, submission: &[u8]) -> Result<Hash, Top<ProcessError>> {
        let (root, witness) = bincode::deserialize::<(Hash, Certificate)>(submission)
            .map_err(ProcessError::deserialize_failed)
            .map_err(ProcessError::into_top)
            .spot(here!())?;

        witness
            .verify_plurality(&membership, &BatchWitness::new(root))
            .pot(ProcessError::WitnessInvalid, here!())?;

        Ok(root)
    }

    fn process(_batch: Batch, _deduplicator: &mut Deduplicator) -> (Hash, Vec<Amendment>) {
        // TODO: Deduplication and applying the batch

        (_batch.root(), Vec::new())
    }
}
