use crate::{
    broadcast::Amendment,
    crypto::{statements::BatchWitness, Certificate},
    order::Order,
    server::{Batch, Deduplicator, Duplicate, Server},
    system::Membership,
    Entry,
};

use doomstack::{here, Doom, ResultExt, Top};

use std::{
    collections::{hash_map::Entry as HashMapEntry, HashMap, VecDeque},
    sync::Arc,
};

use talk::crypto::primitives::hash::Hash;

use tokio::sync::{
    mpsc::{Receiver as MpscReceiver, Sender as MpscSender},
    oneshot::Sender as OneshotSender,
};

pub(in crate::server::server) struct AmendedDelivery {
    pub height: u64,
    pub amended_root: Hash,
    pub amendments: Vec<Amendment>,
}

type BatchOutlet = MpscReceiver<(Batch, DeliveryInlet)>;
type DeliveryInlet = OneshotSender<AmendedDelivery>;
type ApplyInlet = MpscSender<Vec<Option<Entry>>>;

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
        broadcast: Arc<dyn Order>,
        mut batches_outlet: BatchOutlet,
        mut deduplicator: Deduplicator,
        mut apply_inlet: ApplyInlet,
    ) {
        let mut height: u64 = 1;

        let mut pending_batches: HashMap<Hash, VecDeque<(Batch, DeliveryInlet)>> = HashMap::new();
        let mut pending_deliveries: Vec<(u64, Hash)> = Vec::new();

        loop {
            tokio::select! {
                out = batches_outlet.recv() => {
                    match out {
                        Some((batch, delivery_inlet)) => {
                            Self::add_pending_batch(&mut pending_batches, batch, delivery_inlet);
                            Self::flush_deliveries(&mut pending_batches, &mut pending_deliveries, &mut deduplicator, &mut apply_inlet).await;
                        }
                        None => return, // `Server` has dropped, shutdown
                    }
                }
                submission = broadcast.deliver() => {
                    if let Ok(delivery) = Self::validate(&membership, &submission) {
                        pending_deliveries.push((height, delivery));
                        Self::flush_deliveries(&mut pending_batches, &mut pending_deliveries, &mut deduplicator, &mut apply_inlet).await;
                    }

                    height += 1;
                }
            }
        }
    }

    fn add_pending_batch(
        pending_batches: &mut HashMap<Hash, VecDeque<(Batch, DeliveryInlet)>>,
        batch: Batch,
        delivery_inlet: DeliveryInlet,
    ) {
        match pending_batches.entry(batch.entries.root()) {
            HashMapEntry::Vacant(entry) => {
                entry.insert(vec![(batch, delivery_inlet)].into());
            }
            HashMapEntry::Occupied(mut entry) => {
                entry.get_mut().push_front((batch, delivery_inlet));
            }
        }
    }

    fn remove_pending_batch(
        pending_batches: &mut HashMap<Hash, VecDeque<(Batch, DeliveryInlet)>>,
        batch_root: &Hash,
    ) -> Option<(Batch, DeliveryInlet)> {
        match pending_batches.entry(batch_root.clone()) {
            HashMapEntry::Vacant(_) => None,
            HashMapEntry::Occupied(mut entry) => {
                let pending = entry.get_mut();

                let result = pending.pop_back();

                if pending.is_empty() {
                    entry.remove();
                }

                result
            }
        }
    }

    async fn flush_deliveries(
        pending_batches: &mut HashMap<Hash, VecDeque<(Batch, DeliveryInlet)>>,
        pending_deliveries: &mut Vec<(u64, Hash)>,
        deduplicator: &mut Deduplicator,
        apply_inlet: &mut ApplyInlet,
    ) {
        while !pending_deliveries.is_empty() {
            let (height, batch_root) = *pending_deliveries.last().unwrap();

            match Self::remove_pending_batch(pending_batches, &batch_root) {
                Some((batch, delivery_inlet)) => {
                    pending_deliveries.pop().unwrap();

                    let (amended_root, amendments) =
                        Self::process(batch, deduplicator, apply_inlet).await;

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
            .verify_plurality(&membership, &BatchWitness { root: &root })
            .pot(ProcessError::WitnessInvalid, here!())?;

        Ok(root)
    }

    async fn process(
        batch: Batch,
        deduplicator: &mut Deduplicator,
        apply_inlet: &mut ApplyInlet,
    ) -> (Hash, Vec<Amendment>) {
        deduplicator.push(batch).await;

        let (mut batch, duplicates) = deduplicator.pop().await; // TODO: Deduplication

        let mut to_ignore = Vec::new();

        for duplicate in duplicates.iter() {
            let index = batch
                .entries
                .items()
                .binary_search_by(|entry| match entry {
                    None => std::cmp::Ordering::Less,
                    Some(entry) => entry.id.cmp(&duplicate.id()),
                })
                .unwrap();

            match duplicate {
                Duplicate::Ignore { .. } => {
                    to_ignore.push(index);
                }
                Duplicate::Nudge { sequence, .. } => {
                    let mut entry = batch.entries.items()[index].clone().unwrap();
                    entry.sequence = *sequence;
                    batch.entries.set(index, Some(entry)).unwrap();
                }
                Duplicate::Drop { .. } => {
                    batch.entries.set(index, None).unwrap();
                }
            }
        }

        let amended_root = batch.root();

        let mut entries = Vec::from(batch.entries);

        for ignore in to_ignore {
            entries[ignore] = None;
        }

        let _ = apply_inlet.send(entries).await;

        (
            amended_root,
            duplicates
                .iter()
                .flat_map(Duplicate::amendment)
                .collect::<Vec<_>>(),
        )
    }
}
