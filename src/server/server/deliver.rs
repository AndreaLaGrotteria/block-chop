use crate::{
    broadcast::{Amendment, DeliveryShard},
    crypto::{
        statements::{
            BatchDelivery as BatchDeliveryStatement, BatchWitness as BatchWitnessStatement,
        },
        Certificate,
    },
    order::Order,
    server::{Batch, BrokerSlot, Deduplicator, Duplicate, Server, TotalityManager},
    system::Membership,
    Entry,
};
use doomstack::{here, Doom, ResultExt, Top};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use talk::crypto::{primitives::hash::Hash, Identity, KeyChain};
use tokio::sync::{mpsc::Sender as MpscSender, watch::Sender as WatchSender};

type DeliveryShardInlet = WatchSender<Option<(u64, DeliveryShard)>>;
type BurstInlet = MpscSender<Vec<Option<Entry>>>;

#[derive(Doom)]
enum ParseSubmissionError {
    #[doom(description("Failed to deserialize: {}", source))]
    #[doom(wrap(deserialize_failed))]
    DeserializeFailed { source: Box<bincode::ErrorKind> },
    #[doom(description("Witness invalid"))]
    WitnessInvalid,
}

impl Server {
    pub(in crate::server::server) async fn deliver(
        keychain: KeyChain,
        membership: Membership,
        broadcast: Arc<dyn Order>,
        broker_slots: Arc<Mutex<HashMap<Identity, BrokerSlot>>>,
        mut totality_manager: TotalityManager,
        mut deduplicator: Deduplicator,
        mut next_batch_inlet: BurstInlet,
    ) {
        let mut next_height: u64 = 1;

        let mut pending_deliveries: VecDeque<(u64, Arc<DeliveryShardInlet>)> = VecDeque::new();

        loop {
            tokio::select! {
                submission = broadcast.deliver() => {
                    // Parse `submission` to obtain broker identity and batch root

                    if let Ok((broker, root)) = Self::parse_submission(submission.as_slice(), &membership, broker_slots.as_ref()) {
                        // Retrieve broker sequence number, expected batch, and delivery shard inlet

                        let (sequence, expected_batch, delivery_shard_inlet) = {
                            let mut guard = broker_slots.lock().unwrap();

                            // Because parsing was successful, a broker slot is
                            // guaranteed to exist, and the `unwrap()` never fails
                            let broker = guard.get_mut(&broker).unwrap();

                            let sequence = broker.next_sequence;
                            let expected_batch = broker.expected_batch.take();
                            let last_delivery_shard = broker.last_delivery_shard.clone();

                            broker.next_sequence += 1;

                            (sequence, expected_batch, last_delivery_shard)
                        };

                        // If `expected_batch` contains the batch relevant to `root`, make it
                        // available to other servers. Otherwise, retrieve the appropriate batch.

                        match expected_batch {
                            Some((raw_batch, batch)) if batch.root() == root =>
                                totality_manager.hit(raw_batch, batch).await,
                            _ => totality_manager.miss(root).await,
                        };

                        // Push batch metadata to `pending_deliveries`

                        pending_deliveries.push_front((sequence, delivery_shard_inlet));
                    }
                }

                batch = totality_manager.pull() => {
                    deduplicator.push(batch).await;
                }

                (batch, duplicates) = deduplicator.pull() => {
                    // Process `batch` to obtain amended root and amendments

                    let (amended_root, amendments) =
                        Self::burst_batch(batch, duplicates, &mut next_batch_inlet).await;

                    // Assemble and post `DeliveryShard`to the broker slot's inlet

                    let multisignature = keychain.multisign(&BatchDeliveryStatement {
                        height: &next_height,
                        root: &amended_root,
                    }).unwrap();

                    let delivery_shard = DeliveryShard {
                        height: next_height,
                        amendments,
                        multisignature,
                    };

                    // Because `TotalityManager` and `Deduplicator` preserve batches, the `unwrap()` never fails
                    let (sequence, delivery_shard_inlet) = pending_deliveries.pop_back().unwrap();

                    // Unlike `send`, `send_replace` guarantees that the new value is
                    // posted to `delivery_shard_inlet` even if no outlets are subscribed
                    delivery_shard_inlet.send_replace(Some((sequence, delivery_shard)));

                    // Increment `next_height`

                    next_height += 1;
                }
            }
        }
    }

    fn parse_submission(
        submission: &[u8],
        membership: &Membership,
        broker_slots: &Mutex<HashMap<Identity, BrokerSlot>>,
    ) -> Result<(Identity, Hash), Top<ParseSubmissionError>> {
        let (broker, root, witness) =
            bincode::deserialize::<(Identity, Hash, Certificate)>(submission)
                .map_err(ParseSubmissionError::deserialize_failed)
                .map_err(ParseSubmissionError::into_top)
                .spot(here!())?;

        let sequence = broker_slots
            .lock()
            .unwrap()
            .entry(broker)
            .or_default()
            .next_sequence;

        witness
            .verify_plurality(
                &membership,
                &BatchWitnessStatement {
                    broker: &broker,
                    sequence: &sequence,
                    root: &root,
                },
            )
            .pot(ParseSubmissionError::WitnessInvalid, here!())?;

        Ok((broker, root))
    }

    async fn burst_batch(
        mut batch: Batch,
        duplicates: Vec<Duplicate>,
        next_batch_inlet: &mut BurstInlet,
    ) -> (Hash, Vec<Amendment>) {
        // Apply `Nudge` and `Drop` elements of `duplicates` to `batch`,
        // store `Ignore` elements of `duplicates` for later removal

        let mut to_ignore = Vec::new();

        for duplicate in duplicates.iter() {
            // Locate `duplicate` within `batch`

            let index = batch
                .entries
                .items()
                .binary_search_by(|entry| match entry {
                    // Before processing, all elements of `batch.entries` are `Some`.
                    // Moreover, `duplicates` is sorted by id. As a result, if `entry`
                    // is `None`, then its id matches that of a previous duplicate
                    // which, in turn, was smaller than the current `duplicate.id()`.
                    None => std::cmp::Ordering::Less,
                    Some(entry) => entry.id.cmp(&duplicate.id()),
                })
                .unwrap();

            match duplicate {
                Duplicate::Ignore { .. } => {
                    to_ignore.push(index);
                }
                Duplicate::Nudge { sequence, .. } => {
                    // TODO: Streamline the following code when `Vector` supports in-place updates
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

        // Extract `batch`'s entries, remove all `Ignore` duplicates

        let mut entries = Vec::from(batch.entries);

        for ignore in to_ignore {
            entries[ignore] = None;
        }

        // Send `entries` to `next_batch`

        let _ = next_batch_inlet.send(entries).await;

        // Return `amended_root` and all elements of `duplicates` (`Nudge`
        // and `Drop`) that can be transformed into `Amendment`s

        (
            amended_root,
            duplicates
                .iter()
                .flat_map(Duplicate::amendment)
                .collect::<Vec<_>>(),
        )
    }
}
