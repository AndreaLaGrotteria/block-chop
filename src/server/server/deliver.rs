use crate::{
    broadcast::{Amendment, DeliveryShard},
    crypto::{
        statements::{BatchDelivery, BatchWitness},
        Certificate,
    },
    order::Order,
    server::{
        server::BrokerState, totality_manager::TotalityManager, Batch, Deduplicator, Duplicate,
        Server,
    },
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
        keychain: Arc<KeyChain>,
        membership: Membership,
        broadcast: Arc<dyn Order>,
        brokers: Arc<Mutex<HashMap<Identity, BrokerState>>>,
        mut totality_manager: TotalityManager,
        mut deduplicator: Deduplicator,
        mut apply_inlet: ApplyInlet,
    ) {
        let mut height: u64 = 1;

        let mut pending_deliveries: VecDeque<(
            u64,
            Arc<WatchSender<Option<(u64, DeliveryShard)>>>,
        )> = VecDeque::new();

        loop {
            tokio::select! {
                submission = broadcast.deliver() => {
                    if let Ok((broker, expected_root)) = Self::validate(&membership, &submission, &brokers) {
                        let (sequence, expected_batch, watch_sender) = {
                            let mut guard = brokers.lock().unwrap();
                            let broker = guard.get_mut(&broker).unwrap();
                            broker.next_sequence += 1;

                            (broker.next_sequence - 1, broker.expected_batch.take(), broker.last_delivery_shard.clone())
                        };

                        pending_deliveries.push_front((sequence, watch_sender));

                        match expected_batch {
                            Some((raw_batch, batch)) if batch.root() == expected_root =>
                                totality_manager.hit(raw_batch, batch).await,
                            _ => totality_manager.miss(expected_root).await,
                        };
                    }
                }

                batch = totality_manager.pull() => {
                    deduplicator.push(batch).await;
                }

                (batch, duplicates) = deduplicator.pop() => {
                    let (amended_root, amendments) =
                        Self::process(batch, duplicates, &mut apply_inlet).await;

                    let statement = BatchDelivery {
                        height: &height,
                        root: &amended_root,
                    };

                    let multisignature = keychain.multisign(&statement).unwrap();

                    let delivery_shard = DeliveryShard {
                        height,
                        amendments,
                        multisignature,
                    };

                    let (sequence, watch_sender) = pending_deliveries.pop_back().unwrap();

                    watch_sender.send_modify(|value| { *value = Some((sequence, delivery_shard)); });

                    height += 1;
                }
            }
        }
    }

    fn validate(
        membership: &Membership,
        submission: &[u8],
        brokers: &Mutex<HashMap<Identity, BrokerState>>,
    ) -> Result<(Identity, Hash), Top<ProcessError>> {
        let (broker, root, witness) =
            bincode::deserialize::<(Identity, Hash, Certificate)>(submission)
                .map_err(ProcessError::deserialize_failed)
                .map_err(ProcessError::into_top)
                .spot(here!())?;

        let sequence = brokers
            .lock()
            .unwrap()
            .entry(broker)
            .or_insert(Default::default())
            .next_sequence;

        witness
            .verify_plurality(
                &membership,
                &BatchWitness {
                    broker: &broker,
                    sequence: &sequence, // TODO: replace with real sequence
                    root: &root,
                },
            )
            .pot(ProcessError::WitnessInvalid, here!())?;

        Ok((broker, root))
    }

    async fn process(
        mut batch: Batch,
        duplicates: Vec<Duplicate>,
        apply_inlet: &mut ApplyInlet,
    ) -> (Hash, Vec<Amendment>) {
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
