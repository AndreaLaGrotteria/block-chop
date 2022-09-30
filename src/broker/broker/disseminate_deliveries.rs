use crate::{
    broker::{batch::Batch, Broker, Response},
    crypto::Certificate,
    Entry,
};
use talk::net::DatagramSender;

impl Broker {
    pub(in crate::broker::broker) async fn disseminate_deliveries(
        batch: Batch,
        height: u64,
        certificate: Certificate,
        sender: &DatagramSender<Response>,
    ) {
        let root = batch.entries.root();

        let deliveries = batch
            .entries
            .items()
            .into_iter()
            .flat_map(|item| item.as_ref())
            .enumerate()
            .map(|(index, Entry { sequence, .. })| {
                let proof = batch.entries.prove(index);

                let response = Response::Delivery {
                    height,
                    root,
                    certificate: certificate.clone(),
                    sequence: *sequence,
                    proof,
                };

                let address = batch.submissions.get(index).unwrap().address;

                (address, response)
            });

        for (address, delivery) in deliveries {
            sender.send(address, delivery).await;
        }
    }
}
