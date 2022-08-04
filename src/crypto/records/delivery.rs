use crate::{
    broadcast::Entry,
    crypto::{records::Height, statements::BatchDelivery, Certificate},
    system::Membership,
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::Hash;

use zebra::vector::Proof;

#[derive(Clone, Serialize, Deserialize)]
pub struct Delivery {
    height: u64,
    root: Hash,
    certificate: Certificate,
    entry: Entry,
    proof: Proof,
}

#[derive(Doom)]
pub enum DeliveryError {
    #[doom(description("Certificate invalid"))]
    CertificateInvalid,
    #[doom(description("Inclusion proof invalud"))]
    ProofInvalid,
}

impl Delivery {
    pub(crate) fn new(
        height: u64,
        root: Hash,
        certificate: Certificate,
        entry: Entry,
        proof: Proof,
    ) -> Self {
        Delivery {
            height,
            root,
            certificate,
            entry,
            proof,
        }
    }

    pub fn entry(&self) -> &Entry {
        &self.entry
    }

    pub(crate) fn height(&self) -> Height {
        Height::new(self.height, self.root, self.certificate.clone())
    }

    pub fn verify(&self, membership: &Membership) -> Result<(), Top<DeliveryError>> {
        let statement = BatchDelivery {
            height: &self.height,
            root: &self.root,
        };

        self.certificate
            .verify_plurality(membership, &statement)
            .pot(DeliveryError::CertificateInvalid, here!())?;

        self.proof
            .verify(self.root, &self.entry)
            .pot(DeliveryError::ProofInvalid, here!())?;

        Ok(())
    }
}
