use crate::{
    crypto::{statements::Delivery, Certificate},
    system::Membership,
};

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::primitives::hash::Hash;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Height {
    height: u64,
    root: Hash,
    certificate: Certificate,
}

#[derive(Doom)]
pub(crate) enum HeightError {
    #[doom(description("Certificate invalid"))]
    CertificateInvalid,
}

impl Height {
    pub fn new(height: u64, root: Hash, certificate: Certificate) -> Self {
        Height {
            height,
            root,
            certificate,
        }
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    pub fn verify(&self, membership: &Membership) -> Result<(), Top<HeightError>> {
        let statement = Delivery {
            height: self.height,
            root: self.root,
        };

        self.certificate
            .verify_plurality(membership, &statement)
            .pot(HeightError::CertificateInvalid, here!())
    }
}
