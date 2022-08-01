use std::cmp::Ordering;

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

impl PartialEq for Height {
    fn eq(&self, rho: &Self) -> bool {
        self.height == rho.height
    }
}

impl Eq for Height {}

impl PartialOrd for Height {
    fn partial_cmp(&self, rho: &Self) -> Option<Ordering> {
        Some(self.cmp(rho))
    }
}

impl Ord for Height {
    fn cmp(&self, rho: &Self) -> Ordering {
        self.height.cmp(&rho.height)
    }
}
