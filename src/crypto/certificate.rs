use bit_vec::BitVec;

use crate::system::Membership;

use doomstack::{here, Doom, ResultExt, Top};

use serde::{Deserialize, Serialize};

use talk::crypto::{primitives::multi::Signature as MultiSignature, Identity, Statement};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Certificate {
    signers: BitVec,
    signature: MultiSignature,
}

#[derive(Doom)]
pub enum CertificateError {
    #[doom(description("Certificate invalid"))]
    CertificateInvalid,
    #[doom(description("Not enough signers"))]
    NotEnoughSigners,
    #[doom(description("Overlapping signers"))]
    OverlappingSigners,
}

impl Certificate {
    pub fn aggregate_raw<C>(membership: &Membership, components: C) -> Self
    where
        C: IntoIterator<Item = (Identity, MultiSignature)>,
    {
        let mut components = components.into_iter().collect::<Vec<_>>();
        components.sort_by_key(|component| component.0);

        let mut signers = BitVec::from_elem(membership.servers().len(), false);
        let mut signer_identities = components.iter().map(|component| component.0).peekable();

        // Both `membership.members()` and `signer_identities` are sorted. In order to determine which
        // elements of `signers` to set to `true`, loop through all elements of `membership.members()`:
        // for every `member`, if `member` is the next element of `signer_identities`, then set the
        // corresponding element of `signers` to `true`, and move `signer_identities` on.
        for (index, member) in membership.servers().keys().enumerate() {
            if signer_identities.peek() == Some(&member) {
                signers.set(index, true);
                signer_identities.next();
            }
        }

        if signer_identities.next().is_some() {
            panic!("Called `Certificate::aggregate` with a foreign component");
        }

        let signatures = components.into_iter().map(|component| component.1);

        let signature = MultiSignature::aggregate(signatures)
            .expect("Called `Certificate::aggregate` with an incorrect multi-signature");

        Certificate { signers, signature }
    }

    pub fn aggregate_plurality<C>(membership: &Membership, components: C) -> Self
    where
        C: IntoIterator<Item = (Identity, MultiSignature)>,
    {
        let certificate = Self::aggregate_raw(membership, components);

        #[cfg(debug_assertions)]
        {
            if certificate.power() < membership.plurality() {
                panic!("Called `Certificate::aggregate` with an insufficient number of signers for a plurality");
            }
        }

        certificate
    }

    pub fn aggregate_quorum<C>(membership: &Membership, components: C) -> Self
    where
        C: IntoIterator<Item = (Identity, MultiSignature)>,
    {
        let certificate = Self::aggregate_raw(membership, components);

        #[cfg(debug_assertions)]
        {
            if certificate.power() < membership.quorum() {
                panic!("Called `Certificate::aggregate` with an insufficient number of signers for a quorum");
            }
        }

        certificate
    }

    pub fn power(&self) -> usize {
        self.signers.iter().filter(|mask| *mask).count()
    }

    pub fn verify_raw<S>(
        &self,
        membership: &Membership,
        message: &S,
    ) -> Result<(), Top<CertificateError>>
    where
        S: Statement,
    {
        self.signature
            .verify(
                membership
                    .servers()
                    .values()
                    .enumerate()
                    .filter_map(|(index, card)| {
                        if self.signers[index] {
                            Some(card)
                        } else {
                            None
                        }
                    }),
                message,
            )
            .pot(CertificateError::CertificateInvalid, here!())
    }

    pub fn verify_threshold<S>(
        &self,
        membership: &Membership,
        message: &S,
        threshold: usize,
    ) -> Result<(), Top<CertificateError>>
    where
        S: Statement,
    {
        if self.power() >= threshold {
            self.verify_raw(membership, message)
        } else {
            CertificateError::NotEnoughSigners.fail().spot(here!())
        }
    }

    pub fn verify_plurality<S>(
        &self,
        membership: &Membership,
        message: &S,
    ) -> Result<(), Top<CertificateError>>
    where
        S: Statement,
    {
        self.verify_threshold(membership, message, membership.plurality())
    }

    pub fn verify_quorum<S>(
        &self,
        membership: &Membership,
        message: &S,
    ) -> Result<(), Top<CertificateError>>
    where
        S: Statement,
    {
        self.verify_threshold(membership, message, membership.quorum())
    }
}
