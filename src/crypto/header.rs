use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Header {
    // Client-issued statements
    Broadcast = 0,
    Reduction = 1,
    ReductionAuthentication = 2,

    // Server-issued statements
    BatchWitness = 3,
    BatchDelivery = 4,
}
