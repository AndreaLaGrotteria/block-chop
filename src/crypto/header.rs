use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Header {
    // Client-issued statements
    Broadcast = 0,
    Reduction = 1,
    BroadcastAuthentication = 2,
    ReductionAuthentication = 3,

    // Server-issued statements
    BatchWitness = 4,
    BatchDelivery = 5,
}
