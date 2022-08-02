use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Header {
    Broadcast = 0,
    Reduction = 1,
    BatchWitness = 2,
    BatchDelivery = 3,
}
