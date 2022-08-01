use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Header {
    Broadcast = 0,
    Reduction = 1,
    Witness = 2,
    Delivery = 3,
}
