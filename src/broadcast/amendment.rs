use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum Amendment {
    Nudge { id: u64, sequence: u64 },
    Drop { id: u64 },
}
