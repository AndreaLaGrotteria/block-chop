use crate::broadcast::Message;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Entry {
    pub id: u64,
    pub sequence: u64,
    pub message: Message,
}
