use crate::broadcast::Message;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub id: u64,
    pub sequence: u64,
    pub message: Message,
}
