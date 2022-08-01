use crate::broadcast::Message;

pub(crate) struct Entry {
    pub id: u64,
    pub sequence: u64,
    pub message: Message,
}
