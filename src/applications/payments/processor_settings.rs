#[derive(Debug, Clone)]
pub struct ProcessorSettings {
    pub shards: usize,
    pub process_channel_capacity: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        ProcessorSettings {
            shards: 8,
            process_channel_capacity: 8192,
        }
    }
}
