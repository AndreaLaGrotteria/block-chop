#[derive(Debug, Clone)]
pub struct ProcessorSettings {
    pub shards: usize,
    pub batch_burst_size: usize,
    pub process_channel_capacity: usize,
    pub apply_channel_capacity: usize,
    pub deposit_bucket_capacity: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        ProcessorSettings {
            shards: 4,
            batch_burst_size: 8,
            process_channel_capacity: 8192,
            apply_channel_capacity: 8192,
            deposit_bucket_capacity: 524288,
        }
    }
}
