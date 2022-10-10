#[derive(Debug, Clone)]
pub struct ProcessorSettings {
    pub shards: usize,
    pub pipeline: usize,
    pub batch_burst_size: usize,
    pub deposit_bucket_capacity: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        ProcessorSettings {
            shards: 4,
            pipeline: 8192,
            batch_burst_size: 8,
            deposit_bucket_capacity: 524288,
        }
    }
}
