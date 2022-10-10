#[derive(Debug, Clone)]
pub struct ProcessorSettings {
    pub process_channel_capacity: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        ProcessorSettings {
            process_channel_capacity: 8192,
        }
    }
}
