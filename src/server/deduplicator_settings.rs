use std::time::Duration;

#[derive(Debug, Clone)]
pub(in crate::server) struct DeduplicatorSettings {
    pub tasks: usize,
    pub pipeline: usize,

    pub run_duration: Duration,

    pub burst_size: usize,
    pub burst_timeout: Duration,
    pub burst_interval: Duration,
}

impl Default for DeduplicatorSettings {
    fn default() -> Self {
        DeduplicatorSettings {
            tasks: 4,
            pipeline: 1024,
            run_duration: Duration::from_secs(3600),
            burst_size: 16,
            burst_timeout: Duration::from_millis(100),
            burst_interval: Duration::from_millis(10),
        }
    }
}
