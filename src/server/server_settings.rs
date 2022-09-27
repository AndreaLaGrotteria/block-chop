use std::cmp;

#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub serve_tasks: usize,
    pub batch_channel_capacity: usize,
    pub apply_channel_capacity: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            serve_tasks: cmp::max(((num_cpus::get() as f64) * 0.85) as usize, 1),
            batch_channel_capacity: 8192,
            apply_channel_capacity: 8192,
        }
    }
}
