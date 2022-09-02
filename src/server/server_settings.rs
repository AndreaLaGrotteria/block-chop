#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub serve_tasks: usize,
    pub batch_channel_capacity: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            serve_tasks: 27,
            batch_channel_capacity: 10_000,
        }
    }
}
