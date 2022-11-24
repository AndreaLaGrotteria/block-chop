use std::cmp;
use talk::net::PlexListenerSettings;

#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub expand_tasks: usize,
    pub batch_channel_capacity: usize,
    pub next_batch_channel_capacity: usize,
    pub broker_listener_settings: PlexListenerSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            expand_tasks: cmp::max(((num_cpus::get() as f64) * 0.85) as usize, 1),
            batch_channel_capacity: 8192,
            next_batch_channel_capacity: 8192,
            broker_listener_settings: Default::default(),
        }
    }
}
