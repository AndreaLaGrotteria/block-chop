use std::time::Duration;
use talk::net::{PlexConnectorSettings, PlexListenerSettings};

#[derive(Debug, Clone)]
pub(in crate::server) struct TotalityManagerSettings {
    pub pipeline: usize,
    pub extend_timeout: Duration,

    pub update_interval: Duration,
    pub collect_interval: Duration,
    pub wake_interval: Duration,

    pub connector_settings: PlexConnectorSettings,
    pub listener_settings: PlexListenerSettings,

    pub garbage_collect_excluded: usize,
}

impl Default for TotalityManagerSettings {
    fn default() -> Self {
        TotalityManagerSettings {
            pipeline: 8192,
            extend_timeout: Duration::from_secs(2),
            update_interval: Duration::from_secs(1),
            collect_interval: Duration::from_millis(500),
            wake_interval: Duration::from_millis(200),
            connector_settings: Default::default(),
            listener_settings: Default::default(),
            garbage_collect_excluded: 0,
        }
    }
}
