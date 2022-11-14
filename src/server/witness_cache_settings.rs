#[derive(Debug, Clone)]
pub(in crate::server) struct WitnessCacheSettings {
    pub capacity: usize,
}

impl Default for WitnessCacheSettings {
    fn default() -> Self {
        WitnessCacheSettings { capacity: 131072 }
    }
}
