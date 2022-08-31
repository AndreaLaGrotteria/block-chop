#[derive(Debug, Clone)]
pub struct ServerSettings {
    pub serve_tasks: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings { serve_tasks: 27 }
    }
}
