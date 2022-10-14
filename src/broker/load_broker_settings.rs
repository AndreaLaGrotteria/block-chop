use std::{sync::Arc, time::Duration};
use talk::time::{sleep_schedules::CappedExponential, SleepSchedule};

#[derive(Debug, Clone)]
pub struct LoadBrokerSettings {
    pub rate: f64,
    pub witnessing_timeout: Duration,
    pub totality_timeout: Duration,
    pub workers: u16,
    pub minimum_rate_window: Duration,
    pub maximum_rate_window: Duration,
    pub submission_schedule: Arc<dyn SleepSchedule>,
}

impl Default for LoadBrokerSettings {
    fn default() -> Self {
        LoadBrokerSettings {
            rate: 16.,
            witnessing_timeout: Duration::from_secs(15),
            totality_timeout: Duration::from_secs(60),
            workers: 32768,
            minimum_rate_window: Duration::from_millis(50),
            maximum_rate_window: Duration::from_millis(200),
            submission_schedule: Arc::new(CappedExponential::new(
                Duration::from_secs(1),
                2.,
                Duration::from_secs(60),
            )),
        }
    }
}
