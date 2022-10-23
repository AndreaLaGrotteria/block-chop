use std::{sync::Arc, time::Duration};
use talk::time::{sleep_schedules::CappedExponential, SleepSchedule};

#[derive(Debug, Clone)]
pub struct LoadBrokerSettings {
    pub rate: f64,
    pub warmup: Duration,
    pub witnessing_timeout: Duration,
    pub totality_timeout: Duration,
    pub workers: u16,
    pub submission_interval: Duration,
    pub resubmission_schedule: Arc<dyn SleepSchedule>,
}

impl Default for LoadBrokerSettings {
    fn default() -> Self {
        LoadBrokerSettings {
            rate: 16.,
            warmup: Duration::from_secs(15),
            witnessing_timeout: Duration::from_secs(15),
            totality_timeout: Duration::from_secs(60),
            workers: 32768,
            submission_interval: Duration::from_millis(50),
            resubmission_schedule: Arc::new(CappedExponential::new(
                Duration::from_secs(1),
                2.,
                Duration::from_secs(60),
            )),
        }
    }
}
