use std::{sync::Arc, time::Duration};
use talk::time::{sleep_schedules::CappedExponential, SleepSchedule};

#[derive(Debug, Clone)]
pub struct LoadBrokerSettings {
    pub submission_schedule: Arc<dyn SleepSchedule>,
    pub witnessing_timeout: Duration,
    pub totality_timeout: Duration,
}

impl Default for LoadBrokerSettings {
    fn default() -> Self {
        LoadBrokerSettings {
            submission_schedule: Arc::new(CappedExponential::new(
                Duration::from_secs(1),
                2.,
                Duration::from_secs(60),
            )),
            witnessing_timeout: Duration::from_secs(1),
            totality_timeout: Duration::from_secs(30),
        }
    }
}
