use std::{sync::Arc, time::Duration};

use talk::time::{sleep_schedules::CappedExponential, SleepSchedule};

#[derive(Debug, Clone)]
pub struct BrokerSettings {
    pub pool_capacity: usize,
    pub pool_timeout: Duration,
    pub reduction_timeout: Duration,

    pub maximum_packet_rate: f64,
    pub authenticate_tasks: usize,

    pub authentication_burst_size: usize,
    pub authentication_burst_timeout: Duration,
    pub authentication_burst_interval: Duration,
    pub pool_interval: Duration,
    pub reduction_burst_size: usize,
    pub reduction_interval: Duration,

    pub authenticate_channel_capacity: usize,
    pub handle_channel_capacity: usize,
    pub reduction_channel_capacity: usize,

    pub submission_schedule: Arc<dyn SleepSchedule>,
    pub witnessing_timeout: Duration,
    pub totality_timeout: Duration,
}

impl Default for BrokerSettings {
    fn default() -> Self {
        BrokerSettings {
            pool_capacity: 65536,
            pool_timeout: Duration::from_secs(1),
            reduction_timeout: Duration::from_secs(2),
            maximum_packet_rate: 262144.,
            authenticate_tasks: num_cpus::get(),
            authentication_burst_size: 2048,
            authentication_burst_timeout: Duration::from_millis(100),
            authentication_burst_interval: Duration::from_millis(10),
            pool_interval: Duration::from_millis(10),
            reduction_burst_size: 512,
            reduction_interval: Duration::from_millis(10),
            authenticate_channel_capacity: 1024,
            handle_channel_capacity: 1024,
            reduction_channel_capacity: 65536,
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
