mod option_delta_f64 {
    use std::time::SystemTime;

    pub(crate) fn option_delta_f64<F, T>(from: F, to: T) -> Option<f64>
    where
        F: MaybeTime,
        T: MaybeTime,
    {
        let from = from.optionify();
        let to = to.optionify();

        match (from, to) {
            (Some(from), Some(to)) => Some(to.duration_since(from).unwrap().as_secs_f64()),
            _ => None,
        }
    }

    pub(crate) trait MaybeTime {
        fn optionify(self) -> Option<SystemTime>;
    }

    impl MaybeTime for SystemTime {
        fn optionify(self) -> Option<SystemTime> {
            Some(self)
        }
    }

    impl MaybeTime for Option<SystemTime> {
        fn optionify(self) -> Option<SystemTime> {
            self
        }
    }
}

pub(crate) use option_delta_f64::option_delta_f64;
