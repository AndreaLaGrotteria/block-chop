use rayon::slice::ParallelSliceMut;
use std::fmt::{self, Debug, Formatter};

#[allow(dead_code)]
pub(crate) struct Observable {
    pub applicability: f64,
    pub average: f64,
    pub standard_deviation: f64,
    pub percentiles: Percentiles,
}

pub(crate) struct Percentiles {
    pub zero: f64,
    pub five: f64,
    pub twenty_five: f64,
    pub fifty: f64,
    pub seventy_five: f64,
    pub ninety_five: f64,
    pub hundred: f64,
}

impl Observable {
    pub fn from_samples<'s, SI, S, O>(samples: SI, observable: O) -> Self
    where
        SI: IntoIterator<Item = &'s S>,
        S: 's,
        O: Fn(&'s S) -> Option<f64>,
    {
        let mut non_applicable = 0;

        let mut values = samples
            .into_iter()
            .map(observable)
            .filter_map(|value| {
                if value.is_none() {
                    non_applicable += 1;
                }

                value
            })
            .collect::<Vec<_>>();

        let applicability = (values.len() as f64) / ((values.len() + non_applicable) as f64);

        let average = statistical::mean(values.as_slice());
        let standard_deviation = statistical::standard_deviation(values.as_slice(), None);

        values.par_sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let zero = *values.first().unwrap();
        let five = values[((values.len() as f64) * 0.05) as usize];
        let twenty_five = values[((values.len() as f64) * 0.25) as usize];
        let fifty = values[((values.len() as f64) * 0.5) as usize];
        let seventy_five = values[((values.len() as f64) * 0.75) as usize];
        let ninety_five = values[((values.len() as f64) * 0.95) as usize];
        let hundred = *values.last().unwrap();

        Observable {
            applicability,
            average,
            standard_deviation,
            percentiles: Percentiles {
                zero,
                five,
                twenty_five,
                fifty,
                seventy_five,
                ninety_five,
                hundred,
            },
        }
    }
}

impl Debug for Observable {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        fn format_time(mut time: f64) -> String {
            if time >= 1. {
                return format!("{time:.02} s");
            }

            time *= 1000.;

            if time >= 1. {
                return format!("{time:.02} ms");
            }

            time *= 1000.;

            if time >= 1. {
                return format!("{time:.02} us");
            }

            time *= 1000.;

            format!("{time:.02} ns")
        }

        if fmt.alternate() {
            write!(
                fmt,
                "{} ± {} (~{:.02}) [0%: {}, 5%: {}, 25%: {}, 50%: {}, 75%: {}, 95%: {}, 100%: {}]",
                format_time(self.average),
                format_time(self.standard_deviation),
                self.applicability,
                format_time(self.percentiles.zero),
                format_time(self.percentiles.five),
                format_time(self.percentiles.twenty_five),
                format_time(self.percentiles.fifty),
                format_time(self.percentiles.seventy_five),
                format_time(self.percentiles.ninety_five),
                format_time(self.percentiles.hundred),
            )
        } else {
            write!(
                fmt,
                "{} ± {} (~{:.02}) [0%: {}, 5%: {}, 25%: {}, 50%: {}, 75%: {}, 95%: {}, 100%: {}]",
                self.average,
                self.standard_deviation,
                self.applicability,
                self.percentiles.zero,
                self.percentiles.five,
                self.percentiles.twenty_five,
                self.percentiles.fifty,
                self.percentiles.seventy_five,
                self.percentiles.ninety_five,
                self.percentiles.hundred,
            )
        }
    }
}
