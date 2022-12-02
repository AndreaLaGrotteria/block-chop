use rayon::slice::ParallelSliceMut;
use std::fmt::{self, Debug, Formatter};

#[allow(dead_code)]
pub(crate) struct Observable {
    pub applicability: f64,
    pub average: f64,
    pub standard_deviation: f64,
    pub percentiles: [f64; 101],
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

        let mut percentiles = [0f64; 101];
        percentiles.copy_from_slice(
            &(0..=100)
                .map(|percentile| {
                    let index = std::cmp::min(
                        ((values.len() as f64) * percentile as f64 / 100f64) as usize,
                        values.len(),
                    );
                    values[index]
                })
                .collect::<Vec<_>>(),
        );

        Observable {
            applicability,
            average,
            standard_deviation,
            percentiles,
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
                format_time(self.percentiles[0]),
                format_time(self.percentiles[5]),
                format_time(self.percentiles[25]),
                format_time(self.percentiles[50]),
                format_time(self.percentiles[75]),
                format_time(self.percentiles[95]),
                format_time(self.percentiles[100]),
            )
        } else {
            write!(
                fmt,
                "{} ± {} (~{:.02}) [0%: {}, 5%: {}, 25%: {}, 50%: {}, 75%: {}, 95%: {}, 100%: {}]",
                self.average,
                self.standard_deviation,
                self.applicability,
                format_time(self.percentiles[0]),
                format_time(self.percentiles[5]),
                format_time(self.percentiles[25]),
                format_time(self.percentiles[50]),
                format_time(self.percentiles[75]),
                format_time(self.percentiles[95]),
                format_time(self.percentiles[100]),
            )
        }
    }
}
