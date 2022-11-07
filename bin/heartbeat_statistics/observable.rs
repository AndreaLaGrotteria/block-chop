use rayon::slice::ParallelSliceMut;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Observable {
    pub applicability: f64,
    pub average: f64,
    pub standard_deviation: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
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
        let median = statistical::median(values.as_slice());

        values.par_sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let min = *values.first().unwrap();
        let max = *values.last().unwrap();

        Observable {
            applicability,
            average,
            standard_deviation,
            median,
            min,
            max,
        }
    }
}
