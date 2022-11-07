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
