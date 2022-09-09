#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Amendment {
    Nudge { id: u64, sequence: u64 },
    Drop { id: u64 },
}
