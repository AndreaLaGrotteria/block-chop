#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::server) enum Duplicate {
    Ignore { id: u64 },
    Nudge { id: u64, sequence: u64 },
    Drop { id: u64 },
}
