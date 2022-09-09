use crate::broadcast::Amendment;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::server) enum Duplicate {
    Ignore { id: u64 },
    Nudge { id: u64, sequence: u64 },
    Drop { id: u64 },
}

impl Duplicate {
    pub fn amendment(&self) -> Option<Amendment> {
        match *self {
            Duplicate::Ignore { .. } => None,
            Duplicate::Nudge { id, sequence } => Some(Amendment::Nudge { id, sequence }),
            Duplicate::Drop { id } => Some(Amendment::Drop { id }),
        }
    }
}
