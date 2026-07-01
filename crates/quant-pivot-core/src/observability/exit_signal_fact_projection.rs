//! Projections from exit-signal domain verdicts to `ClickHouse` audit row enums.

use quant_pivot_models::enums::clickhouse::ChExitSignalVerdict;

use crate::execution::ExitSignalVerdict;

impl From<&ExitSignalVerdict> for ChExitSignalVerdict {
    fn from(verdict: &ExitSignalVerdict) -> Self {
        match verdict {
            ExitSignalVerdict::ThesisInvalidated { .. } => Self::ThesisInvalidated,
            ExitSignalVerdict::OpportunisticSell { .. } => Self::OpportunisticSell,
            ExitSignalVerdict::Holds => Self::Holds,
            ExitSignalVerdict::Indeterminate { .. } => Self::Indeterminate,
        }
    }
}
