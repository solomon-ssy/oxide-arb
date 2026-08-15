//! Exchange-history progress read boundary.

use crate::domain::data_plane::ExchangeHistoryFrontierProgress;

/// Read-only lock-free progress surface owned by the history worker.
pub trait ExchangeHistoryProgressPort: Send + Sync {
    fn snapshot(&self) -> ExchangeHistoryFrontierProgress;
}
