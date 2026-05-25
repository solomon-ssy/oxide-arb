//! System health enums.

use serde::{Deserialize, Serialize};

/// Oracle/data source health state for sliding-window evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceHealth {
    Healthy,
    Degraded,
    Down,
}
