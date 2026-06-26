//! Market-selection wire enums (snapshot JSON; no Postgres column).

use serde::{Deserialize, Serialize};

/// Why a market was excluded from the selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    NotOpen,
    CategoryDisabled,
    InsufficientLiquidity,
    SpreadTooWide,
    StaleBook,
    FactLagExceeded,
    ResolutionAmbiguous,
    ManuallyBlocked,
    SelectionCapExceeded,
    ModelFeatureUnavailable { missing: Vec<String> },
}
