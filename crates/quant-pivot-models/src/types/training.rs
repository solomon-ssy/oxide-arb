//! Training dataset shared wire/domain types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainingSampleSource {
    HistoricalPit,
    LiveAttribution,
    /// Per-tick hold-vs-exit decision points sampled along a closed/settled
    /// lot's life (Phase 06.1 Sell scorer training). Anchored on position-lot
    /// timelines rather than a uniform market grid.
    ExitDecision,
}

#[must_use]
pub fn default_sample_sources() -> Vec<TrainingSampleSource> {
    vec![
        TrainingSampleSource::HistoricalPit,
        TrainingSampleSource::LiveAttribution,
    ]
}
