//! Training dataset shared wire/domain types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrainingSampleSource {
    HistoricalPit,
    LiveAttribution,
}

#[must_use]
pub fn default_sample_sources() -> Vec<TrainingSampleSource> {
    vec![
        TrainingSampleSource::HistoricalPit,
        TrainingSampleSource::LiveAttribution,
    ]
}
