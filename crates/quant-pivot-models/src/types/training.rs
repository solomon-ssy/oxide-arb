//! Training dataset shared wire/domain types.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::quant::DatasetPurpose,
    jsonb_active,
    types::{
        ContentHash, ModelSpecId, RuntimeConfigVersionId, TradePolicyArtifactId, TrainingDatasetId,
    },
};

/// Breaking dataset artifact and manifest wire version.
pub const DATASET_ARTIFACT_FORMAT_VERSION: u32 = 4;

/// Immutable manifest embedded in a frozen dataset artifact and ledger.
///
/// This is the single data contract shared by research, persistence, and the
/// admin API; integrity algorithms remain in research.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct DatasetManifest {
    pub format_version: u32,
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: u64,
    pub sample_interval_secs: u64,
    pub horizons_secs: Vec<u64>,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub semantic_dataset_hash: ContentHash,
    pub source_fingerprint: ContentHash,
    pub sample_count: u64,
}

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

/// Ordered sample-source contract frozen on a dataset plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct TrainingSampleSources(pub Vec<TrainingSampleSource>);

jsonb_active!(DatasetManifest, TrainingSampleSources);

#[must_use]
pub fn default_sample_sources() -> Vec<TrainingSampleSource> {
    vec![
        TrainingSampleSource::HistoricalPit,
        TrainingSampleSource::LiveAttribution,
    ]
}
