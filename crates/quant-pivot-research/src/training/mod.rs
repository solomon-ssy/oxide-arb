//! Training-data plane: dataset planning/building and labeling contracts.
//!
//! Offline closure (3.5). 3.0 fixes the trait surface + minimal I/O shells; the
//! dataset/label bodies (polars materialization, PIT label resolution) are
//! filled in 3.5.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{
    ArtifactUri, ContentHash, MarketId, SchemaVersion, TokenId, TrainingDatasetId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{naming::stable_name, pit::PitQueryEngine};

stable_name! {
    /// Stable, compile-time-known label name (e.g. `"realized_return_1h"`).
    LabelName
}

/// Request to plan a training dataset over a historical window.
///
/// Extended in 3.5 with selection/feature/label specs and sampling policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPlanRequest {
    /// Decision time the plan was requested as of.
    pub as_of: DateTime<Utc>,
    /// Inclusive window start.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end.
    pub window_end: DateTime<Utc>,
    /// Feature schema version to materialize against.
    pub feature_schema_version: SchemaVersion,
}

/// A resolved plan: which markets/instants the dataset will materialize.
///
/// Extended in 3.5 with per-market sampling instants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPlan {
    /// The originating request.
    pub request: DatasetPlanRequest,
    /// Markets included in the plan.
    pub market_ids: Vec<MarketId>,
}

/// A frozen, content-addressed training dataset artifact.
///
/// Extended in 3.5 with column schema and row-group metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingDatasetArtifact {
    /// Dataset id.
    pub training_dataset_id: TrainingDatasetId,
    /// Feature-schema hash the dataset was built against.
    pub feature_schema_hash: ContentHash,
    /// Label-schema hash the dataset was built against.
    pub label_schema_hash: ContentHash,
    /// Location of the materialized parquet bytes.
    pub parquet_uri: ArtifactUri,
    /// Number of sample rows.
    pub row_count: u64,
}

/// Inputs to building a single training label.
pub struct LabelBuildInput<'a> {
    /// Market the label is for.
    pub market_id: &'a MarketId,
    /// Outcome token the label is for.
    pub token_id: &'a TokenId,
    /// Decision time the label is anchored at.
    pub as_of: DateTime<Utc>,
    /// Forward horizon, in seconds, the label looks ahead to (post-`as_of`).
    pub horizon_secs: u64,
    /// Historical PIT engine used to resolve the realized outcome.
    pub pit: &'a dyn PitQueryEngine,
}

/// The resolved value of a training label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelBuildOutput {
    /// Label name.
    pub label_name: LabelName,
    /// Resolved label value.
    pub value: Decimal,
    /// Whether the outcome was fully resolved (vs. censored at window end).
    pub is_resolved: bool,
}

/// Plans a training dataset (which markets / instants to materialize).
#[async_trait]
pub trait TrainingDatasetPlanner: Send + Sync {
    /// Resolve a plan from a request.
    async fn plan(&self, request: DatasetPlanRequest) -> QuantResult<DatasetPlan>;
}

/// Materializes a planned dataset into a frozen, hashed artifact.
#[async_trait]
pub trait TrainingDatasetBuilder: Send + Sync {
    /// Build the dataset artifact from a resolved plan.
    async fn build(&self, plan: DatasetPlan) -> QuantResult<TrainingDatasetArtifact>;
}

/// Builds a single forward-looking training label, point-in-time correct.
#[async_trait]
pub trait Labeler: Send + Sync {
    /// The label this labeler produces.
    fn label_name(&self) -> LabelName;

    /// Resolve the label for one sample.
    async fn build_label(&self, input: LabelBuildInput<'_>) -> QuantResult<LabelBuildOutput>;
}
