//! Admin port for authoring governed model specifications.
//!
//! A model spec is the root of the offline research lifecycle
//! (`Draft ModelSpec → Dataset Plan → … → Published ModelVersion`). This port
//! is the production write path an operator uses to mint a spec before planning
//! a training dataset or training a version.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{GovernanceActor, ModelSpecInfo},
    enums::model::ModelFamily,
    types::{ModelSpecId, SchemaVersion},
};

/// Service input to author a new `draft` model specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateModelSpecCommand {
    /// Human-facing spec name shown in the catalog picker.
    pub name: String,
    /// Model family the spec authors.
    pub model_family: ModelFamily,
    /// Model-intrinsic prediction horizon in seconds.
    pub prediction_horizon_secs: i64,
    /// Feature schema version the spec targets.
    pub feature_schema_version: SchemaVersion,
    /// Label schema version the spec targets.
    pub label_schema_version: SchemaVersion,
    /// Free-form authoring metadata.
    pub spec_json: serde_json::Value,
    /// Governed feature-requirements contract (11.2.2 remediation R7):
    /// deserializes to `quant_pivot_research::selection::ModelFeatureRequirements`.
    /// Validated by `ModelSpecService::create` before persistence — an
    /// unparseable value fails the request closed, never silently defaults.
    pub feature_requirements: serde_json::Value,
    /// Operator reason (HTTP op-log only).
    pub reason: String,
}

/// Model-spec authoring boundary, implemented in `quant-pivot-core`.
#[async_trait]
pub trait ModelSpecPort: Send + Sync {
    /// Mint a new `draft` model specification and return the persisted row.
    async fn create(
        &self,
        command: CreateModelSpecCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelSpecInfo>;

    /// Load one model specification row (detail drawer).
    async fn find(&self, model_spec_id: &ModelSpecId) -> QuantResult<Option<ModelSpecInfo>>;
}
