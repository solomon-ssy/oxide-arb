//! Admin port for authoring governed model specifications.
//!
//! A model spec is the root of the offline research lifecycle
//! (`immutable ModelSpec → Dataset Plan → … → Published ModelVersion`). This port
//! is the production write path an operator uses to mint a spec before planning
//! a training dataset or training a version.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{FeatureContractView, GovernanceActor, ModelSpecInfo},
    enums::model::ModelFamily,
    types::{
        ModelInputContract, ModelSpecId, ModelTrainingContract, SchemaVersion,
        model_spec::ModelSpecThesis,
    },
};

/// Service input to author a new immutable model specification.
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
    /// Closed research thesis; executable parameters cannot be stored here.
    pub thesis: ModelSpecThesis,
    /// Ordered raw-input contract. It is validated against the governed feature
    /// schema before persistence and never accepts transform-generated columns.
    pub input_contract: ModelInputContract,
    /// Frozen supervised target and CV policy.
    pub training_contract: ModelTrainingContract,
    /// Mandatory authoring rationale frozen atomically with the WORM spec.
    pub reason: String,
}

/// Model-spec authoring boundary, implemented in `quant-pivot-core`.
#[async_trait]
pub trait ModelSpecPort: Send + Sync {
    /// Return the active hash-bound raw-feature catalog used to author input
    /// contracts. Implementations must derive this from the same runtime
    /// snapshot used by [`Self::create`].
    async fn feature_contract(&self) -> QuantResult<FeatureContractView>;

    /// Mint a new `draft` model specification and return the persisted row.
    async fn create(
        &self,
        command: CreateModelSpecCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelSpecInfo>;

    /// Load one model specification row (detail drawer).
    async fn find(&self, model_spec_id: &ModelSpecId) -> QuantResult<Option<ModelSpecInfo>>;
}
