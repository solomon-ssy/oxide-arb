//! [`ModelSpecService`]: authoring of governed model specifications — the
//! production write path that seeds the offline research lifecycle root.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{CreateModelSpecCommand, GovernanceActor, ModelSpecInfo, ModelSpecPort, NewModelSpec},
    enums::quant::PublicationStatus,
    types::ModelSpecId,
};
use quant_pivot_repository::traits::ModelRegistryRepository;

/// Dependencies for model-spec authoring.
pub struct ModelSpecDeps {
    /// Model registry persistence port.
    pub model_registry: Arc<dyn ModelRegistryRepository>,
}

/// Authoring orchestration for governed model specifications.
pub struct ModelSpecService {
    deps: ModelSpecDeps,
}

impl ModelSpecService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub const fn new(deps: ModelSpecDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl ModelSpecPort for ModelSpecService {
    async fn create(
        &self,
        command: CreateModelSpecCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelSpecInfo> {
        let _reason = command.reason;
        self.deps
            .model_registry
            .create_model_spec(NewModelSpec {
                model_spec_id: ModelSpecId::from_v7(),
                name: command.name,
                model_family: command.model_family,
                prediction_horizon_secs: command.prediction_horizon_secs,
                feature_schema_version: command.feature_schema_version,
                label_schema_version: command.label_schema_version,
                spec_json: command.spec_json,
                status: PublicationStatus::Draft,
            })
            .await
            .map_err(Into::into)
    }

    async fn find(&self, model_spec_id: &ModelSpecId) -> QuantResult<Option<ModelSpecInfo>> {
        self.deps
            .model_registry
            .find_model_spec_by_id(model_spec_id)
            .await
            .map_err(Into::into)
    }
}
