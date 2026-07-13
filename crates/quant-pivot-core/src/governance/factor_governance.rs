//! [`FactorGovernanceService`]: publish / retire orchestration for governed factor
//! definitions (Phase 05.7).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        FactorDefinitionInfo, FactorGovernancePort, GovernanceActor, PublishFactorCommand,
        PublishFactorsBatchCommand, RegisterFactorDefinitionsCommand, RetireFactorCommand,
    },
    enums::quant::PublicationStatus,
    types::FactorDefinitionId,
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::factors::FactorEngine;

/// Dependencies for factor-definition governance.
pub struct FactorGovernanceDeps {
    /// Factor-definition persistence port.
    pub factor_repo: Arc<dyn FactorRepository>,
}

/// Publish / retire orchestration for governed factor definitions.
pub struct FactorGovernanceService {
    deps: FactorGovernanceDeps,
}

impl FactorGovernanceService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub const fn new(deps: FactorGovernanceDeps) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl FactorGovernancePort for FactorGovernanceService {
    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        self.deps
            .factor_repo
            .find_definition(factor_definition_id)
            .await
            .map_err(Into::into)
    }

    async fn publish(
        &self,
        command: PublishFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo> {
        let _reason = command.reason;
        let current = self
            .require_definition(&command.factor_definition_id)
            .await?;
        if current.status == PublicationStatus::Published {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "factor definition {} is already published",
                    current.factor_definition_id
                ),
            }
            .into());
        }
        if !current
            .status
            .allows_transition_to(PublicationStatus::Published)
        {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot publish factor definition {} from status {}",
                    current.factor_definition_id,
                    current.status.as_str()
                ),
            }
            .into());
        }
        self.deps
            .factor_repo
            .publish_definition(&command.factor_definition_id)
            .await
            .map_err(Into::into)
    }

    async fn retire(
        &self,
        command: RetireFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo> {
        let _reason = command.reason;
        let current = self
            .require_definition(&command.factor_definition_id)
            .await?;
        if current.status != PublicationStatus::Published {
            return Err(GovernanceError::IllegalTransition {
                detail: format!(
                    "cannot retire factor definition {} in status {}",
                    current.factor_definition_id,
                    current.status.as_str()
                ),
            }
            .into());
        }
        self.deps
            .factor_repo
            .retire_definition(&command.factor_definition_id)
            .await
            .map_err(Into::into)
    }

    async fn register_enabled_definitions(
        &self,
        command: RegisterFactorDefinitionsCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>> {
        let engine = FactorEngine::new(&command.factors, &command.features, &command.domain, None);
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }
        let mut registered = Vec::new();
        for spec in &engine.factor_set().definitions {
            let identity = engine.definition_identity(&spec.name)?;
            let definition = spec.try_to_new(command.features.feature_schema_version, &identity)?;
            let row = self
                .deps
                .factor_repo
                .create_definition(definition)
                .await
                .map_err(QuantError::from)?;
            registered.push(row);
        }
        Ok(registered)
    }

    async fn publish_batch(
        &self,
        command: PublishFactorsBatchCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>> {
        let _reason = command.reason;
        self.deps
            .factor_repo
            .publish_definitions(&command.factor_definition_ids)
            .await
            .map_err(Into::into)
    }
}

impl FactorGovernanceService {
    async fn require_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<FactorDefinitionInfo> {
        self.deps
            .factor_repo
            .find_definition(factor_definition_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                GovernanceError::IllegalTransition {
                    detail: format!("factor definition not found: {factor_definition_id}"),
                }
                .into()
            })
    }
}
