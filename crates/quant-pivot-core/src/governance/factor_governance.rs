//! [`FactorGovernanceService`]: publish / retire orchestration for governed factor
//! definitions (Phase 05.7).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        FactorDefinitionInfo, FactorGovernancePort, GovernanceActor, PublishFactorCommand,
        RetireFactorCommand,
    },
    enums::quant::PublicationStatus,
    types::FactorDefinitionId,
};
use quant_pivot_repository::traits::FactorRepository;

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
