//! Admin port for factor-definition publish / retire (Phase 05.7).

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{FactorDefinitionInfo, GovernanceActor},
    types::FactorDefinitionId,
};

/// Service input to publish a draft / retired factor definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishFactorCommand {
    /// The factor definition to publish.
    pub factor_definition_id: FactorDefinitionId,
    /// Operator reason (HTTP op-log only in 05.7).
    pub reason: String,
}

/// Service input to retire a published factor definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireFactorCommand {
    /// The published factor definition to retire.
    pub factor_definition_id: FactorDefinitionId,
    /// Operator reason (HTTP op-log only in 05.7).
    pub reason: String,
}

/// Factor-definition governance boundary, implemented in `quant-pivot-core`.
#[async_trait]
pub trait FactorGovernancePort: Send + Sync {
    /// Load one governed factor definition row.
    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>>;

    /// Promote a draft / retired definition to `Published`.
    async fn publish(
        &self,
        command: PublishFactorCommand,
        actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo>;

    /// Retire a published definition without deleting historical factor values.
    async fn retire(
        &self,
        command: RetireFactorCommand,
        actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo>;
}
