//! Admin port for factor-definition register / publish / retire (Phase 05.7).

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{FactorDefinitionInfo, GovernanceActor},
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig},
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

/// Service input to register the currently enabled factor definitions.
///
/// The factor set is derived from the frozen runtime config the operator is
/// running; the definitions are upserted as `Draft` (idempotent, preserving the
/// publication status of any already-registered definition).
#[derive(Debug, Clone)]
pub struct RegisterFactorDefinitionsCommand {
    /// Frozen factor config selecting the enabled factor set.
    pub factors: FactorsConfig,
    /// Frozen feature config resolving windowed factor inputs + schema version.
    pub features: FeaturesConfig,
    /// Frozen domain config selecting the category-routed domain factor set.
    pub domain: DomainConfig,
    /// Operator reason (HTTP op-log only).
    pub reason: String,
}

/// Service input to publish a batch of draft / retired factor definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishFactorsBatchCommand {
    /// The factor definitions to publish (already-published ids are a no-op).
    pub factor_definition_ids: Vec<FactorDefinitionId>,
    /// Operator reason (HTTP op-log only).
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

    /// Register (idempotent upsert) every enabled factor definition as `Draft`,
    /// returning the resulting rows (with their current publication status). This
    /// is the explicit bootstrap step that seeds the factor catalog before the
    /// online report path (which fails closed on non-`Published` definitions).
    async fn register_enabled_definitions(
        &self,
        command: RegisterFactorDefinitionsCommand,
        actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>>;

    /// Publish a batch of draft / retired definitions in one governed action.
    /// Already-`Published` ids are skipped (idempotent); an illegal transition on
    /// any id aborts before mutation.
    async fn publish_batch(
        &self,
        command: PublishFactorsBatchCommand,
        actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>>;
}
