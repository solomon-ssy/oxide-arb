use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        NewTradePolicyArtifact, NewTradePolicyGovernanceAudit, Paginated, TradePolicyArtifactInfo,
        TradePolicyListQuery,
    },
    enums::quant::TradePolicyStatus,
    types::TradePolicyArtifactId,
};

#[async_trait::async_trait]
pub trait TradePolicyRepository: Send + Sync {
    async fn insert(
        &self,
        artifact: NewTradePolicyArtifact,
    ) -> Result<TradePolicyArtifactInfo, StorageError>;

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyArtifactInfo>, StorageError>;

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> Result<Paginated<TradePolicyArtifactInfo>, StorageError>;

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        expected: TradePolicyStatus,
        target: TradePolicyStatus,
        audit: NewTradePolicyGovernanceAudit,
    ) -> Result<TradePolicyArtifactInfo, StorageError>;
}
