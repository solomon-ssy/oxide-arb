use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        BalanceSnapshotInfo, ControlFactorShadowDecisionInfo, ControlFactorTrainingDatasetInfo,
        NewBalanceSnapshot, NewControlFactorShadowDecision, NewControlFactorTrainingDataset,
        ShadowDecisionAggregate, evidence::EvidenceQueryResult,
    },
    types::{FactorPublicationId, TrainingDatasetId},
};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait BalanceSnapshotRepository: Send + Sync {
    async fn create_balance_snapshot(
        &self,
        snapshot: NewBalanceSnapshot,
    ) -> Result<BalanceSnapshotInfo, StorageError>;

    async fn latest_balance_before(
        &self,
        holder_address: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<BalanceSnapshotInfo>, StorageError>;

    async fn latest_balance_before_evidence(
        &self,
        holder_address: &str,
        before: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<BalanceSnapshotInfo>, StorageError> {
        let rows = self
            .latest_balance_before(holder_address, before)
            .await?
            .into_iter()
            .collect();
        evidence_query_result(
            "BalanceSnapshotRepository",
            "latest_balance_before",
            &(holder_address, before),
            vec!["observed_at DESC".to_owned(), "id DESC".to_owned()],
            Some(1),
            rows,
        )
    }
}

#[async_trait::async_trait]
pub trait ControlFactorDatasetRepository: Send + Sync {
    async fn create_training_dataset(
        &self,
        dataset: NewControlFactorTrainingDataset,
    ) -> Result<ControlFactorTrainingDatasetInfo, StorageError>;

    async fn load_training_dataset(
        &self,
        dataset_id: &TrainingDatasetId,
    ) -> Result<Option<ControlFactorTrainingDatasetInfo>, StorageError>;
}

#[async_trait::async_trait]
pub trait ControlFactorShadowDecisionRepository: Send + Sync {
    async fn append_shadow_decision(
        &self,
        decision: NewControlFactorShadowDecision,
    ) -> Result<ControlFactorShadowDecisionInfo, StorageError>;

    /// Lists raw shadow decisions for a publication within `[from, to)`, newest
    /// first, capped at `limit`. The promotion-review consumer derives delta
    /// percentile distributions from these rows.
    async fn list_shadow_decisions(
        &self,
        publication_id: &FactorPublicationId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<ControlFactorShadowDecisionInfo>, StorageError>;

    /// Aggregates shadow-decision counts for a publication within `[from, to)`,
    /// grouped by `decision_type` plus a distinct-market count.
    async fn aggregate_shadow_decisions(
        &self,
        publication_id: &FactorPublicationId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<ShadowDecisionAggregate, StorageError>;
}
