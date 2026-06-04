use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        BalanceSnapshotInfo, ControlFactorShadowDecisionInfo, ControlFactorTrainingDatasetInfo,
        NewBalanceSnapshot, NewControlFactorShadowDecision, NewControlFactorTrainingDataset,
        NewPositionExitExecution, NewPositionExitPlan, NewPositionUnwindAudit,
        NewTokenBalanceSnapshot, PositionExitExecutionInfo, PositionExitPlanInfo,
        PositionUnwindAuditInfo, TokenBalanceSnapshotInfo, evidence::EvidenceQueryResult,
    },
    enums::fact::ExitPlanStatus,
    types::{ExitPlanId, MarketId, PositionId, TokenId, TrainingDatasetId},
};

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait BalanceSnapshotRepository: Send + Sync {
    async fn create_balance_snapshot(
        &self,
        snapshot: NewBalanceSnapshot,
    ) -> Result<BalanceSnapshotInfo, StorageError>;

    async fn create_token_balance_snapshots(
        &self,
        snapshots: Vec<NewTokenBalanceSnapshot>,
    ) -> Result<Vec<TokenBalanceSnapshotInfo>, StorageError>;

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

    async fn latest_token_balance_before(
        &self,
        holder_address: &str,
        market_id: &MarketId,
        token_id: &TokenId,
        before: DateTime<Utc>,
    ) -> Result<Option<TokenBalanceSnapshotInfo>, StorageError>;

    async fn latest_token_balances_before(
        &self,
        holder_address: &str,
        market_ids: &[MarketId],
        token_ids: &[TokenId],
        before: DateTime<Utc>,
    ) -> Result<Vec<TokenBalanceSnapshotInfo>, StorageError>;

    async fn latest_token_balances_before_evidence(
        &self,
        holder_address: &str,
        market_ids: &[MarketId],
        token_ids: &[TokenId],
        before: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<TokenBalanceSnapshotInfo>, StorageError> {
        let mut sorted_market_ids = market_ids.to_vec();
        let mut sorted_token_ids = token_ids.to_vec();
        sorted_market_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sorted_market_ids.dedup();
        sorted_token_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        sorted_token_ids.dedup();
        let rows = self
            .latest_token_balances_before(
                holder_address,
                &sorted_market_ids,
                &sorted_token_ids,
                before,
            )
            .await?;
        evidence_query_result(
            "BalanceSnapshotRepository",
            "latest_token_balances_before",
            &(holder_address, sorted_market_ids, sorted_token_ids, before),
            vec![
                "market_id ASC".to_owned(),
                "token_id ASC".to_owned(),
                "observed_at DESC".to_owned(),
            ],
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
}

#[async_trait::async_trait]
pub trait PositionExitRepository: Send + Sync {
    async fn create_exit_plan(
        &self,
        plan: NewPositionExitPlan,
    ) -> Result<PositionExitPlanInfo, StorageError>;

    async fn append_exit_execution(
        &self,
        execution: NewPositionExitExecution,
    ) -> Result<PositionExitExecutionInfo, StorageError>;

    async fn append_unwind_audit(
        &self,
        audit: NewPositionUnwindAudit,
    ) -> Result<PositionUnwindAuditInfo, StorageError>;

    async fn exit_plans_by_position(
        &self,
        position_id: &PositionId,
        status: Option<ExitPlanStatus>,
    ) -> Result<Vec<PositionExitPlanInfo>, StorageError>;

    async fn load_exit_plan(
        &self,
        exit_plan_id: &ExitPlanId,
    ) -> Result<Option<PositionExitPlanInfo>, StorageError>;
}
