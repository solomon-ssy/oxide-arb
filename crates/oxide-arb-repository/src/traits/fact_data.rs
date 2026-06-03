use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        BalanceSnapshotInfo, ControlFactorShadowDecisionInfo, ControlFactorTrainingDatasetInfo,
        NewBalanceSnapshot, NewControlFactorShadowDecision, NewControlFactorTrainingDataset,
        NewPositionExitExecution, NewPositionExitPlan, NewPositionUnwindAudit,
        NewTokenBalanceSnapshot, PositionExitExecutionInfo, PositionExitPlanInfo,
        PositionUnwindAuditInfo, TokenBalanceSnapshotInfo,
    },
    enums::fact::ExitPlanStatus,
    types::{ExitPlanId, MarketId, PositionId, TokenId, TrainingDatasetId},
};

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

    async fn latest_token_balance_before(
        &self,
        market_id: &MarketId,
        token_id: &TokenId,
        before: DateTime<Utc>,
    ) -> Result<Option<TokenBalanceSnapshotInfo>, StorageError>;
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
