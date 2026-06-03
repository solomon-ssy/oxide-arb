use crate::traits::{
    BalanceSnapshotRepository, ControlFactorDatasetRepository,
    ControlFactorShadowDecisionRepository, PositionExitRepository,
};
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
    entities::{
        balance_snapshot::{Column as BalanceColumn, Entity as BalanceEntity},
        control_factor_shadow_decision::Entity as ShadowDecisionEntity,
        control_factor_training_dataset::Entity as TrainingDatasetEntity,
        position_exit_execution::Entity as ExitExecutionEntity,
        position_exit_plan::{Column as ExitPlanColumn, Entity as ExitPlanEntity},
        position_unwind_audit::Entity as UnwindAuditEntity,
        token_balance_snapshot::{Column as TokenBalanceColumn, Entity as TokenBalanceEntity},
    },
    enums::fact::ExitPlanStatus,
    types::{ExitPlanId, MarketId, PositionId, TokenId, TrainingDatasetId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};
use std::collections::HashSet;

pub struct PgFactDataRepository {
    db: DatabaseConnection,
}

impl PgFactDataRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl BalanceSnapshotRepository for PgFactDataRepository {
    async fn create_balance_snapshot(
        &self,
        snapshot: NewBalanceSnapshot,
    ) -> Result<BalanceSnapshotInfo, StorageError> {
        BalanceEntity::insert(snapshot.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn create_token_balance_snapshots(
        &self,
        snapshots: Vec<NewTokenBalanceSnapshot>,
    ) -> Result<Vec<TokenBalanceSnapshotInfo>, StorageError> {
        if snapshots.is_empty() {
            return Ok(Vec::new());
        }
        TokenBalanceEntity::insert_many(
            snapshots
                .into_iter()
                .map(IntoActiveModel::into_active_model),
        )
        .exec_with_returning_many(&self.db)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
        .map_err(StorageError::from)
    }

    async fn latest_balance_before(
        &self,
        holder_address: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<BalanceSnapshotInfo>, StorageError> {
        BalanceEntity::find()
            .filter(BalanceColumn::HolderAddress.eq(holder_address))
            .filter(BalanceColumn::ObservedAt.lte(before))
            .order_by_desc(BalanceColumn::ObservedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_token_balance_before(
        &self,
        holder_address: &str,
        market_id: &MarketId,
        token_id: &TokenId,
        before: DateTime<Utc>,
    ) -> Result<Option<TokenBalanceSnapshotInfo>, StorageError> {
        TokenBalanceEntity::find()
            .filter(TokenBalanceColumn::HolderAddress.eq(holder_address))
            .filter(TokenBalanceColumn::MarketId.eq(market_id.clone()))
            .filter(TokenBalanceColumn::TokenId.eq(token_id.clone()))
            .filter(TokenBalanceColumn::ObservedAt.lte(before))
            .order_by_desc(TokenBalanceColumn::ObservedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_token_balances_before(
        &self,
        holder_address: &str,
        market_ids: &[MarketId],
        token_ids: &[TokenId],
        before: DateTime<Utc>,
    ) -> Result<Vec<TokenBalanceSnapshotInfo>, StorageError> {
        if market_ids.is_empty() || token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = TokenBalanceEntity::find()
            .filter(TokenBalanceColumn::HolderAddress.eq(holder_address))
            .filter(TokenBalanceColumn::MarketId.is_in(market_ids.iter().map(MarketId::as_str)))
            .filter(TokenBalanceColumn::TokenId.is_in(token_ids.iter().map(TokenId::as_str)))
            .filter(TokenBalanceColumn::ObservedAt.lte(before))
            .order_by_desc(TokenBalanceColumn::ObservedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut seen = HashSet::new();
        let mut latest = Vec::new();
        for row in rows {
            let key = (row.market_id.clone(), row.token_id.clone());
            if seen.insert(key) {
                latest.push(row.into());
            }
        }
        Ok(latest)
    }
}

#[async_trait::async_trait]
impl ControlFactorDatasetRepository for PgFactDataRepository {
    async fn create_training_dataset(
        &self,
        dataset: NewControlFactorTrainingDataset,
    ) -> Result<ControlFactorTrainingDatasetInfo, StorageError> {
        TrainingDatasetEntity::insert(dataset.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn load_training_dataset(
        &self,
        dataset_id: &TrainingDatasetId,
    ) -> Result<Option<ControlFactorTrainingDatasetInfo>, StorageError> {
        TrainingDatasetEntity::find_by_id(dataset_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}

#[async_trait::async_trait]
impl ControlFactorShadowDecisionRepository for PgFactDataRepository {
    async fn append_shadow_decision(
        &self,
        decision: NewControlFactorShadowDecision,
    ) -> Result<ControlFactorShadowDecisionInfo, StorageError> {
        ShadowDecisionEntity::insert(decision.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }
}

#[async_trait::async_trait]
impl PositionExitRepository for PgFactDataRepository {
    async fn create_exit_plan(
        &self,
        plan: NewPositionExitPlan,
    ) -> Result<PositionExitPlanInfo, StorageError> {
        ExitPlanEntity::insert(plan.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn append_exit_execution(
        &self,
        execution: NewPositionExitExecution,
    ) -> Result<PositionExitExecutionInfo, StorageError> {
        ExitExecutionEntity::insert(execution.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn append_unwind_audit(
        &self,
        audit: NewPositionUnwindAudit,
    ) -> Result<PositionUnwindAuditInfo, StorageError> {
        UnwindAuditEntity::insert(audit.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn exit_plans_by_position(
        &self,
        position_id: &PositionId,
        status: Option<ExitPlanStatus>,
    ) -> Result<Vec<PositionExitPlanInfo>, StorageError> {
        let mut query =
            ExitPlanEntity::find().filter(ExitPlanColumn::PositionId.eq(position_id.clone()));
        if let Some(status) = status {
            query = query.filter(ExitPlanColumn::Status.eq(status));
        }
        query
            .order_by_desc(ExitPlanColumn::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn load_exit_plan(
        &self,
        exit_plan_id: &ExitPlanId,
    ) -> Result<Option<PositionExitPlanInfo>, StorageError> {
        ExitPlanEntity::find_by_id(exit_plan_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
