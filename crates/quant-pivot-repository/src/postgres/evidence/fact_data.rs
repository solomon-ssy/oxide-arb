use crate::traits::{
    BalanceSnapshotRepository, ControlFactorDatasetRepository,
    ControlFactorShadowDecisionRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        BalanceSnapshotInfo, ControlFactorShadowDecisionInfo, ControlFactorTrainingDatasetInfo,
        NewBalanceSnapshot, NewControlFactorShadowDecision, NewControlFactorTrainingDataset,
        ShadowDecisionAggregate,
    },
    entities::{
        balance_snapshot::{Column as BalanceColumn, Entity as BalanceEntity},
        control_factor_shadow_decision::{
            Column as ShadowDecisionColumn, Entity as ShadowDecisionEntity,
        },
        control_factor_training_dataset::Entity as TrainingDatasetEntity,
    },
    enums::fact::ShadowDecisionType,
    types::{FactorPublicationId, TrainingDatasetId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, IntoActiveModel, QueryFilter,
    QueryOrder, QuerySelect,
    sea_query::{Expr, Func},
};

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

/// Grouped `decision_type` count row used to assemble [`ShadowDecisionAggregate`].
#[derive(Debug, FromQueryResult)]
struct ShadowDecisionTypeCount {
    decision_type: ShadowDecisionType,
    count: i64,
}

/// Single-row `COUNT(DISTINCT market_id)` projection for the shadow aggregate.
#[derive(Debug, FromQueryResult)]
struct ShadowDistinctMarkets {
    distinct_markets: i64,
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

    async fn list_shadow_decisions(
        &self,
        publication_id: &FactorPublicationId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<ControlFactorShadowDecisionInfo>, StorageError> {
        ShadowDecisionEntity::find()
            .filter(ShadowDecisionColumn::PublicationId.eq(publication_id.clone()))
            .filter(ShadowDecisionColumn::DecidedAt.gte(from))
            .filter(ShadowDecisionColumn::DecidedAt.lt(to))
            .order_by_desc(ShadowDecisionColumn::DecidedAt)
            .order_by_desc(ShadowDecisionColumn::ShadowDecisionId)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn aggregate_shadow_decisions(
        &self,
        publication_id: &FactorPublicationId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<ShadowDecisionAggregate, StorageError> {
        let counts: Vec<ShadowDecisionTypeCount> = ShadowDecisionEntity::find()
            .select_only()
            .column(ShadowDecisionColumn::DecisionType)
            .column_as(ShadowDecisionColumn::ShadowDecisionId.count(), "count")
            .filter(ShadowDecisionColumn::PublicationId.eq(publication_id.clone()))
            .filter(ShadowDecisionColumn::DecidedAt.gte(from))
            .filter(ShadowDecisionColumn::DecidedAt.lt(to))
            .group_by(ShadowDecisionColumn::DecisionType)
            .into_model::<ShadowDecisionTypeCount>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;

        let distinct: Option<ShadowDistinctMarkets> = ShadowDecisionEntity::find()
            .select_only()
            .column_as(
                Expr::expr(Func::count_distinct(Expr::col(
                    ShadowDecisionColumn::MarketId,
                ))),
                "distinct_markets",
            )
            .filter(ShadowDecisionColumn::PublicationId.eq(publication_id.clone()))
            .filter(ShadowDecisionColumn::DecidedAt.gte(from))
            .filter(ShadowDecisionColumn::DecidedAt.lt(to))
            .into_model::<ShadowDistinctMarkets>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;

        let mut aggregate = ShadowDecisionAggregate::empty(publication_id.clone());
        for bucket in counts {
            aggregate.add_bucket(
                bucket.decision_type,
                u64::try_from(bucket.count).unwrap_or(0),
            );
        }
        aggregate.distinct_markets =
            distinct.map_or(0, |row| u64::try_from(row.distinct_markets).unwrap_or(0));
        Ok(aggregate)
    }
}
