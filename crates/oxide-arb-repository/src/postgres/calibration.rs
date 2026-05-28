use super::orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter,
};
use crate::traits::CalibrationRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::calibration::{
    ActiveModel as CalibActiveModel, Column as CalibColumn, Entity as CalibEntity,
};
use oxide_arb_models::entities::calibration_outcome::{
    Column as OutcomeColumn, Entity as OutcomeEntity,
};
use oxide_arb_models::{
    domain::{
        CalibrationBucketInfo, CalibrationOutcomeInfo, NewCalibrationOutcome, UpsertCalibration,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
    },
};
use sea_orm::sea_query::{Expr, OnConflict};

// ── helpers ──────────────────────────────────────────────────────────

async fn get_bucket_q(
    db: &impl ConnectionTrait,
    category: MarketCategory,
    price_zone: PriceZone,
    duration_bucket: DurationBucket,
) -> Result<Option<CalibrationBucketInfo>, StorageError> {
    CalibEntity::find()
        .filter(CalibColumn::Category.eq(category))
        .filter(CalibColumn::PriceZone.eq(price_zone))
        .filter(CalibColumn::DurationBucket.eq(duration_bucket))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn get_buckets_by_category_q(
    db: &impl ConnectionTrait,
    category: MarketCategory,
) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
    CalibEntity::find()
        .filter(CalibColumn::Category.eq(category))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn get_all_buckets_q(
    db: &impl ConnectionTrait,
) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
    CalibEntity::find()
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn upsert_q(
    db: &impl ConnectionTrait,
    dto: UpsertCalibration,
) -> Result<CalibrationBucketInfo, StorageError> {
    let am: CalibActiveModel = dto.into_active_model();
    let model = CalibEntity::insert(am)
        .on_conflict(
            OnConflict::columns([
                CalibColumn::Category,
                CalibColumn::PriceZone,
                CalibColumn::DurationBucket,
            ])
            .update_columns([
                CalibColumn::TotalCount,
                CalibColumn::CorrectCount,
                CalibColumn::AlphaPrior,
                CalibColumn::BetaPrior,
                CalibColumn::PosteriorMean,
                CalibColumn::UpdatedAt,
            ])
            .to_owned(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn create_outcome_q(
    db: &impl ConnectionTrait,
    outcome: NewCalibrationOutcome,
) -> Result<CalibrationOutcomeInfo, StorageError> {
    let am = outcome.into_active_model();
    let model = OutcomeEntity::insert(am)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn get_unresolved_outcomes_q(
    db: &impl ConnectionTrait,
) -> Result<Vec<CalibrationOutcomeInfo>, StorageError> {
    OutcomeEntity::find()
        .filter(OutcomeColumn::ActualYes.is_null())
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn resolve_outcome_q(
    db: &impl ConnectionTrait,
    outcome_id: i64,
    actual_yes: bool,
) -> Result<(), StorageError> {
    OutcomeEntity::update_many()
        .col_expr(OutcomeColumn::ActualYes, Expr::value(Some(actual_yes)))
        .col_expr(OutcomeColumn::ResolvedAt, Expr::value(Some(Utc::now())))
        .filter(OutcomeColumn::Id.eq(outcome_id))
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

// ── connection-based impl ────────────────────────────────────────────

pub struct PgCalibrationRepository {
    db: DatabaseConnection,
}

impl PgCalibrationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgCalibrationRepositoryTxn<'_> {
        PgCalibrationRepositoryTxn { txn }
    }
}

#[async_trait::async_trait]
impl CalibrationRepository for PgCalibrationRepository {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<CalibrationBucketInfo>, StorageError> {
        get_bucket_q(&self.db, category, price_zone, duration_bucket).await
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        get_buckets_by_category_q(&self.db, category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        get_all_buckets_q(&self.db).await
    }

    async fn upsert(&self, dto: UpsertCalibration) -> Result<CalibrationBucketInfo, StorageError> {
        upsert_q(&self.db, dto).await
    }

    async fn create_outcome(
        &self,
        outcome: NewCalibrationOutcome,
    ) -> Result<CalibrationOutcomeInfo, StorageError> {
        create_outcome_q(&self.db, outcome).await
    }

    async fn get_unresolved_outcomes(&self) -> Result<Vec<CalibrationOutcomeInfo>, StorageError> {
        get_unresolved_outcomes_q(&self.db).await
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        resolve_outcome_q(&self.db, outcome_id, actual_yes).await
    }
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgCalibrationRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[async_trait::async_trait]
impl CalibrationRepository for PgCalibrationRepositoryTxn<'_> {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<CalibrationBucketInfo>, StorageError> {
        get_bucket_q(self.txn, category, price_zone, duration_bucket).await
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        get_buckets_by_category_q(self.txn, category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        get_all_buckets_q(self.txn).await
    }

    async fn upsert(&self, dto: UpsertCalibration) -> Result<CalibrationBucketInfo, StorageError> {
        upsert_q(self.txn, dto).await
    }

    async fn create_outcome(
        &self,
        outcome: NewCalibrationOutcome,
    ) -> Result<CalibrationOutcomeInfo, StorageError> {
        create_outcome_q(self.txn, outcome).await
    }

    async fn get_unresolved_outcomes(&self) -> Result<Vec<CalibrationOutcomeInfo>, StorageError> {
        get_unresolved_outcomes_q(self.txn).await
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        resolve_outcome_q(self.txn, outcome_id, actual_yes).await
    }
}
