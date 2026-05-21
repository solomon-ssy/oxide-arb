use crate::traits::CalibrationRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::calibration::{DurationBucket, PriceZone},
    entities::calibration::{
        self, ActiveModel as CalibActiveModel, Column as CalibColumn, Entity as CalibEntity,
    },
    entities::calibration_outcome::{
        self, ActiveModel as OutcomeActiveModel, Column as OutcomeColumn, Entity as OutcomeEntity,
    },
    enums::common::MarketCategory,
};
use sea_orm::sea_query::Expr;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

// ── helpers ──────────────────────────────────────────────────────────

async fn get_bucket_q(
    db: &impl ConnectionTrait,
    category: MarketCategory,
    price_zone: PriceZone,
    duration_bucket: DurationBucket,
) -> Result<Option<calibration::Model>, StorageError> {
    CalibEntity::find()
        .filter(CalibColumn::Category.eq(category))
        .filter(CalibColumn::PriceZone.eq(price_zone))
        .filter(CalibColumn::DurationBucket.eq(duration_bucket))
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn get_buckets_by_category_q(
    db: &impl ConnectionTrait,
    category: MarketCategory,
) -> Result<Vec<calibration::Model>, StorageError> {
    CalibEntity::find()
        .filter(CalibColumn::Category.eq(category))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn get_all_buckets_q(
    db: &impl ConnectionTrait,
) -> Result<Vec<calibration::Model>, StorageError> {
    CalibEntity::find()
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn insert_bucket_q(
    db: &impl ConnectionTrait,
    bucket: CalibActiveModel,
) -> Result<calibration::Model, StorageError> {
    CalibEntity::insert(bucket)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn update_bucket_q(
    db: &impl ConnectionTrait,
    bucket: CalibActiveModel,
) -> Result<calibration::Model, StorageError> {
    bucket.update(db).await.map_err(StorageError::from)
}

async fn record_outcome_q(
    db: &impl ConnectionTrait,
    outcome: OutcomeActiveModel,
) -> Result<(), StorageError> {
    OutcomeEntity::insert(outcome)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn get_unresolved_outcomes_q(
    db: &impl ConnectionTrait,
) -> Result<Vec<calibration_outcome::Model>, StorageError> {
    OutcomeEntity::find()
        .filter(OutcomeColumn::ActualYes.is_null())
        .all(db)
        .await
        .map_err(StorageError::from)
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

impl CalibrationRepository for PgCalibrationRepository {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<calibration::Model>, StorageError> {
        get_bucket_q(&self.db, category, price_zone, duration_bucket).await
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<calibration::Model>, StorageError> {
        get_buckets_by_category_q(&self.db, category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<calibration::Model>, StorageError> {
        get_all_buckets_q(&self.db).await
    }

    async fn insert_bucket(
        &self,
        bucket: CalibActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        insert_bucket_q(&self.db, bucket).await
    }

    async fn update_bucket(
        &self,
        bucket: CalibActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        update_bucket_q(&self.db, bucket).await
    }

    async fn record_outcome(&self, outcome: OutcomeActiveModel) -> Result<(), StorageError> {
        record_outcome_q(&self.db, outcome).await
    }

    async fn get_unresolved_outcomes(
        &self,
    ) -> Result<Vec<calibration_outcome::Model>, StorageError> {
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

impl CalibrationRepository for PgCalibrationRepositoryTxn<'_> {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<calibration::Model>, StorageError> {
        get_bucket_q(self.txn, category, price_zone, duration_bucket).await
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<calibration::Model>, StorageError> {
        get_buckets_by_category_q(self.txn, category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<calibration::Model>, StorageError> {
        get_all_buckets_q(self.txn).await
    }

    async fn insert_bucket(
        &self,
        bucket: CalibActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        insert_bucket_q(self.txn, bucket).await
    }

    async fn update_bucket(
        &self,
        bucket: CalibActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        update_bucket_q(self.txn, bucket).await
    }

    async fn record_outcome(&self, outcome: OutcomeActiveModel) -> Result<(), StorageError> {
        record_outcome_q(self.txn, outcome).await
    }

    async fn get_unresolved_outcomes(
        &self,
    ) -> Result<Vec<calibration_outcome::Model>, StorageError> {
        get_unresolved_outcomes_q(self.txn).await
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        resolve_outcome_q(self.txn, outcome_id, actual_yes).await
    }
}
