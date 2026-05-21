use crate::traits::AccountingRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::accounting_period::{self, ActiveModel, Column, Entity};
use sea_orm::sea_query::Expr;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

// ── helpers ──────────────────────────────────────────────────────────

async fn get_current_daily_q(
    db: &impl ConnectionTrait,
) -> Result<Option<accounting_period::Model>, StorageError> {
    let today = Utc::now().date_naive();
    Entity::find()
        .filter(Column::PeriodType.eq("daily"))
        .filter(Column::StartDate.eq(today))
        .filter(Column::Finalized.eq(false))
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn get_current_weekly_q(
    db: &impl ConnectionTrait,
) -> Result<Option<accounting_period::Model>, StorageError> {
    let today = Utc::now().date_naive();
    Entity::find()
        .filter(Column::PeriodType.eq("weekly"))
        .filter(Column::StartDate.lte(today))
        .filter(Column::EndDate.gte(today))
        .filter(Column::Finalized.eq(false))
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn create_period_q(
    db: &impl ConnectionTrait,
    period: ActiveModel,
) -> Result<accounting_period::Model, StorageError> {
    Entity::insert(period)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn update_period_q(
    db: &impl ConnectionTrait,
    period: ActiveModel,
) -> Result<accounting_period::Model, StorageError> {
    period.update(db).await.map_err(StorageError::from)
}

async fn finalize_period_q(db: &impl ConnectionTrait, period_id: &str) -> Result<(), StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::Finalized, Expr::value(true))
        .filter(Column::PeriodId.eq(period_id))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    if result.rows_affected == 0 {
        return Err(StorageError::NotFound {
            entity: "accounting_period",
            id: period_id.to_string(),
        });
    }
    Ok(())
}

async fn get_history_q(
    db: &impl ConnectionTrait,
    period_type: &str,
    limit: u64,
) -> Result<Vec<accounting_period::Model>, StorageError> {
    Entity::find()
        .filter(Column::PeriodType.eq(period_type))
        .order_by_desc(Column::StartDate)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
}

// ── connection-based impl ────────────────────────────────────────────

pub struct PgAccountingRepository {
    db: DatabaseConnection,
}

impl PgAccountingRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgAccountingRepositoryTxn<'_> {
        PgAccountingRepositoryTxn { txn }
    }
}

impl AccountingRepository for PgAccountingRepository {
    async fn get_current_daily(&self) -> Result<Option<accounting_period::Model>, StorageError> {
        get_current_daily_q(&self.db).await
    }

    async fn get_current_weekly(&self) -> Result<Option<accounting_period::Model>, StorageError> {
        get_current_weekly_q(&self.db).await
    }

    async fn create_period(
        &self,
        period: ActiveModel,
    ) -> Result<accounting_period::Model, StorageError> {
        create_period_q(&self.db, period).await
    }

    async fn update_period(
        &self,
        period: ActiveModel,
    ) -> Result<accounting_period::Model, StorageError> {
        update_period_q(&self.db, period).await
    }

    async fn finalize_period(&self, period_id: &str) -> Result<(), StorageError> {
        finalize_period_q(&self.db, period_id).await
    }

    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<accounting_period::Model>, StorageError> {
        get_history_q(&self.db, period_type, limit).await
    }
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgAccountingRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

impl AccountingRepository for PgAccountingRepositoryTxn<'_> {
    async fn get_current_daily(&self) -> Result<Option<accounting_period::Model>, StorageError> {
        get_current_daily_q(self.txn).await
    }

    async fn get_current_weekly(&self) -> Result<Option<accounting_period::Model>, StorageError> {
        get_current_weekly_q(self.txn).await
    }

    async fn create_period(
        &self,
        period: ActiveModel,
    ) -> Result<accounting_period::Model, StorageError> {
        create_period_q(self.txn, period).await
    }

    async fn update_period(
        &self,
        period: ActiveModel,
    ) -> Result<accounting_period::Model, StorageError> {
        update_period_q(self.txn, period).await
    }

    async fn finalize_period(&self, period_id: &str) -> Result<(), StorageError> {
        finalize_period_q(self.txn, period_id).await
    }

    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<accounting_period::Model>, StorageError> {
        get_history_q(self.txn, period_type, limit).await
    }
}
