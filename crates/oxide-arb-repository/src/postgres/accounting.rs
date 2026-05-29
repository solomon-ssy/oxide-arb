use crate::traits::AccountingRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{AccountingPeriodInfo, AccountingPeriodPatch, NewAccountingPeriod},
    entities::accounting_period::{ActiveModel, Column, Entity},
    enums::common::ReportType,
    types::PeriodId,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, sea_query::Expr,
};

// ── helpers ──────────────────────────────────────────────────────────

async fn get_current_daily_q(
    db: &impl ConnectionTrait,
) -> Result<Option<AccountingPeriodInfo>, StorageError> {
    let today = Utc::now().date_naive();
    Entity::find()
        .filter(Column::PeriodType.eq(ReportType::Daily))
        .filter(Column::StartDate.eq(today))
        .filter(Column::Finalized.eq(false))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn get_current_weekly_q(
    db: &impl ConnectionTrait,
) -> Result<Option<AccountingPeriodInfo>, StorageError> {
    let today = Utc::now().date_naive();
    Entity::find()
        .filter(Column::PeriodType.eq(ReportType::Weekly))
        .filter(Column::StartDate.lte(today))
        .filter(Column::EndDate.gte(today))
        .filter(Column::Finalized.eq(false))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn create_q(
    db: &impl ConnectionTrait,
    period: NewAccountingPeriod,
) -> Result<AccountingPeriodInfo, StorageError> {
    let am: ActiveModel = period.into_active_model();
    let model = Entity::insert(am)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn update_q(
    db: &impl ConnectionTrait,
    period_id: &PeriodId,
    patch: AccountingPeriodPatch,
) -> Result<AccountingPeriodInfo, StorageError> {
    let models = Entity::update_many()
        .set(patch.into_active_model())
        .filter(Column::PeriodId.eq(period_id.clone()))
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;

    models
        .into_iter()
        .next()
        .map(Into::into)
        .ok_or_else(|| StorageError::NotFound {
            entity: "accounting_period",
            id: period_id.to_string(),
        })
}

async fn finalize_period_q(
    db: &impl ConnectionTrait,
    period_id: &PeriodId,
) -> Result<(), StorageError> {
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
) -> Result<Vec<AccountingPeriodInfo>, StorageError> {
    Entity::find()
        .filter(Column::PeriodType.eq(period_type))
        .order_by_desc(Column::StartDate)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
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

#[async_trait::async_trait]
impl AccountingRepository for PgAccountingRepository {
    async fn get_current_daily(&self) -> Result<Option<AccountingPeriodInfo>, StorageError> {
        get_current_daily_q(&self.db).await
    }

    async fn get_current_weekly(&self) -> Result<Option<AccountingPeriodInfo>, StorageError> {
        get_current_weekly_q(&self.db).await
    }

    async fn create(
        &self,
        period: NewAccountingPeriod,
    ) -> Result<AccountingPeriodInfo, StorageError> {
        create_q(&self.db, period).await
    }

    async fn update(
        &self,
        period_id: &PeriodId,
        patch: AccountingPeriodPatch,
    ) -> Result<AccountingPeriodInfo, StorageError> {
        update_q(&self.db, period_id, patch).await
    }

    async fn finalize_period(&self, period_id: &PeriodId) -> Result<(), StorageError> {
        finalize_period_q(&self.db, period_id).await
    }

    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<AccountingPeriodInfo>, StorageError> {
        get_history_q(&self.db, period_type, limit).await
    }
}

// ── transaction-based impl ───────────────────────────────────────────

pub struct PgAccountingRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

#[async_trait::async_trait]
impl AccountingRepository for PgAccountingRepositoryTxn<'_> {
    async fn get_current_daily(&self) -> Result<Option<AccountingPeriodInfo>, StorageError> {
        get_current_daily_q(self.txn).await
    }

    async fn get_current_weekly(&self) -> Result<Option<AccountingPeriodInfo>, StorageError> {
        get_current_weekly_q(self.txn).await
    }

    async fn create(
        &self,
        period: NewAccountingPeriod,
    ) -> Result<AccountingPeriodInfo, StorageError> {
        create_q(self.txn, period).await
    }

    async fn update(
        &self,
        period_id: &PeriodId,
        patch: AccountingPeriodPatch,
    ) -> Result<AccountingPeriodInfo, StorageError> {
        update_q(self.txn, period_id, patch).await
    }

    async fn finalize_period(&self, period_id: &PeriodId) -> Result<(), StorageError> {
        finalize_period_q(self.txn, period_id).await
    }

    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<AccountingPeriodInfo>, StorageError> {
        get_history_q(self.txn, period_type, limit).await
    }
}
