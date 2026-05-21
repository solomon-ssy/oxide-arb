use crate::traits::PotentialLossRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::potential_loss_ledger::{self, ActiveModel, Column, Entity};
use oxide_arb_models::enums::common::LedgerStatus;
use oxide_arb_models::types::{MarketId, Usd};
use sea_orm::sea_query::Expr;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

pub struct PgPotentialLossRepository {
    db: DatabaseConnection,
}

impl PgPotentialLossRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgPotentialLossRepositoryTxn<'_> {
        PgPotentialLossRepositoryTxn { txn }
    }
}

pub struct PgPotentialLossRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_record(
    db: &impl ConnectionTrait,
    entry: ActiveModel,
) -> Result<potential_loss_ledger::Model, StorageError> {
    Entity::insert(entry)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_active(
    db: &impl ConnectionTrait,
) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(LedgerStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_find_by_market(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id.as_str()))
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_resolve(db: &impl ConnectionTrait, ledger_id: &str) -> Result<(), StorageError> {
    let result = Entity::update_many()
        .col_expr(Column::Status, Expr::value(LedgerStatus::Resolved))
        .col_expr(Column::ResolvedAt, Expr::value(Some(Utc::now())))
        .filter(Column::LedgerId.eq(ledger_id))
        .filter(Column::Status.eq(LedgerStatus::Active))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    if result.rows_affected == 0 {
        return Err(StorageError::NotFound {
            entity: "potential_loss_ledger",
            id: ledger_id.to_string(),
        });
    }
    Ok(())
}

async fn do_total_active_loss(db: &impl ConnectionTrait) -> Result<Usd, StorageError> {
    let entries = Entity::find()
        .filter(Column::Status.eq(LedgerStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(entries.iter().map(|e| e.max_loss_usd).sum())
}

impl PotentialLossRepository for PgPotentialLossRepository {
    async fn record(
        &self,
        entry: ActiveModel,
    ) -> Result<potential_loss_ledger::Model, StorageError> {
        do_record(&self.db, entry).await
    }

    async fn find_active(&self) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
        do_find_by_market(&self.db, market_id).await
    }

    async fn resolve(&self, ledger_id: &str) -> Result<(), StorageError> {
        do_resolve(&self.db, ledger_id).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(&self.db).await
    }
}

impl PotentialLossRepository for PgPotentialLossRepositoryTxn<'_> {
    async fn record(
        &self,
        entry: ActiveModel,
    ) -> Result<potential_loss_ledger::Model, StorageError> {
        do_record(self.txn, entry).await
    }

    async fn find_active(&self) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<potential_loss_ledger::Model>, StorageError> {
        do_find_by_market(self.txn, market_id).await
    }

    async fn resolve(&self, ledger_id: &str) -> Result<(), StorageError> {
        do_resolve(self.txn, ledger_id).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(self.txn).await
    }
}
