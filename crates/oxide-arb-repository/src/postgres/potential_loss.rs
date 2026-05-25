use crate::traits::PotentialLossRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{NewPotentialLoss, PotentialLossInfo, UpdatePotentialLoss};
use oxide_arb_models::entities::potential_loss_ledger::{Column, Entity};
use oxide_arb_models::enums::common::LedgerStatus;
use oxide_arb_models::types::{LedgerId, MarketId, Usd};
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

async fn do_create(
    db: &impl ConnectionTrait,
    entry: NewPotentialLoss,
) -> Result<PotentialLossInfo, StorageError> {
    let am = entry.into_active_model();
    let model = Entity::insert(am)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(model.into())
}

async fn do_find_active(db: &impl ConnectionTrait) -> Result<Vec<PotentialLossInfo>, StorageError> {
    Entity::find()
        .filter(Column::Status.eq(LedgerStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_by_market(
    db: &impl ConnectionTrait,
    market_id: &MarketId,
) -> Result<Vec<PotentialLossInfo>, StorageError> {
    Entity::find()
        .filter(Column::MarketId.eq(market_id))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_update(
    db: &impl ConnectionTrait,
    ledger_id: &LedgerId,
) -> Result<PotentialLossInfo, StorageError> {
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
    let model = Entity::find_by_id(ledger_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "potential_loss_ledger",
            id: ledger_id.to_string(),
        })?;
    Ok(model.into())
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
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError> {
        do_create(&self.db, entry).await
    }

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_by_market(&self.db, market_id).await
    }

    async fn update(
        &self,
        ledger_id: &LedgerId,
        _update: UpdatePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError> {
        do_update(&self.db, ledger_id).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(&self.db).await
    }
}

impl PotentialLossRepository for PgPotentialLossRepositoryTxn<'_> {
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError> {
        do_create(self.txn, entry).await
    }

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_by_market(self.txn, market_id).await
    }

    async fn update(
        &self,
        ledger_id: &LedgerId,
        _update: UpdatePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError> {
        do_update(self.txn, ledger_id).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(self.txn).await
    }
}
