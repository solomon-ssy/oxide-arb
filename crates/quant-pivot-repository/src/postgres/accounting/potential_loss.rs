use crate::traits::PotentialLossRepository;
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewPotentialLoss, PotentialLossInfo, ResolvePotentialLoss},
    entities::potential_loss_ledger::{Column, Entity},
    enums::common::LedgerStatus,
    types::{LedgerId, MarketId, Usd},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DeriveIntoActiveModel,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};

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

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_pivot_models::entities::potential_loss_ledger::ActiveModel")]
struct ResolvePotentialLossPatch {
    status: LedgerStatus,
    resolved_at: Option<DateTime<Utc>>,
}

pub(crate) async fn do_create(
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
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::LedgerId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_find_active_as_of(
    db: &impl ConnectionTrait,
    at: DateTime<Utc>,
) -> Result<Vec<PotentialLossInfo>, StorageError> {
    Entity::find()
        .filter(Column::CreatedAt.lte(at))
        .filter(Column::ResolvedAt.is_null().or(Column::ResolvedAt.gt(at)))
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::LedgerId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn do_find_changed_between(
    db: &impl ConnectionTrait,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PotentialLossInfo>, StorageError> {
    Entity::find()
        .filter(
            Column::CreatedAt
                .gte(from)
                .and(Column::CreatedAt.lt(to))
                .or(Column::ResolvedAt.gte(from).and(Column::ResolvedAt.lt(to))),
        )
        .order_by_asc(Column::CreatedAt)
        .order_by_asc(Column::LedgerId)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
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
    command: ResolvePotentialLoss,
) -> Result<PotentialLossInfo, StorageError> {
    let patch = ResolvePotentialLossPatch {
        status: LedgerStatus::Resolved,
        resolved_at: Some(command.resolved_at),
    };
    let models = Entity::update_many()
        .set(patch.into_active_model())
        .filter(Column::LedgerId.eq(ledger_id))
        .filter(Column::Status.eq(LedgerStatus::Active))
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;

    models
        .into_iter()
        .next()
        .map(Into::into)
        .ok_or_else(|| StorageError::NotFound {
            entity: "potential_loss_ledger",
            id: ledger_id.to_string(),
        })
}

async fn do_total_active_loss(db: &impl ConnectionTrait) -> Result<Usd, StorageError> {
    let entries = Entity::find()
        .filter(Column::Status.eq(LedgerStatus::Active))
        .all(db)
        .await
        .map_err(StorageError::from)?;

    Ok(entries.iter().map(|e| e.max_loss_usd).sum())
}

#[async_trait::async_trait]
impl PotentialLossRepository for PgPotentialLossRepository {
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError> {
        do_create(&self.db, entry).await
    }

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active(&self.db).await
    }

    async fn find_active_as_of(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active_as_of(&self.db, at).await
    }

    async fn find_changed_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_changed_between(&self.db, from, to).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_by_market(&self.db, market_id).await
    }

    async fn resolve(
        &self,
        ledger_id: &LedgerId,
        command: ResolvePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError> {
        do_update(&self.db, ledger_id, command).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(&self.db).await
    }
}

#[async_trait::async_trait]
impl PotentialLossRepository for PgPotentialLossRepositoryTxn<'_> {
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError> {
        do_create(self.txn, entry).await
    }

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active(self.txn).await
    }

    async fn find_active_as_of(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_active_as_of(self.txn, at).await
    }

    async fn find_changed_between(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_changed_between(self.txn, from, to).await
    }

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError> {
        do_find_by_market(self.txn, market_id).await
    }

    async fn resolve(
        &self,
        ledger_id: &LedgerId,
        command: ResolvePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError> {
        do_update(self.txn, ledger_id, command).await
    }

    async fn total_active_loss(&self) -> Result<Usd, StorageError> {
        do_total_active_loss(self.txn).await
    }
}
