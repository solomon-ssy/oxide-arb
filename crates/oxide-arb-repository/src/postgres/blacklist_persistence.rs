use crate::traits::BlacklistPersistenceRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{BlacklistInfo, UpsertBlacklistEntry};
use oxide_arb_models::entities::blacklist_entry::{Column, Entity};
use oxide_arb_models::types::MarketId;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

pub struct PgBlacklistPersistenceRepository {
    db: DatabaseConnection,
}

impl PgBlacklistPersistenceRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgBlacklistPersistenceRepositoryTxn<'_> {
        PgBlacklistPersistenceRepositoryTxn { txn }
    }
}

pub struct PgBlacklistPersistenceRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    entry: UpsertBlacklistEntry,
) -> Result<(), StorageError> {
    let am = entry.into_active_model();
    Entity::insert(am)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(Column::MarketId)
                .update_columns([
                    Column::TokenId,
                    Column::Scope,
                    Column::Reason,
                    Column::ExpiresAt,
                    Column::MissCount,
                    Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

async fn do_remove(db: &impl ConnectionTrait, market_id: &MarketId) -> Result<(), StorageError> {
    let result = Entity::delete_many()
        .filter(Column::MarketId.eq(market_id))
        .exec(db)
        .await
        .map_err(StorageError::from)?;

    if result.rows_affected == 0 {
        return Err(StorageError::NotFound {
            entity: "blacklist_entry",
            id: market_id.as_str().to_string(),
        });
    }
    Ok(())
}

async fn do_load_active(db: &impl ConnectionTrait) -> Result<Vec<BlacklistInfo>, StorageError> {
    Entity::find()
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

impl BlacklistPersistenceRepository for PgBlacklistPersistenceRepository {
    async fn upsert(&self, entry: UpsertBlacklistEntry) -> Result<(), StorageError> {
        do_upsert(&self.db, entry).await
    }

    async fn remove(&self, market_id: &MarketId) -> Result<(), StorageError> {
        do_remove(&self.db, market_id).await
    }

    async fn load_active(&self) -> Result<Vec<BlacklistInfo>, StorageError> {
        do_load_active(&self.db).await
    }
}

impl BlacklistPersistenceRepository for PgBlacklistPersistenceRepositoryTxn<'_> {
    async fn upsert(&self, entry: UpsertBlacklistEntry) -> Result<(), StorageError> {
        do_upsert(self.txn, entry).await
    }

    async fn remove(&self, market_id: &MarketId) -> Result<(), StorageError> {
        do_remove(self.txn, market_id).await
    }

    async fn load_active(&self) -> Result<Vec<BlacklistInfo>, StorageError> {
        do_load_active(self.txn).await
    }
}
