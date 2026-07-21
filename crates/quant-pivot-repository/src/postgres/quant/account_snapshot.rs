//! Postgres-backed account snapshot repository.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{AccountSnapshotInfo, NewAccountSnapshot},
    entities::quant_account_snapshot::Entity,
    types::AccountSnapshotId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

use crate::traits::AccountSnapshotRepository;

/// Postgres-backed account snapshot repository.
pub struct PgAccountSnapshotRepository {
    db: DatabaseConnection,
}

impl PgAccountSnapshotRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl AccountSnapshotRepository for PgAccountSnapshotRepository {
    async fn create(
        &self,
        snapshot: NewAccountSnapshot,
    ) -> Result<AccountSnapshotInfo, StorageError> {
        Entity::insert(snapshot.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        account_snapshot_id: &AccountSnapshotId,
    ) -> Result<Option<AccountSnapshotInfo>, StorageError> {
        Entity::find_by_id(account_snapshot_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
