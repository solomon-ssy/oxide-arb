//! Postgres-backed account snapshot repository.

use crate::traits::AccountSnapshotRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AccountSnapshotInfo, NewAccountSnapshot},
    entities::quant_account_snapshot,
    types::AccountSnapshotId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

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
        quant_account_snapshot::Entity::insert(snapshot.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        account_snapshot_id: &AccountSnapshotId,
    ) -> Result<Option<AccountSnapshotInfo>, StorageError> {
        quant_account_snapshot::Entity::find_by_id(account_snapshot_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}
