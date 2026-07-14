//! `PostgreSQL` WORM ledger for verified `ClickHouse` partition archives.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ArchivePartitionDropAuditInfo, ArchivePartitionManifestInfo, NewArchivePartitionManifest,
    },
    entities::{
        quant_archive_partition_drop_audit, quant_archive_partition_drop_command,
        quant_archive_partition_manifest,
    },
    hashing::CanonicalDigest,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType, OnConflict},
};
use uuid::Uuid;

use crate::traits::ArchivePartitionRepository;

const ENTITY_MANIFEST: &str = "quant_archive_partition_manifest";

pub struct PgArchivePartitionRepository {
    db: DatabaseConnection,
}

impl PgArchivePartitionRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ArchivePartitionRepository for PgArchivePartitionRepository {
    async fn find_manifest(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Result<Option<ArchivePartitionManifestInfo>, StorageError> {
        quant_archive_partition_manifest::Entity::find()
            .filter(quant_archive_partition_manifest::Column::TableName.eq(table_name))
            .filter(quant_archive_partition_manifest::Column::PartitionKey.eq(partition_key))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn seal_manifest(
        &self,
        manifest: NewArchivePartitionManifest,
    ) -> Result<ArchivePartitionManifestInfo, StorageError> {
        if manifest.row_count <= 0 || manifest.retention_days <= 0 {
            return Err(invariant(
                "archive manifest count and retention must be positive",
            ));
        }
        let expected_hash = CanonicalDigest::content_hash_json(&(
            &manifest.table_name,
            &manifest.partition_key,
            manifest.retention_days,
            manifest.row_count,
            &manifest.parquet_uri,
            &manifest.byte_hash,
            &manifest.content_hash,
            manifest.sealed_at,
        ))
        .map_err(|error| invariant(error.to_string()))?;
        if manifest.manifest_hash != expected_hash {
            return Err(invariant(
                "archive manifest hash does not match canonical fields",
            ));
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        quant_archive_partition_manifest::Entity::insert(manifest.clone().into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_archive_partition_manifest::Column::TableName,
                    quant_archive_partition_manifest::Column::PartitionKey,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        let stored = quant_archive_partition_manifest::Entity::find()
            .filter(quant_archive_partition_manifest::Column::TableName.eq(&manifest.table_name))
            .filter(
                quant_archive_partition_manifest::Column::PartitionKey.eq(&manifest.partition_key),
            )
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| invariant("sealed archive manifest disappeared"))?;
        if stored.row_count != manifest.row_count
            || stored.retention_days != manifest.retention_days
            || stored.parquet_uri != manifest.parquet_uri
            || stored.byte_hash != manifest.byte_hash
            || stored.content_hash != manifest.content_hash
        {
            return Err(invariant(
                "partition already has a different immutable archive manifest",
            ));
        }
        quant_archive_partition_drop_command::Entity::insert(
            quant_archive_partition_drop_command::ActiveModel {
                manifest_id: ActiveValue::Set(stored.manifest_id),
                claim_owner: ActiveValue::Set(None),
                lease_expires_at: ActiveValue::Set(None),
                attempts: ActiveValue::Set(0),
                last_error: ActiveValue::Set(None),
                completed_at: ActiveValue::Set(None),
                ..Default::default()
            },
        )
        .on_conflict(
            OnConflict::column(quant_archive_partition_drop_command::Column::ManifestId)
                .do_nothing()
                .to_owned(),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(stored.into())
    }

    async fn claim_pending_drop(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<ArchivePartitionManifestInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let command = quant_archive_partition_drop_command::Entity::find()
            .filter(quant_archive_partition_drop_command::Column::CompletedAt.is_null())
            .filter(
                Condition::any()
                    .add(quant_archive_partition_drop_command::Column::LeaseExpiresAt.is_null())
                    .add(quant_archive_partition_drop_command::Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(quant_archive_partition_drop_command::Column::CreatedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(command) = command else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let attempts = command
            .attempts
            .checked_add(1)
            .ok_or_else(|| invariant("archive drop attempt overflow"))?;
        let manifest_id = command.manifest_id;
        let mut active = command.into_active_model();
        active.claim_owner = ActiveValue::Set(Some(worker_id));
        active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
        active.attempts = ActiveValue::Set(attempts);
        active.update(&txn).await.map_err(StorageError::from)?;
        let manifest = quant_archive_partition_manifest::Entity::find_by_id(manifest_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| invariant("archive drop command has no sealed manifest"))?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(manifest.into()))
    }

    async fn find_drop_audit(
        &self,
        manifest_id: Uuid,
    ) -> Result<Option<ArchivePartitionDropAuditInfo>, StorageError> {
        quant_archive_partition_drop_audit::Entity::find()
            .filter(quant_archive_partition_drop_audit::Column::ManifestId.eq(manifest_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn complete_drop(
        &self,
        manifest_id: Uuid,
        worker_id: Uuid,
        dropped_at: DateTime<Utc>,
    ) -> Result<ArchivePartitionDropAuditInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let command = quant_archive_partition_drop_command::Entity::find_by_id(manifest_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_archive_partition_drop_command",
                id: manifest_id.to_string(),
            })?;
        if command.completed_at.is_some() {
            let audit = load_drop_audit(&txn, manifest_id).await?;
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(audit.into());
        }
        if command.claim_owner != Some(worker_id) {
            return Err(state_conflict(
                manifest_id,
                "archive drop claim owner no longer matches",
            ));
        }
        let audit = quant_archive_partition_drop_audit::ActiveModel {
            audit_id: ActiveValue::Set(Uuid::now_v7()),
            manifest_id: ActiveValue::Set(manifest_id),
            dropped_at: ActiveValue::Set(dropped_at),
            ..Default::default()
        }
        .insert(&txn)
        .await
        .map_err(StorageError::from)?;
        let mut active = command.into_active_model();
        active.completed_at = ActiveValue::Set(Some(dropped_at));
        active.claim_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        active.last_error = ActiveValue::Set(None);
        active.update(&txn).await.map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(audit.into())
    }

    async fn mark_drop_failed(
        &self,
        manifest_id: Uuid,
        worker_id: Uuid,
        detail: String,
    ) -> Result<(), StorageError> {
        let result = quant_archive_partition_drop_command::Entity::update_many()
            .col_expr(
                quant_archive_partition_drop_command::Column::ClaimOwner,
                Expr::value(Option::<Uuid>::None),
            )
            .col_expr(
                quant_archive_partition_drop_command::Column::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .col_expr(
                quant_archive_partition_drop_command::Column::LastError,
                Expr::value(Some(detail)),
            )
            .filter(quant_archive_partition_drop_command::Column::ManifestId.eq(manifest_id))
            .filter(quant_archive_partition_drop_command::Column::ClaimOwner.eq(worker_id))
            .filter(quant_archive_partition_drop_command::Column::CompletedAt.is_null())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected != 1 {
            return Err(state_conflict(
                manifest_id,
                "archive drop failure claim owner no longer matches",
            ));
        }
        Ok(())
    }
}

async fn load_drop_audit<C: sea_orm::ConnectionTrait>(
    db: &C,
    manifest_id: Uuid,
) -> Result<quant_archive_partition_drop_audit::Model, StorageError> {
    quant_archive_partition_drop_audit::Entity::find()
        .filter(quant_archive_partition_drop_audit::Column::ManifestId.eq(manifest_id))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| invariant("completed archive drop command has no WORM audit"))
}

fn invariant(detail: impl Into<String>) -> StorageError {
    StorageError::InvariantViolation {
        entity: Some(ENTITY_MANIFEST),
        detail: detail.into(),
    }
}

fn state_conflict(manifest_id: Uuid, detail: impl Into<String>) -> StorageError {
    StorageError::StateConflict {
        entity: "quant_archive_partition_drop_command",
        id: Some(manifest_id.to_string()),
        detail: detail.into(),
    }
}
