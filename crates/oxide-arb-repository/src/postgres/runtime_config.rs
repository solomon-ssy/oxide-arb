use crate::{
    postgres::control_factor::append_audit_event_chained_q, traits::RuntimeConfigVersionRepository,
};
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo, control_factor::NewControlFactorAuditEvent,
    },
    entities::{
        runtime_config_activation::{Column as ActivationColumn, Entity as ActivationEntity},
        runtime_config_version::{Column as VersionColumn, Entity as VersionEntity},
    },
    types::RuntimeConfigVersionId,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

pub struct PgRuntimeConfigVersionRepository {
    db: DatabaseConnection,
}

impl PgRuntimeConfigVersionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgRuntimeConfigVersionRepositoryTxn<'_> {
        PgRuntimeConfigVersionRepositoryTxn { txn }
    }
}

pub struct PgRuntimeConfigVersionRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_create_version(
    db: &impl ConnectionTrait,
    version: NewRuntimeConfigVersion,
) -> Result<RuntimeConfigVersionInfo, StorageError> {
    VersionEntity::insert(version.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn do_activate_version(
    db: &impl ConnectionTrait,
    activation: NewRuntimeConfigActivation,
) -> Result<RuntimeConfigActivationInfo, StorageError> {
    ActivationEntity::insert(activation.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(StorageError::from)
}

async fn do_create_version_governed(
    db: &impl ConnectionTrait,
    version: NewRuntimeConfigVersion,
    audit: NewControlFactorAuditEvent,
) -> Result<RuntimeConfigVersionInfo, StorageError> {
    let info = do_create_version(db, version).await?;
    append_audit_event_chained_q(db, audit, Utc::now()).await?;
    Ok(info)
}

async fn do_activate_version_governed(
    db: &impl ConnectionTrait,
    mut activation: NewRuntimeConfigActivation,
    audit: NewControlFactorAuditEvent,
) -> Result<RuntimeConfigActivationInfo, StorageError> {
    let event = append_audit_event_chained_q(db, audit, Utc::now()).await?;
    activation.audit_event_id = Some(event.event_id);
    do_activate_version(db, activation).await
}

async fn do_load_version(
    db: &impl ConnectionTrait,
    version_id: &RuntimeConfigVersionId,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find_by_id(version_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn do_load_by_hash(
    db: &impl ConnectionTrait,
    config_hash: &str,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find()
        .filter(VersionColumn::ConfigHash.eq(config_hash))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn do_load_current(
    db: &impl ConnectionTrait,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    let activation = ActivationEntity::find()
        .order_by_desc(ActivationColumn::ActivatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match activation {
        Some(row) => do_load_version(db, &row.runtime_config_version_id).await,
        None => Ok(None),
    }
}

#[async_trait::async_trait]
impl RuntimeConfigVersionRepository for PgRuntimeConfigVersionRepository {
    async fn create_version(
        &self,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        do_create_version(&self.db, version).await
    }

    async fn activate_version(
        &self,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        do_activate_version(&self.db, activation).await
    }

    async fn create_version_governed(
        &self,
        version: NewRuntimeConfigVersion,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info = do_create_version_governed(&txn, version, audit).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn activate_version_governed(
        &self,
        activation: NewRuntimeConfigActivation,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info = do_activate_version_governed(&txn, activation, audit).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_version(&self.db, version_id).await
    }

    async fn load_by_hash(
        &self,
        config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_by_hash(&self.db, config_hash).await
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_current(&self.db).await
    }

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_active_at(&self.db, at).await
    }

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        do_list_activations(&self.db, limit).await
    }
}

#[async_trait::async_trait]
impl RuntimeConfigVersionRepository for PgRuntimeConfigVersionRepositoryTxn<'_> {
    async fn create_version(
        &self,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        do_create_version(self.txn, version).await
    }

    async fn activate_version(
        &self,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        do_activate_version(self.txn, activation).await
    }

    async fn create_version_governed(
        &self,
        version: NewRuntimeConfigVersion,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        do_create_version_governed(self.txn, version, audit).await
    }

    async fn activate_version_governed(
        &self,
        activation: NewRuntimeConfigActivation,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        do_activate_version_governed(self.txn, activation, audit).await
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_version(self.txn, version_id).await
    }

    async fn load_by_hash(
        &self,
        config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_by_hash(self.txn, config_hash).await
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_current(self.txn).await
    }

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_active_at(self.txn, at).await
    }

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        do_list_activations(self.txn, limit).await
    }
}

async fn do_load_active_at(
    db: &impl ConnectionTrait,
    at: DateTime<Utc>,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    let activation = ActivationEntity::find()
        .filter(ActivationColumn::ActivatedAt.lte(at))
        .order_by_desc(ActivationColumn::ActivatedAt)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match activation {
        Some(row) => do_load_version(db, &row.runtime_config_version_id).await,
        None => Ok(None),
    }
}

async fn do_list_activations(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
    ActivationEntity::find()
        .order_by_desc(ActivationColumn::ActivatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
}
