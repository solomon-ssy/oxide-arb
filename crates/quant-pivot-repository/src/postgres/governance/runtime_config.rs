use crate::{postgres::error, traits::RuntimeConfigVersionRepository};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo,
    },
    entities::{
        runtime_config_activation::{
            Column as ActivationColumn, Entity as ActivationEntity, Relation as ActivationRelation,
        },
        runtime_config_version::{Column as VersionColumn, Entity as VersionEntity},
    },
    types::{ContentHash, RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    Statement, TransactionTrait,
};

const RUNTIME_CONFIG_ACTIVATION_LOCK_KEY: i64 = 0x_11_06_52_43;

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
    let config_hash = version.config_hash.to_string();
    VersionEntity::insert(version.into_active_model())
        .exec_with_returning(db)
        .await
        .map(Into::into)
        .map_err(|err| error::map_unique(err, entity::RUNTIME_CONFIG_VERSION, &config_hash))
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

async fn do_load_current_activation(
    db: &impl ConnectionTrait,
) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
    ActivationEntity::find()
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::RuntimeConfigActivationId)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

pub(crate) async fn acquire_activation_lock(txn: &DatabaseTransaction) -> Result<(), StorageError> {
    txn.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT pg_advisory_xact_lock($1)",
        [RUNTIME_CONFIG_ACTIVATION_LOCK_KEY.into()],
    ))
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

async fn verify_current_activation(
    db: &impl ConnectionTrait,
    expected: Option<&RuntimeConfigActivationId>,
) -> Result<(), StorageError> {
    let current = do_load_current_activation(db).await?;
    if current
        .as_ref()
        .map(|row| &row.runtime_config_activation_id)
        != expected
    {
        return Err(StorageError::state_conflict(
            "runtime_config_activation",
            expected,
            format!(
                "runtime config activation generation changed; current is {}",
                current.as_ref().map_or_else(
                    || "<none>".to_owned(),
                    |row| row.runtime_config_activation_id.to_string(),
                )
            ),
        ));
    }
    Ok(())
}

pub(crate) async fn append_activation_if_current(
    txn: &DatabaseTransaction,
    expected: Option<&RuntimeConfigActivationId>,
    activation: Option<NewRuntimeConfigActivation>,
) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
    acquire_activation_lock(txn).await?;
    verify_current_activation(txn, expected).await?;
    match activation {
        Some(activation) => do_activate_version(txn, activation).await.map(Some),
        None => Ok(None),
    }
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
    config_hash: &ContentHash,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find()
        .filter(VersionColumn::ConfigHash.eq(config_hash))
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

pub(crate) async fn do_load_current(
    db: &impl ConnectionTrait,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find()
        .join_rev(JoinType::InnerJoin, ActivationRelation::Version.def())
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::RuntimeConfigActivationId)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn do_load_active_at(
    db: &impl ConnectionTrait,
    at: DateTime<Utc>,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find()
        .join_rev(JoinType::InnerJoin, ActivationRelation::Version.def())
        .filter(ActivationColumn::ActivatedAt.lte(at))
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::RuntimeConfigActivationId)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.map(Into::into))
}

async fn do_list_versions(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
    VersionEntity::find()
        .order_by_desc(VersionColumn::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|rows| rows.into_iter().map(Into::into).collect())
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
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_activation_lock(&txn).await?;
        let activated = do_activate_version(&txn, activation).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(activated)
    }

    async fn activate_version_if_current(
        &self,
        expected_current_activation_id: Option<&RuntimeConfigActivationId>,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let activated =
            append_activation_if_current(&txn, expected_current_activation_id, Some(activation))
                .await?
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("runtime_config_activation"),
                        "activation CAS returned no inserted activation",
                    )
                })?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(activated)
    }

    async fn load_current_activation(
        &self,
    ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
        do_load_current_activation(&self.db).await
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_version(&self.db, version_id).await
    }

    async fn load_by_hash(
        &self,
        config_hash: &ContentHash,
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

    async fn list_versions(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
        do_list_versions(&self.db, limit).await
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
        acquire_activation_lock(self.txn).await?;
        do_activate_version(self.txn, activation).await
    }

    async fn activate_version_if_current(
        &self,
        expected_current_activation_id: Option<&RuntimeConfigActivationId>,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        append_activation_if_current(self.txn, expected_current_activation_id, Some(activation))
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("runtime_config_activation"),
                    "activation CAS returned no inserted activation",
                )
            })
    }

    async fn load_current_activation(
        &self,
    ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
        do_load_current_activation(self.txn).await
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        do_load_version(self.txn, version_id).await
    }

    async fn load_by_hash(
        &self,
        config_hash: &ContentHash,
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

    async fn list_versions(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
        do_list_versions(self.txn, limit).await
    }

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        do_list_activations(self.txn, limit).await
    }
}
