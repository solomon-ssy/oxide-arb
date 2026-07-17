use crate::{
    postgres::{error, primitives},
    traits::RuntimeConfigVersionRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigApproval, NewRuntimeConfigVersion,
        RuntimeConfigActivationInfo, RuntimeConfigApprovalInfo, RuntimeConfigVersionInfo,
    },
    entities::{
        runtime_config_activation::{
            Column as ActivationColumn, Entity as ActivationEntity,
            Model as RuntimeConfigActivationModel,
        },
        runtime_config_approval,
        runtime_config_version::{
            Column as VersionColumn, Entity as VersionEntity, Model as RuntimeConfigVersionModel,
        },
    },
    enums::runtime_config::RuntimeConfigApprovalDecision,
    types::{
        ContentHash, RuntimeConfigActivationId, RuntimeConfigApprovalId, RuntimeConfigVersionId,
    },
};
use sea_orm::{
    ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
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
    let mut active = activation.into_active_model();
    active.activated_at = ActiveValue::Set(primitives::statement_timestamp(db).await?);
    ActivationEntity::insert(active)
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
    primitives::advisory_xact_lock(txn, RUNTIME_CONFIG_ACTIVATION_LOCK_KEY).await
}

pub(crate) struct ValidatedRuntimeConfigApproval {
    pub version: RuntimeConfigVersionModel,
    pub approval: runtime_config_approval::Model,
}

pub(crate) async fn validate_operator_approval(
    db: &impl ConnectionTrait,
    activation: &NewRuntimeConfigActivation,
    require_separation: bool,
) -> Result<ValidatedRuntimeConfigApproval, StorageError> {
    let approval_id = activation
        .runtime_config_approval_id
        .as_ref()
        .ok_or_else(|| {
            StorageError::invariant_violation(
                Some("runtime_config_activation"),
                "operator activation requires runtime-config approval evidence",
            )
        })?;
    let approval = runtime_config_approval::Entity::find_by_id(approval_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found("runtime_config_approval", approval_id))?;
    let version = VersionEntity::find_by_id(activation.runtime_config_version_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            StorageError::not_found(
                "runtime_config_version",
                &activation.runtime_config_version_id,
            )
        })?;
    let now = primitives::statement_timestamp(db).await?;
    if approval.runtime_config_version_id != version.runtime_config_version_id
        || approval.config_hash != version.config_hash
        || approval.decision != RuntimeConfigApprovalDecision::Approved
        || approval
            .expires_at
            .is_some_and(|expires_at| expires_at <= now)
    {
        return Err(StorageError::state_conflict(
            "runtime_config_approval",
            Some(approval_id),
            "approval is not valid for the exact runtime-config version/hash at activation time",
        ));
    }
    if require_separation && approval.decided_by == activation.activated_by {
        return Err(StorageError::state_conflict(
            "runtime_config_approval",
            Some(approval_id),
            "config approver and activation operator must be different users",
        ));
    }
    Ok(ValidatedRuntimeConfigApproval { version, approval })
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
    let row = ActivationEntity::find()
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::RuntimeConfigActivationId)
        .find_also_related(VersionEntity)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    activation_version(row)
}

async fn do_load_active_at(
    db: &impl ConnectionTrait,
    at: DateTime<Utc>,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    let row = ActivationEntity::find()
        .filter(ActivationColumn::ActivatedAt.lte(at))
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::RuntimeConfigActivationId)
        .find_also_related(VersionEntity)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    activation_version(row)
}

fn activation_version(
    row: Option<(
        RuntimeConfigActivationModel,
        Option<RuntimeConfigVersionModel>,
    )>,
) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
    match row {
        None => Ok(None),
        Some((_activation, Some(version))) => Ok(Some(version.into())),
        Some((activation, None)) => Err(StorageError::invariant_violation(
            Some(entity::RUNTIME_CONFIG_ACTIVATION),
            format!(
                "activation {} references a missing runtime-config version {}",
                activation.runtime_config_activation_id, activation.runtime_config_version_id
            ),
        )),
    }
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
    async fn record_approval(
        &self,
        approval: NewRuntimeConfigApproval,
    ) -> Result<RuntimeConfigApprovalInfo, StorageError> {
        runtime_config_approval::Entity::insert(approval.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn load_approval(
        &self,
        approval_id: &RuntimeConfigApprovalId,
    ) -> Result<Option<RuntimeConfigApprovalInfo>, StorageError> {
        runtime_config_approval::Entity::find_by_id(approval_id.clone())
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn list_valid_approvals(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigApprovalInfo>, StorageError> {
        let now = primitives::statement_timestamp(&self.db).await?;
        runtime_config_approval::Entity::find()
            .filter(
                runtime_config_approval::Column::Decision
                    .eq(RuntimeConfigApprovalDecision::Approved),
            )
            .filter(
                Condition::any()
                    .add(runtime_config_approval::Column::ExpiresAt.is_null())
                    .add(runtime_config_approval::Column::ExpiresAt.gt(now)),
            )
            .order_by_desc(runtime_config_approval::Column::DecidedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

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

    async fn activate_approved_version(
        &self,
        activation: NewRuntimeConfigActivation,
        require_approver_activator_separation: bool,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_activation_lock(&txn).await?;
        validate_operator_approval(&txn, &activation, require_approver_activator_separation)
            .await?;
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
    async fn record_approval(
        &self,
        approval: NewRuntimeConfigApproval,
    ) -> Result<RuntimeConfigApprovalInfo, StorageError> {
        runtime_config_approval::Entity::insert(approval.into_active_model())
            .exec_with_returning(self.txn)
            .await
            .map(Into::into)
            .map_err(StorageError::from)
    }

    async fn load_approval(
        &self,
        approval_id: &RuntimeConfigApprovalId,
    ) -> Result<Option<RuntimeConfigApprovalInfo>, StorageError> {
        runtime_config_approval::Entity::find_by_id(approval_id.clone())
            .one(self.txn)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn list_valid_approvals(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigApprovalInfo>, StorageError> {
        runtime_config_approval::Entity::find()
            .filter(
                runtime_config_approval::Column::Decision
                    .eq(RuntimeConfigApprovalDecision::Approved),
            )
            .filter(
                Condition::any()
                    .add(runtime_config_approval::Column::ExpiresAt.is_null())
                    .add(runtime_config_approval::Column::ExpiresAt.gt(Utc::now())),
            )
            .order_by_desc(runtime_config_approval::Column::DecidedAt)
            .limit(limit)
            .all(self.txn)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

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

    async fn activate_approved_version(
        &self,
        activation: NewRuntimeConfigActivation,
        require_approver_activator_separation: bool,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        acquire_activation_lock(self.txn).await?;
        validate_operator_approval(self.txn, &activation, require_approver_activator_separation)
            .await?;
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
