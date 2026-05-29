use crate::traits::RuntimeConfigRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{RuntimeConfigInfo, UpsertRuntimeConfig},
    entities::runtime_config::{Column, Entity},
    enums::runtime_config::RuntimeConfigKey,
};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait, IntoActiveModel,
    QueryOrder, sea_query::OnConflict,
};

pub struct PgRuntimeConfigRepository {
    db: DatabaseConnection,
}

impl PgRuntimeConfigRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgRuntimeConfigRepositoryTxn<'_> {
        PgRuntimeConfigRepositoryTxn { txn }
    }
}

pub struct PgRuntimeConfigRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_get(
    db: &impl ConnectionTrait,
    key: RuntimeConfigKey,
) -> Result<Option<RuntimeConfigInfo>, StorageError> {
    Entity::find_by_id(key)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|opt| opt.map(Into::into))
}

async fn do_upsert(
    db: &impl ConnectionTrait,
    dto: UpsertRuntimeConfig,
) -> Result<RuntimeConfigInfo, StorageError> {
    let result = Entity::insert(dto.into_active_model())
        .on_conflict(
            OnConflict::column(Column::Key)
                .update_columns([Column::Value, Column::UpdatedBy])
                .to_owned(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.into())
}

async fn do_get_all(db: &impl ConnectionTrait) -> Result<Vec<RuntimeConfigInfo>, StorageError> {
    Entity::find()
        .order_by_asc(Column::Key)
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|v| v.into_iter().map(Into::into).collect())
}

async fn do_delete(db: &impl ConnectionTrait, key: RuntimeConfigKey) -> Result<bool, StorageError> {
    let result = Entity::delete_by_id(key)
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
}

#[async_trait::async_trait]
impl RuntimeConfigRepository for PgRuntimeConfigRepository {
    async fn get(&self, key: RuntimeConfigKey) -> Result<Option<RuntimeConfigInfo>, StorageError> {
        do_get(&self.db, key).await
    }

    async fn upsert(&self, dto: UpsertRuntimeConfig) -> Result<RuntimeConfigInfo, StorageError> {
        do_upsert(&self.db, dto).await
    }

    async fn get_all(&self) -> Result<Vec<RuntimeConfigInfo>, StorageError> {
        do_get_all(&self.db).await
    }

    async fn delete(&self, key: RuntimeConfigKey) -> Result<bool, StorageError> {
        do_delete(&self.db, key).await
    }
}

#[async_trait::async_trait]
impl RuntimeConfigRepository for PgRuntimeConfigRepositoryTxn<'_> {
    async fn get(&self, key: RuntimeConfigKey) -> Result<Option<RuntimeConfigInfo>, StorageError> {
        do_get(self.txn, key).await
    }

    async fn upsert(&self, dto: UpsertRuntimeConfig) -> Result<RuntimeConfigInfo, StorageError> {
        do_upsert(self.txn, dto).await
    }

    async fn get_all(&self) -> Result<Vec<RuntimeConfigInfo>, StorageError> {
        do_get_all(self.txn).await
    }

    async fn delete(&self, key: RuntimeConfigKey) -> Result<bool, StorageError> {
        do_delete(self.txn, key).await
    }
}
