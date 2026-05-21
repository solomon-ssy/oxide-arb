use crate::traits::RuntimeConfigRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::runtime_config::{
    self, ActiveModel, Column, Entity, RuntimeConfigKey,
};
use sea_orm::sea_query::OnConflict;
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

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
    key: &str,
) -> Result<Option<runtime_config::Model>, StorageError> {
    Entity::find_by_id(key.to_string())
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn do_set(
    db: &impl ConnectionTrait,
    key: &str,
    value: &serde_json::Value,
    updated_by: &str,
) -> Result<runtime_config::Model, StorageError> {
    let now = Utc::now();
    let model = ActiveModel {
        key: Set(key.to_string()),
        value: Set(value.clone()),
        description: NotSet,
        updated_by: Set(updated_by.to_string()),
        updated_at: Set(now),
    };

    Entity::insert(model)
        .on_conflict(
            OnConflict::column(Column::Key)
                .update_columns([Column::Value, Column::UpdatedBy, Column::UpdatedAt])
                .to_owned(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn do_get_all(db: &impl ConnectionTrait) -> Result<Vec<runtime_config::Model>, StorageError> {
    Entity::find()
        .order_by_asc(Column::Key)
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn do_delete(db: &impl ConnectionTrait, key: &str) -> Result<bool, StorageError> {
    let result = Entity::delete_by_id(key.to_string())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(result.rows_affected > 0)
}

impl RuntimeConfigRepository for PgRuntimeConfigRepository {
    async fn get(&self, key: &str) -> Result<Option<runtime_config::Model>, StorageError> {
        do_get(&self.db, key).await
    }

    async fn get_typed(
        &self,
        key: RuntimeConfigKey,
    ) -> Result<Option<runtime_config::Model>, StorageError> {
        do_get(&self.db, key.as_str()).await
    }

    async fn set(
        &self,
        key: &str,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        do_set(&self.db, key, value, updated_by).await
    }

    async fn set_typed(
        &self,
        key: RuntimeConfigKey,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        do_set(&self.db, key.as_str(), value, updated_by).await
    }

    async fn get_all(&self) -> Result<Vec<runtime_config::Model>, StorageError> {
        do_get_all(&self.db).await
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        do_delete(&self.db, key).await
    }
}

impl RuntimeConfigRepository for PgRuntimeConfigRepositoryTxn<'_> {
    async fn get(&self, key: &str) -> Result<Option<runtime_config::Model>, StorageError> {
        do_get(self.txn, key).await
    }

    async fn get_typed(
        &self,
        key: RuntimeConfigKey,
    ) -> Result<Option<runtime_config::Model>, StorageError> {
        do_get(self.txn, key.as_str()).await
    }

    async fn set(
        &self,
        key: &str,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        do_set(self.txn, key, value, updated_by).await
    }

    async fn set_typed(
        &self,
        key: RuntimeConfigKey,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        do_set(self.txn, key.as_str(), value, updated_by).await
    }

    async fn get_all(&self) -> Result<Vec<runtime_config::Model>, StorageError> {
        do_get_all(self.txn).await
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        do_delete(self.txn, key).await
    }
}
