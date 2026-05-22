use crate::traits::LifecycleRepository;
use chrono::Utc;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    entities::lifecycle_event::{self, ActiveModel, Column, Entity},
    enums::lifecycle::LifecyclePhase,
};
#[allow(clippy::wildcard_imports)]
use sea_orm::*;

pub struct PgLifecycleRepository {
    db: DatabaseConnection,
}

impl PgLifecycleRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub const fn with_txn(txn: &DatabaseTransaction) -> PgLifecycleRepositoryTxn<'_> {
        PgLifecycleRepositoryTxn { txn }
    }
}

pub struct PgLifecycleRepositoryTxn<'a> {
    txn: &'a DatabaseTransaction,
}

async fn do_record(
    db: &impl ConnectionTrait,
    phase: LifecyclePhase,
    stage: Option<&str>,
    message: &str,
    metadata: Option<serde_json::Value>,
) -> Result<lifecycle_event::Model, StorageError> {
    let model = ActiveModel {
        id: NotSet,
        phase: Set(phase),
        stage: Set(stage.map(String::from)),
        message: Set(message.to_string()),
        metadata: Set(metadata),
        created_at: Set(Utc::now()),
    };

    Entity::insert(model)
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
}

async fn do_get_recent(
    db: &impl ConnectionTrait,
    limit: u64,
) -> Result<Vec<lifecycle_event::Model>, StorageError> {
    Entity::find()
        .order_by_desc(Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .map_err(StorageError::from)
}

impl LifecycleRepository for PgLifecycleRepository {
    async fn record(
        &self,
        phase: LifecyclePhase,
        stage: Option<&str>,
        message: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<lifecycle_event::Model, StorageError> {
        do_record(&self.db, phase, stage, message, metadata).await
    }

    async fn get_recent(&self, limit: u64) -> Result<Vec<lifecycle_event::Model>, StorageError> {
        do_get_recent(&self.db, limit).await
    }
}

impl LifecycleRepository for PgLifecycleRepositoryTxn<'_> {
    async fn record(
        &self,
        phase: LifecyclePhase,
        stage: Option<&str>,
        message: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<lifecycle_event::Model, StorageError> {
        do_record(self.txn, phase, stage, message, metadata).await
    }

    async fn get_recent(&self, limit: u64) -> Result<Vec<lifecycle_event::Model>, StorageError> {
        do_get_recent(self.txn, limit).await
    }
}
