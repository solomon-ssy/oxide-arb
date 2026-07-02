//! Postgres implementation of [`SystemRuntimeStateRepository`].

use crate::traits::SystemRuntimeStateRepository;
use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{SystemRuntimeStateInfo, UpsertSystemRuntimeState},
    entities::system_runtime_state::{ActiveModel, Column, Entity},
    enums::quant::QuantRuntimeMode,
    schema::column,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel, sea_query::OnConflict};

const SINGLETON_ID: i32 = 1;

pub struct PgSystemRuntimeStateRepository {
    db: DatabaseConnection,
}

impl PgSystemRuntimeStateRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait]
impl SystemRuntimeStateRepository for PgSystemRuntimeStateRepository {
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError> {
        Entity::find_by_id(SINGLETON_ID)
            .one(&self.db)
            .await
            .map(|model| model.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn upsert_quant_runtime_mode(
        &self,
        mode: QuantRuntimeMode,
        changed_by: &str,
        reason: &str,
    ) -> Result<(), StorageError> {
        let active_model: ActiveModel = UpsertSystemRuntimeState {
            id: SINGLETON_ID,
            quant_runtime_mode: mode,
            changed_by: changed_by.to_owned(),
            reason: reason.to_owned(),
            changed_at: Utc::now(),
        }
        .into_active_model();
        Entity::insert(active_model)
            .on_conflict(
                OnConflict::column(Column::Id)
                    .update_columns([Column::ChangedBy, Column::Reason, Column::ChangedAt])
                    .values([(
                        Column::QuantRuntimeMode,
                        column::pg_enum_excluded::<QuantRuntimeMode>(Column::QuantRuntimeMode),
                    )])
                    .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
