//! Postgres-backed factor definition + value repository.

use crate::traits::FactorRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{FactorDefinitionInfo, FactorValueInfo, NewFactorDefinition, NewFactorValue},
    entities::{quant_factor_definition, quant_factor_value},
    types::{FactorDefinitionId, ModelRunId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    sea_query::OnConflict,
};

/// Postgres-backed factor repository: governed definitions (idempotent on the
/// stable definition id) plus the insert-only factor-value ledger.
pub struct PgFactorRepository {
    db: DatabaseConnection,
}

impl PgFactorRepository {
    /// Build a repository over a database connection.
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl FactorRepository for PgFactorRepository {
    async fn create_definition(
        &self,
        definition: NewFactorDefinition,
    ) -> Result<FactorDefinitionInfo, StorageError> {
        // Definition ids are deterministic (UUID v5 of the factor name), so a
        // re-registration is a no-op upsert that refreshes the governed metadata.
        quant_factor_definition::Entity::insert(definition.into_active_model())
            .on_conflict(
                OnConflict::column(quant_factor_definition::Column::FactorDefinitionId)
                    .update_columns([
                        quant_factor_definition::Column::Name,
                        quant_factor_definition::Column::FactorFamily,
                        quant_factor_definition::Column::Scope,
                        quant_factor_definition::Column::InputSchemaVersion,
                        quant_factor_definition::Column::OutputSchemaVersion,
                        quant_factor_definition::Column::DefinitionJson,
                        quant_factor_definition::Column::Status,
                        quant_factor_definition::Column::CreatedBy,
                    ])
                    .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn create_values(
        &self,
        values: Vec<NewFactorValue>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        // One `INSERT ... RETURNING` statement — atomic and a single round-trip.
        // Postgres returns rows in insertion order, so the result aligns with the
        // input.
        let models = quant_factor_value::Entity::insert_many(
            values.into_iter().map(IntoActiveModel::into_active_model),
        )
        .exec_with_returning_many(&self.db)
        .await
        .map_err(StorageError::from)?;
        Ok(models.into_iter().map(Into::into).collect())
    }

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError> {
        quant_factor_definition::Entity::find_by_id(factor_definition_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        quant_factor_value::Entity::find()
            .filter(quant_factor_value::Column::ModelRunId.eq(model_run_id.clone()))
            .order_by_desc(quant_factor_value::Column::AsOf)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
