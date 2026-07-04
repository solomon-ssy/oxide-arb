//! Postgres-backed factor definition + value repository.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::FactorRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        FactorDefinitionInfo, FactorDefinitionListQuery, FactorValueInfo, NewFactorDefinition,
        NewFactorValue, PageWindow, Paginated,
    },
    entities::{quant_factor_definition, quant_factor_value},
    enums::quant::PublicationStatus,
    types::{FactorDefinitionId, ModelRunId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, TransactionTrait, sea_query::OnConflict,
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
        // re-registration is a no-op upsert that refreshes governed metadata but
        // preserves publication status (Draft / Published / Retired).
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
        // Insert per row inside one transaction rather than a single multi-row
        // `insert_many`: sea-query's batched VALUES drops the Postgres enum cast
        // for the *nullable* native-enum columns (`normalization_source` /
        // `indeterminate_reason`), binding them as `text` and failing the insert.
        // A single-row insert carries the enum cast correctly; the transaction
        // keeps the ledger write atomic.
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let mut inserted = Vec::with_capacity(values.len());
        for value in values {
            let model = value
                .into_active_model()
                .insert(&txn)
                .await
                .map_err(StorageError::from)?;
            inserted.push(model.into());
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
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

    async fn find_definitions_by_ids(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
        if factor_definition_ids.is_empty() {
            return Ok(Vec::new());
        }
        quant_factor_definition::Entity::find()
            .filter(
                quant_factor_definition::Column::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().cloned()),
            )
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn page_definitions(
        &self,
        query: FactorDefinitionListQuery,
    ) -> Result<Paginated<FactorDefinitionInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .factor_family
                    .map(|family| quant_factor_definition::Column::FactorFamily.eq(family)),
            )
            .add_option(
                query
                    .scope
                    .map(|scope| quant_factor_definition::Column::Scope.eq(scope)),
            )
            .add_option(
                query
                    .status
                    .map(|status| quant_factor_definition::Column::Status.eq(status)),
            );
        paginate_mapped(
            quant_factor_definition::Entity::find()
                .filter(condition)
                .order_by_desc(quant_factor_definition::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn publish_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError> {
        update_definition_status(&self.db, factor_definition_id, PublicationStatus::Published).await
    }

    async fn retire_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError> {
        update_definition_status(&self.db, factor_definition_id, PublicationStatus::Retired).await
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

    async fn recent_values(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        from: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        if factor_definition_ids.is_empty() {
            return Ok(Vec::new());
        }
        quant_factor_value::Entity::find()
            .filter(
                quant_factor_value::Column::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().cloned()),
            )
            .filter(quant_factor_value::Column::AsOf.gte(from))
            .filter(quant_factor_value::Column::AsOf.lt(until))
            .order_by_asc(quant_factor_value::Column::AsOf)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

async fn update_definition_status(
    db: &DatabaseConnection,
    factor_definition_id: &FactorDefinitionId,
    next: PublicationStatus,
) -> Result<FactorDefinitionInfo, StorageError> {
    let Some(row) = quant_factor_definition::Entity::find_by_id(factor_definition_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(entity::QUANT_FACTOR, factor_definition_id));
    };
    let from = row.status;
    if !from.allows_transition_to(next) {
        return Err(error::illegal_transition(
            entity::QUANT_FACTOR,
            Some(factor_definition_id),
            from,
            next,
        ));
    }
    let mut active = row.into_active_model();
    active.status = ActiveValue::Set(next);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}
