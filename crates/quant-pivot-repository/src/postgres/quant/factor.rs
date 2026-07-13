//! Postgres-backed factor definition + value repository.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::FactorRepository,
};
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        FactorDefinitionInfo, FactorDefinitionListQuery, FactorValueInfo, NewFactorDefinition,
        NewFactorValue, PageWindow, Paginated,
    },
    entities::{quant_factor_definition, quant_factor_value},
    enums::quant::PublicationStatus,
    schema::column,
    types::{FactorDefinitionId, ModelRunId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseBackend,
    DatabaseConnection, DatabaseTransaction, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect, SqlErr, Statement, TransactionTrait,
};

/// Postgres-backed factor repository: immutable, content-addressed definition
/// revisions plus the insert-only factor-value ledger.
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
        let expected_id = FactorDefinitionId::from_definition_hash(&definition.definition_hash);
        if definition.factor_definition_id != expected_id {
            return Err(error::invariant_violation(
                Some(entity::QUANT_FACTOR),
                format!(
                    "factor definition id {} does not match definition hash {} (expected {})",
                    definition.factor_definition_id, definition.definition_hash, expected_id
                ),
            ));
        }
        if definition.status != PublicationStatus::Draft {
            return Err(error::invariant_violation(
                Some(entity::QUANT_FACTOR),
                "new factor-definition revisions must be registered as Draft",
            ));
        }

        match quant_factor_definition::Entity::insert(definition.clone().into_active_model())
            .exec_with_returning(&self.db)
            .await
        {
            Ok(row) => Ok(row.into()),
            Err(db_error)
                if matches!(
                    db_error.sql_err(),
                    Some(SqlErr::UniqueConstraintViolation(_))
                ) =>
            {
                let mut conflicts = quant_factor_definition::Entity::find()
                    .filter(
                        Condition::any()
                            .add(
                                quant_factor_definition::Column::FactorDefinitionId
                                    .eq(definition.factor_definition_id.clone()),
                            )
                            .add(
                                quant_factor_definition::Column::DefinitionHash
                                    .eq(definition.definition_hash.clone()),
                            ),
                    )
                    .all(&self.db)
                    .await
                    .map_err(StorageError::from)?;
                if conflicts.len() != 1 {
                    return Err(error::state_conflict(
                        entity::QUANT_FACTOR,
                        Some(&definition.factor_definition_id),
                        format!(
                            "content-addressed insert conflicted with {} revisions; expected exactly one",
                            conflicts.len()
                        ),
                    ));
                }
                let existing = conflicts.pop().ok_or_else(|| {
                    error::state_conflict(
                        entity::QUANT_FACTOR,
                        Some(&definition.factor_definition_id),
                        "content-addressed conflict row disappeared",
                    )
                })?;
                ensure_identical_definition(&existing, &definition)?;
                Ok(existing.into())
            }
            Err(db_error) => Err(StorageError::from(db_error)),
        }
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
        let mut rows =
            publish_definition_revisions(&self.db, std::slice::from_ref(factor_definition_id))
                .await?;
        rows.pop().ok_or_else(|| {
            error::invariant_violation(
                Some(entity::QUANT_FACTOR),
                "single factor publication returned no revision",
            )
        })
    }

    async fn publish_definitions(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
        publish_definition_revisions(&self.db, factor_definition_ids).await
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
            .order_by_desc(quant_factor_value::Column::DecisionAt)
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
            .filter(quant_factor_value::Column::DecisionAt.gte(from))
            .filter(quant_factor_value::Column::DecisionAt.lt(until))
            .order_by_asc(quant_factor_value::Column::DecisionAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

fn ensure_identical_definition(
    existing: &quant_factor_definition::Model,
    requested: &NewFactorDefinition,
) -> Result<(), StorageError> {
    let identical = existing.factor_definition_id == requested.factor_definition_id
        && existing.definition_hash == requested.definition_hash
        && existing.feature_contract_hash == requested.feature_contract_hash
        && existing.name == requested.name
        && existing.factor_family == requested.factor_family
        && existing.scope == requested.scope
        && existing.input_schema_version == requested.input_schema_version
        && existing.output_schema_version == requested.output_schema_version
        && existing.definition_json == requested.definition_json;
    if identical {
        return Ok(());
    }
    Err(error::invariant_violation(
        Some(entity::QUANT_FACTOR),
        format!(
            "factor definition content-address collision for id {} / hash {}",
            requested.factor_definition_id, requested.definition_hash
        ),
    ))
}

async fn publish_definition_revisions(
    db: &DatabaseConnection,
    factor_definition_ids: &[FactorDefinitionId],
) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
    if factor_definition_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_unique_publish_ids(factor_definition_ids)?;
    let txn = db.begin().await.map_err(StorageError::from)?;
    let initial = quant_factor_definition::Entity::find()
        .filter(
            quant_factor_definition::Column::FactorDefinitionId
                .is_in(factor_definition_ids.iter().cloned()),
        )
        .all(&txn)
        .await
        .map_err(StorageError::from)?;
    let by_id = definitions_by_id(initial);
    let names = logical_names_for_publish(factor_definition_ids, &by_id)?;
    // Sorting gives every concurrent multi-factor publication the same lock
    // order; the partial unique index remains the final invariant.
    for name in &names {
        acquire_factor_publication_lock(&txn, name).await?;
    }
    let locked = quant_factor_definition::Entity::find()
        .filter(
            quant_factor_definition::Column::FactorDefinitionId
                .is_in(factor_definition_ids.iter().cloned()),
        )
        .order_by_asc(quant_factor_definition::Column::Name)
        .lock_exclusive()
        .all(&txn)
        .await
        .map_err(StorageError::from)?;
    let locked_by_id = definitions_by_id(locked);
    validate_publish_transitions(factor_definition_ids, &locked_by_id)?;
    replace_published_revisions(&txn, factor_definition_ids, &locked_by_id).await?;
    let published = load_published_batch(&txn, factor_definition_ids).await?;
    txn.commit().await.map_err(StorageError::from)?;
    Ok(published)
}

fn validate_unique_publish_ids(
    factor_definition_ids: &[FactorDefinitionId],
) -> Result<(), StorageError> {
    let mut seen_ids = HashSet::with_capacity(factor_definition_ids.len());
    for id in factor_definition_ids {
        if !seen_ids.insert(id.clone()) {
            return Err(error::invariant_violation(
                Some(entity::QUANT_FACTOR),
                format!("duplicate factor definition id in publish batch: {id}"),
            ));
        }
    }
    Ok(())
}

fn definitions_by_id(
    definitions: Vec<quant_factor_definition::Model>,
) -> HashMap<FactorDefinitionId, quant_factor_definition::Model> {
    definitions
        .into_iter()
        .map(|row| (row.factor_definition_id.clone(), row))
        .collect()
}

fn logical_names_for_publish(
    factor_definition_ids: &[FactorDefinitionId],
    by_id: &HashMap<FactorDefinitionId, quant_factor_definition::Model>,
) -> Result<Vec<String>, StorageError> {
    for id in factor_definition_ids {
        if !by_id.contains_key(id) {
            return Err(error::not_found(entity::QUANT_FACTOR, id));
        }
    }

    let mut names: Vec<String> = factor_definition_ids
        .iter()
        .filter_map(|id| by_id.get(id).map(|row| row.name.clone()))
        .collect();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(error::invariant_violation(
            Some(entity::QUANT_FACTOR),
            "a publish batch cannot contain multiple revisions of the same logical factor",
        ));
    }
    Ok(names)
}

fn validate_publish_transitions(
    factor_definition_ids: &[FactorDefinitionId],
    locked_by_id: &HashMap<FactorDefinitionId, quant_factor_definition::Model>,
) -> Result<(), StorageError> {
    for id in factor_definition_ids {
        let row = locked_by_id
            .get(id)
            .ok_or_else(|| error::not_found(entity::QUANT_FACTOR, id))?;
        if row.status != PublicationStatus::Published
            && !row
                .status
                .allows_transition_to(PublicationStatus::Published)
        {
            return Err(error::illegal_transition(
                entity::QUANT_FACTOR,
                Some(id),
                row.status,
                PublicationStatus::Published,
            ));
        }
    }
    Ok(())
}

async fn replace_published_revisions(
    txn: &DatabaseTransaction,
    factor_definition_ids: &[FactorDefinitionId],
    locked_by_id: &HashMap<FactorDefinitionId, quant_factor_definition::Model>,
) -> Result<(), StorageError> {
    for id in factor_definition_ids {
        let target = locked_by_id
            .get(id)
            .ok_or_else(|| error::not_found(entity::QUANT_FACTOR, id))?;
        quant_factor_definition::Entity::update_many()
            .col_expr(
                quant_factor_definition::Column::Status,
                column::pg_enum_value(&PublicationStatus::Retired),
            )
            .filter(quant_factor_definition::Column::Name.eq(target.name.clone()))
            .filter(quant_factor_definition::Column::FactorDefinitionId.ne(id.clone()))
            .filter(quant_factor_definition::Column::Status.eq(PublicationStatus::Published))
            .exec(txn)
            .await
            .map_err(StorageError::from)?;

        if target.status != PublicationStatus::Published {
            let mut active = target.clone().into_active_model();
            active.status = ActiveValue::Set(PublicationStatus::Published);
            active.update(txn).await.map_err(StorageError::from)?;
        }
    }
    Ok(())
}

async fn load_published_batch(
    txn: &DatabaseTransaction,
    factor_definition_ids: &[FactorDefinitionId],
) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
    let refreshed = quant_factor_definition::Entity::find()
        .filter(
            quant_factor_definition::Column::FactorDefinitionId
                .is_in(factor_definition_ids.iter().cloned()),
        )
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    let mut refreshed_by_id: HashMap<FactorDefinitionId, FactorDefinitionInfo> = refreshed
        .into_iter()
        .map(|row| (row.factor_definition_id.clone(), row.into()))
        .collect();
    let published = factor_definition_ids
        .iter()
        .map(|id| {
            refreshed_by_id
                .remove(id)
                .ok_or_else(|| error::not_found(entity::QUANT_FACTOR, id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(published)
}

async fn acquire_factor_publication_lock(
    txn: &DatabaseTransaction,
    logical_name: &str,
) -> Result<(), StorageError> {
    txn.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 8609321192504036405))",
        [logical_name.into()],
    ))
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

async fn update_definition_status(
    db: &DatabaseConnection,
    factor_definition_id: &FactorDefinitionId,
    next: PublicationStatus,
) -> Result<FactorDefinitionInfo, StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;
    let Some(initial) = quant_factor_definition::Entity::find_by_id(factor_definition_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(entity::QUANT_FACTOR, factor_definition_id));
    };
    acquire_factor_publication_lock(&txn, &initial.name).await?;
    let Some(row) = quant_factor_definition::Entity::find_by_id(factor_definition_id.clone())
        .lock_exclusive()
        .one(&txn)
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
    let updated = active
        .update(&txn)
        .await
        .map_err(StorageError::from)
        .map(Into::into)?;
    txn.commit().await.map_err(StorageError::from)?;
    Ok(updated)
}
