//! Postgres-backed factor definition + value repository.

use std::{
    collections::{HashMap, HashSet},
    slice,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_FACTOR};
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        pagination::{PageWindow, Paginated},
        quant::{
            FactorDefinitionInfo, FactorValueInfo, LatestFactorSnapshotBundleInfo,
            LatestFactorSnapshotInfo, LatestFactorSnapshotValueInfo, NewFactorDefinition,
            NewFactorValue,
        },
    },
    entities::{
        quant_factor_definition::{Column, Entity, Model as QuantFactorDefinitionModel},
        quant_factor_value::{
            Column as QuantFactorValueColumn, Entity as QuantFactorValueEntity, Model,
        },
        quant_model_run::{Column as QuantModelRunColumn, Entity as QuantModelRunEntity},
    },
    enums::{factor::FactorValueState, quant::PublicationStatus},
    hashing::CanonicalDigest,
    types::{
        FactorDefinitionId, MarketId, ModelRunId, ModelVersionId,
        factor::factor_definition_content_hash,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, DatabaseTransaction,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, SqlErr, TransactionTrait,
};

use crate::{
    postgres::{
        error, primitives, quant::condition_wake::notify_input_change, query::paginate_mapped,
    },
    traits::FactorRepository,
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
        validate_new_definition(&definition)?;

        match Entity::insert(definition.clone().into_active_model())
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
                let mut conflicts = Entity::find()
                    .filter(
                        Condition::any()
                            .add(
                                Column::FactorDefinitionId
                                    .eq(definition.factor_definition_id.clone()),
                            )
                            .add(Column::DefinitionHash.eq(definition.definition_hash.clone())),
                    )
                    .all(&self.db)
                    .await
                    .map_err(StorageError::from)?;
                if conflicts.len() != 1 {
                    return Err(error::state_conflict(
                        QUANT_FACTOR,
                        Some(&definition.factor_definition_id),
                        format!(
                            "content-addressed insert conflicted with {} revisions; expected exactly one",
                            conflicts.len()
                        ),
                    ));
                }
                let existing = conflicts.pop().ok_or_else(|| {
                    error::state_conflict(
                        QUANT_FACTOR,
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
        notify_input_change(&txn, "factor").await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError> {
        Entity::find_by_id(factor_definition_id.clone())
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
        Entity::find()
            .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().cloned()))
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
                    .map(|family| Column::FactorFamily.eq(family)),
            )
            .add_option(query.scope.map(|scope| Column::Scope.eq(scope)))
            .add_option(query.status.map(|status| Column::Status.eq(status)));
        paginate_mapped(
            Entity::find()
                .filter(condition)
                .order_by_desc(Column::CreatedAt),
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
            publish_definition_revisions(&self.db, slice::from_ref(factor_definition_id)).await?;
        rows.pop().ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_FACTOR),
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
        QuantFactorValueEntity::find()
            .filter(QuantFactorValueColumn::ModelRunId.eq(model_run_id.clone()))
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
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
        QuantFactorValueEntity::find()
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().cloned()),
            )
            .filter(QuantFactorValueColumn::DecisionAt.gte(from))
            .filter(QuantFactorValueColumn::DecisionAt.lt(until))
            .order_by_asc(QuantFactorValueColumn::DecisionAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn latest_snapshot(
        &self,
        factor_definition_id: &FactorDefinitionId,
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<LatestFactorSnapshotInfo>, StorageError> {
        let row = QuantFactorValueEntity::find()
            .find_also_related(QuantModelRunEntity)
            .filter(QuantFactorValueColumn::FactorDefinitionId.eq(factor_definition_id.clone()))
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(QuantFactorValueColumn::ValueState.eq(FactorValueState::Scored))
            .filter(QuantModelRunColumn::ModelVersionId.eq(model_version_id.clone()))
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        let Some((value, Some(run))) = row else {
            return Ok(None);
        };
        let Some(raw_value) = value.raw_value else {
            return Ok(None);
        };
        let Some(normalized_score) = value.normalized_score else {
            return Ok(None);
        };
        let definition = Entity::find_by_id(factor_definition_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: QUANT_FACTOR,
                id: factor_definition_id.to_string(),
            })?;
        let snapshot_hash = CanonicalDigest::content_hash_json(&(
            &value.factor_value_id,
            &definition.definition_hash,
            &run.model_version_id,
            &value.market_id,
            raw_value,
            normalized_score,
            value.confidence,
            value.decision_at,
            value.created_at,
        ))
        .map_err(|error| {
            error::invariant_violation(
                Some(QUANT_FACTOR),
                format!("latest factor snapshot hash failed: {error}"),
            )
        })?;
        Ok(Some(LatestFactorSnapshotInfo {
            factor_value_id: value.factor_value_id,
            factor_definition_id: value.factor_definition_id,
            definition_hash: definition.definition_hash,
            model_version_id: model_version_id.clone(),
            market_id: value.market_id,
            raw_value,
            normalized_value: normalized_score.inner(),
            confidence: value.confidence.inner(),
            observed_at: value.decision_at,
            available_at: value.created_at,
            snapshot_hash,
        }))
    }

    async fn latest_snapshot_bundle(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<LatestFactorSnapshotBundleInfo>, StorageError> {
        if factor_definition_ids.is_empty() {
            return Err(error::invariant_violation(
                Some(QUANT_FACTOR),
                "latest factor snapshot bundle requires at least one definition",
            ));
        }
        let requested = factor_definition_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        if requested.len() != factor_definition_ids.len() {
            return Err(error::invariant_violation(
                Some(QUANT_FACTOR),
                "latest factor snapshot bundle contains duplicate definition ids",
            ));
        }

        let latest = QuantFactorValueEntity::find()
            .find_also_related(QuantModelRunEntity)
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().cloned()),
            )
            .filter(QuantModelRunColumn::ModelVersionId.eq(model_version_id.clone()))
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
            .order_by_desc(QuantFactorValueColumn::CreatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        let Some((latest_value, Some(run))) = latest else {
            return Ok(None);
        };

        let rows = QuantFactorValueEntity::find()
            .filter(QuantFactorValueColumn::ModelRunId.eq(run.model_run_id.clone()))
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().cloned()),
            )
            .order_by_asc(QuantFactorValueColumn::FactorDefinitionId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let found = rows
            .iter()
            .map(|row| row.factor_definition_id.clone())
            .collect::<HashSet<_>>();
        if found != requested || rows.len() != requested.len() {
            return Ok(None);
        }
        if rows
            .iter()
            .any(|row| row.decision_at != latest_value.decision_at)
        {
            return Err(error::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "model run {} contains mixed decision timestamps for market {}",
                    run.model_run_id, market_id
                ),
            ));
        }

        let available_at = rows.iter().map(|row| row.created_at).max().ok_or_else(|| {
            error::invariant_violation(
                Some(QUANT_FACTOR),
                "factor snapshot bundle unexpectedly has no values",
            )
        })?;
        let values = project_snapshot_values(
            &self.db,
            factor_definition_ids,
            market_id,
            rows,
            requested.len(),
        )
        .await?;
        let snapshot_hash = CanonicalDigest::content_hash_json(&(
            &run.model_run_id,
            model_version_id,
            market_id,
            latest_value.decision_at,
            available_at,
            &values,
        ))
        .map_err(|error| {
            error::invariant_violation(
                Some(QUANT_FACTOR),
                format!("latest factor snapshot bundle hash failed: {error}"),
            )
        })?;
        Ok(Some(LatestFactorSnapshotBundleInfo {
            model_run_id: run.model_run_id,
            model_version_id: model_version_id.clone(),
            market_id: market_id.clone(),
            observed_at: latest_value.decision_at,
            available_at,
            values,
            snapshot_hash,
        }))
    }
}

fn validate_new_definition(definition: &NewFactorDefinition) -> Result<(), StorageError> {
    if definition.name != definition.definition.name.as_str() {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "factor definition name projection does not match the typed document",
        ));
    }
    if definition.factor_family != definition.definition.family {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "factor family projection does not match the typed document",
        ));
    }
    if definition.scope != definition.factor_family.definition_scope() {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "factor scope does not match the factor family",
        ));
    }
    if definition.definition.owner.trim().is_empty() {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "factor definition owner must be non-empty",
        ));
    }
    let expected_hash =
        factor_definition_content_hash(&definition.definition, &definition.feature_contract_hash)
            .map_err(|hash_error| {
            error::invariant_violation(
                Some(QUANT_FACTOR),
                format!("factor definition cannot be canonically hashed: {hash_error}"),
            )
        })?;
    if definition.definition_hash != expected_hash {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            format!(
                "factor definition hash {} does not match canonical content hash {}",
                definition.definition_hash, expected_hash
            ),
        ));
    }
    let expected_id = FactorDefinitionId::from_definition_hash(&expected_hash);
    if definition.factor_definition_id != expected_id {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            format!(
                "factor definition id {} does not match definition hash {} (expected {})",
                definition.factor_definition_id, definition.definition_hash, expected_id
            ),
        ));
    }
    if definition.status != PublicationStatus::Draft {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "new factor-definition revisions must be registered as Draft",
        ));
    }
    Ok(())
}

async fn project_snapshot_values(
    db: &DatabaseConnection,
    factor_definition_ids: &[FactorDefinitionId],
    market_id: &MarketId,
    rows: Vec<Model>,
    expected_definition_count: usize,
) -> Result<Vec<LatestFactorSnapshotValueInfo>, StorageError> {
    let definitions = Entity::find()
        .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().cloned()))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|definition| (definition.factor_definition_id.clone(), definition))
        .collect::<HashMap<_, _>>();
    if definitions.len() != expected_definition_count {
        return Err(error::state_conflict(
            QUANT_FACTOR,
            Some(market_id),
            "factor snapshot bundle references a missing definition revision",
        ));
    }
    rows.into_iter()
        .map(|row| {
            let definition = definitions.get(&row.factor_definition_id).ok_or_else(|| {
                error::state_conflict(
                    QUANT_FACTOR,
                    Some(&row.factor_definition_id),
                    "factor snapshot definition disappeared",
                )
            })?;
            Ok(LatestFactorSnapshotValueInfo {
                factor_value_id: row.factor_value_id,
                factor_definition_id: row.factor_definition_id,
                definition_hash: definition.definition_hash.clone(),
                name: definition.name.clone(),
                family: definition.factor_family,
                value_state: row.value_state,
                raw_value: row.raw_value,
                normalized_score: row.normalized_score,
                normalization_source: row.normalization_source,
                indeterminate_reason: row.indeterminate_reason,
                direction: row.direction,
                confidence: row.confidence,
                explanation: row.explanation,
            })
        })
        .collect()
}

fn ensure_identical_definition(
    existing: &QuantFactorDefinitionModel,
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
        && existing.definition == requested.definition;
    if identical {
        return Ok(());
    }
    Err(error::invariant_violation(
        Some(QUANT_FACTOR),
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
    let initial = Entity::find()
        .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().cloned()))
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
    let locked = Entity::find()
        .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().cloned()))
        .order_by_asc(Column::Name)
        .lock_exclusive()
        .all(&txn)
        .await
        .map_err(StorageError::from)?;
    let locked_by_id = definitions_by_id(locked);
    validate_publish_transitions(factor_definition_ids, &locked_by_id)?;
    let newly_published_ids = factor_definition_ids
        .iter()
        .filter(|id| {
            locked_by_id
                .get(*id)
                .is_some_and(|row| row.status != PublicationStatus::Published)
        })
        .cloned()
        .collect::<Vec<_>>();
    replace_published_revisions(&txn, factor_definition_ids, &locked_by_id).await?;
    let published = load_published_batch(&txn, &newly_published_ids).await?;
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
                Some(QUANT_FACTOR),
                format!("duplicate factor definition id in publish batch: {id}"),
            ));
        }
    }
    Ok(())
}

fn definitions_by_id(
    definitions: Vec<QuantFactorDefinitionModel>,
) -> HashMap<FactorDefinitionId, QuantFactorDefinitionModel> {
    definitions
        .into_iter()
        .map(|row| (row.factor_definition_id.clone(), row))
        .collect()
}

fn logical_names_for_publish(
    factor_definition_ids: &[FactorDefinitionId],
    by_id: &HashMap<FactorDefinitionId, QuantFactorDefinitionModel>,
) -> Result<Vec<String>, StorageError> {
    for id in factor_definition_ids {
        if !by_id.contains_key(id) {
            return Err(error::not_found(QUANT_FACTOR, id));
        }
    }

    let mut names: Vec<String> = factor_definition_ids
        .iter()
        .filter_map(|id| by_id.get(id).map(|row| row.name.clone()))
        .collect();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(error::invariant_violation(
            Some(QUANT_FACTOR),
            "a publish batch cannot contain multiple revisions of the same logical factor",
        ));
    }
    Ok(names)
}

fn validate_publish_transitions(
    factor_definition_ids: &[FactorDefinitionId],
    locked_by_id: &HashMap<FactorDefinitionId, QuantFactorDefinitionModel>,
) -> Result<(), StorageError> {
    for id in factor_definition_ids {
        let row = locked_by_id
            .get(id)
            .ok_or_else(|| error::not_found(QUANT_FACTOR, id))?;
        if row.status != PublicationStatus::Published
            && !row
                .status
                .allows_transition_to(PublicationStatus::Published)
        {
            return Err(error::illegal_transition(
                QUANT_FACTOR,
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
    locked_by_id: &HashMap<FactorDefinitionId, QuantFactorDefinitionModel>,
) -> Result<(), StorageError> {
    for id in factor_definition_ids {
        let target = locked_by_id
            .get(id)
            .ok_or_else(|| error::not_found(QUANT_FACTOR, id))?;
        Entity::update_many()
            .col_expr(
                Column::Status,
                primitives::enum_value(&PublicationStatus::Retired),
            )
            .filter(Column::Name.eq(target.name.clone()))
            .filter(Column::FactorDefinitionId.ne(id.clone()))
            .filter(Column::Status.eq(PublicationStatus::Published))
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
    let refreshed = Entity::find()
        .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().cloned()))
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
                .ok_or_else(|| error::not_found(QUANT_FACTOR, id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(published)
}

async fn acquire_factor_publication_lock(
    txn: &DatabaseTransaction,
    logical_name: &str,
) -> Result<(), StorageError> {
    primitives::advisory_text_xact_lock(txn, logical_name, 8_609_321_192_504_036_405).await
}

async fn update_definition_status(
    db: &DatabaseConnection,
    factor_definition_id: &FactorDefinitionId,
    next: PublicationStatus,
) -> Result<FactorDefinitionInfo, StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;
    let Some(initial) = Entity::find_by_id(factor_definition_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(QUANT_FACTOR, factor_definition_id));
    };
    acquire_factor_publication_lock(&txn, &initial.name).await?;
    let Some(row) = Entity::find_by_id(factor_definition_id.clone())
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(QUANT_FACTOR, factor_definition_id));
    };
    let from = row.status;
    if !from.allows_transition_to(next) {
        return Err(error::illegal_transition(
            QUANT_FACTOR,
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
