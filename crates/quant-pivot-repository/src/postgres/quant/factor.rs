//! Postgres-backed factor definition + value repository.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::{
    hashing::CanonicalDigestError,
    storage::{
        StorageError,
        entity::{QUANT_FACTOR, QUANT_MODEL_RUN},
    },
};
use quant_pivot_models::{
    domain::{
        api::FactorDefinitionListQuery,
        pagination::{PageWindow, Paginated},
        quant::{
            FactorDefinitionInfo, FactorRegistrationOutcome, FactorValueInfo,
            LatestFactorSnapshotBundleInfo, LatestFactorSnapshotInfo,
            LatestFactorSnapshotValueInfo, NewFactorDefinition, NewFactorValue,
        },
    },
    entities::{
        quant_factor_definition::{Column, Entity, Model as QuantFactorDefinitionModel},
        quant_factor_value::{
            Column as QuantFactorValueColumn, Entity as QuantFactorValueEntity, Model,
            Relation as QuantFactorValueRelation,
        },
        quant_feature_vector::{
            Column as QuantFeatureVectorColumn, Entity as QuantFeatureVectorEntity,
            Model as QuantFeatureVectorModel,
        },
        quant_model_run::{
            Column as QuantModelRunColumn, Entity as QuantModelRunEntity,
            Model as QuantModelRunModel,
        },
    },
    enums::{
        factor::FactorValueState,
        quant::{ModelRunKind, ModelRunStatus},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, FactorDefinitionId, FactorValueId, FeatureVectorId, MarketId, ModelRunId,
        ModelVersionId, factor::FactorDefinitionRef,
    },
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, ExprTrait, IntoActiveModel, JoinType, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, RelationTrait, TransactionTrait, TryInsertResult,
    sea_query::{Expr, Func, OnConflict},
};
use serde::Serialize;

use crate::{
    postgres::{error, quant::condition_wake::notify_input_change, query::find_id_chunks},
    traits::FactorRepository,
};

/// Postgres-backed factor repository: immutable, content-addressed definition
/// revisions plus the insert-only factor-value ledger.
pub struct PgFactorRepository {
    db: DatabaseConnection,
}

struct FactorValuePersistenceContext {
    run: QuantModelRunModel,
    definitions: HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    feature_vectors: HashMap<FeatureVectorId, QuantFeatureVectorModel>,
}

#[derive(Serialize)]
struct LatestSnapshotPreimage<'a> {
    factor_value_id: &'a FactorValueId,
    factor_definition_id: &'a FactorDefinitionId,
    feature_vector_id: &'a FeatureVectorId,
    model_run_id: &'a ModelRunId,
    definition_hash: &'a ContentHash,
    model_version_id: &'a ModelVersionId,
    market_id: &'a MarketId,
    raw_value: Decimal,
    normalized_value: Decimal,
    confidence: Decimal,
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
}

impl LatestSnapshotPreimage<'_> {
    fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed("quant-pivot/factor/latest-snapshot", 1, self)
    }
}

#[derive(Serialize)]
struct LatestSnapshotBundlePreimage<'a> {
    model_run_id: &'a ModelRunId,
    feature_vector_id: &'a FeatureVectorId,
    model_version_id: &'a ModelVersionId,
    market_id: &'a MarketId,
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    values: &'a [LatestFactorSnapshotValueInfo],
}

impl LatestSnapshotBundlePreimage<'_> {
    fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_typed("quant-pivot/factor/latest-snapshot-bundle", 1, self)
    }
}

impl PgFactorRepository {
    /// Build a repository over a database connection.
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn validate_value_batch(
        values: &[NewFactorValue],
        model_run_id: ModelRunId,
    ) -> Result<(), StorageError> {
        let mut value_ids = HashSet::with_capacity(values.len());
        let mut value_keys = HashSet::with_capacity(values.len());
        let mut vector_bindings = HashMap::with_capacity(values.len());
        for value in values {
            if value.model_run_id != model_run_id {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    "one factor-value persistence batch cannot span model runs",
                ));
            }
            if !value_ids.insert(value.factor_value_id) {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!(
                        "duplicate factor value id in persistence batch: {}",
                        value.factor_value_id
                    ),
                ));
            }
            let value_key = (
                value.model_run_id,
                value.feature_vector_id,
                value.factor_definition_id,
            );
            if !value_keys.insert(value_key) {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!(
                        "duplicate model-run/feature-vector/factor-definition tuple in persistence batch: {model_run_id}/{}/{}",
                        value.feature_vector_id, value.factor_definition_id
                    ),
                ));
            }
            let binding_key = (
                value.market_id.clone(),
                value.decision_at.timestamp_micros(),
            );
            if let Some(existing) = vector_bindings.insert(binding_key, value.feature_vector_id)
                && existing != value.feature_vector_id
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!(
                        "one factor plane cannot bind market {} at {} to feature vectors {existing} and {}",
                        value.market_id, value.decision_at, value.feature_vector_id
                    ),
                ));
            }
        }
        Ok(())
    }

    async fn load_value_context(
        txn: &DatabaseTransaction,
        values: &[NewFactorValue],
        model_run_id: ModelRunId,
    ) -> Result<FactorValuePersistenceContext, StorageError> {
        let run = QuantModelRunEntity::find_by_id(model_run_id)
            .lock_exclusive()
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, model_run_id))?;
        Self::validate_running_run(&run)?;

        let mut vector_bindings = values
            .iter()
            .map(|value| {
                (
                    (
                        value.market_id.clone(),
                        value.decision_at.timestamp_micros(),
                    ),
                    value.feature_vector_id,
                )
            })
            .collect::<HashMap<_, _>>();
        let persisted_bindings = QuantFactorValueEntity::find()
            .select_only()
            .column(QuantFactorValueColumn::MarketId)
            .column(QuantFactorValueColumn::DecisionAt)
            .column(QuantFactorValueColumn::FeatureVectorId)
            .filter(QuantFactorValueColumn::ModelRunId.eq(model_run_id))
            .into_tuple::<(MarketId, DateTime<Utc>, FeatureVectorId)>()
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        for (market_id, decision_at, feature_vector_id) in persisted_bindings {
            let binding_key = (market_id.clone(), decision_at.timestamp_micros());
            if let Some(existing) = vector_bindings.insert(binding_key, feature_vector_id)
                && existing != feature_vector_id
            {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!(
                        "model run {model_run_id} binds market {market_id} at {decision_at} to multiple feature vectors"
                    ),
                ));
            }
        }

        let mut definition_ids = values
            .iter()
            .map(|value| value.factor_definition_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        definition_ids.sort_unstable_by_key(|id| id.as_uuid());
        let definitions =
            find_id_chunks::<Entity, _, _>(txn, &definition_ids, Column::FactorDefinitionId)
                .await?
                .into_iter()
                .map(Self::verified_definition)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|definition| (definition.factor_definition_id, definition))
                .collect::<HashMap<_, _>>();

        let mut feature_vector_ids = values
            .iter()
            .map(|value| value.feature_vector_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        feature_vector_ids.sort_unstable_by_key(|id| id.as_uuid());
        let feature_vectors = find_id_chunks::<QuantFeatureVectorEntity, _, _>(
            txn,
            &feature_vector_ids,
            QuantFeatureVectorColumn::FeatureVectorId,
        )
        .await?
        .into_iter()
        .map(|vector| (vector.feature_vector_id, vector))
        .collect::<HashMap<_, _>>();

        Ok(FactorValuePersistenceContext {
            run,
            definitions,
            feature_vectors,
        })
    }

    fn validate_value_context(
        values: &[NewFactorValue],
        context: &FactorValuePersistenceContext,
    ) -> Result<(), StorageError> {
        for value in values {
            Self::validate_factor_run_lineage(
                value.factor_value_id,
                value.model_run_id,
                value.decision_at,
                &context.run,
            )?;
            let definition = context
                .definitions
                .get(&value.factor_definition_id)
                .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, value.factor_definition_id))?;
            Self::validate_value_definition(value, definition)?;
            let feature_vector = context
                .feature_vectors
                .get(&value.feature_vector_id)
                .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, value.feature_vector_id))?;
            Self::validate_factor_value_lineage(
                value.factor_value_id,
                value.feature_vector_id,
                &value.market_id,
                value.decision_at,
                feature_vector,
            )?;
        }
        Ok(())
    }

    async fn insert_values(
        txn: &DatabaseTransaction,
        values: Vec<NewFactorValue>,
        context: &FactorValuePersistenceContext,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        let mut inserted = Vec::with_capacity(values.len());
        for value in values {
            let duplicate_key = format!(
                "{}/{}/{}",
                value.model_run_id, value.feature_vector_id, value.factor_definition_id
            );
            let model = value
                .into_active_model()
                .insert(txn)
                .await
                .map_err(|source| error::map_unique(source, QUANT_FACTOR, &duplicate_key))?;
            let definition = context
                .definitions
                .get(&model.factor_definition_id)
                .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, model.factor_definition_id))?;
            let feature_vector = context
                .feature_vectors
                .get(&model.feature_vector_id)
                .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, model.feature_vector_id))?;
            inserted.push(Self::verified_value(
                model,
                definition,
                feature_vector,
                &context.run,
            )?);
        }
        Ok(inserted)
    }

    async fn find_bundle_candidate(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
        requested_count: i64,
    ) -> Result<Option<(ModelRunId, FeatureVectorId, DateTime<Utc>)>, StorageError> {
        QuantFactorValueEntity::find()
            .join(
                JoinType::InnerJoin,
                QuantFactorValueRelation::ModelRun.def(),
            )
            .select_only()
            .column(QuantFactorValueColumn::ModelRunId)
            .column(QuantFactorValueColumn::FeatureVectorId)
            .column(QuantFactorValueColumn::DecisionAt)
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().copied()),
            )
            .filter(QuantFactorValueColumn::DecisionAt.lte(available_by))
            .filter(QuantFactorValueColumn::CreatedAt.lte(available_by))
            .filter(
                Expr::col((QuantFactorValueEntity, QuantFactorValueColumn::CreatedAt)).lte(
                    Expr::col((QuantModelRunEntity, QuantModelRunColumn::FinishedAt)),
                ),
            )
            .filter(QuantModelRunColumn::ModelVersionId.eq(*model_version_id))
            .filter(QuantModelRunColumn::RunKind.eq(ModelRunKind::LiveInference))
            .filter(QuantModelRunColumn::Status.eq(ModelRunStatus::Succeeded))
            .filter(QuantModelRunColumn::FinishedAt.lte(available_by))
            .group_by(QuantFactorValueColumn::ModelRunId)
            .group_by(QuantFactorValueColumn::FeatureVectorId)
            .group_by(QuantFactorValueColumn::DecisionAt)
            .group_by(QuantModelRunColumn::FinishedAt)
            .having(
                Expr::col(QuantFactorValueColumn::FactorDefinitionId)
                    .count_distinct()
                    .eq(requested_count),
            )
            .having(
                Expr::col(QuantFactorValueColumn::FactorValueId)
                    .count()
                    .eq(requested_count),
            )
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
            .order_by_desc(Func::greatest([
                Expr::col((QuantFactorValueEntity, QuantFactorValueColumn::CreatedAt)).max(),
                Expr::col((QuantModelRunEntity, QuantModelRunColumn::FinishedAt)),
            ]))
            .order_by_desc(QuantModelRunColumn::FinishedAt)
            .order_by_desc(QuantFactorValueColumn::ModelRunId)
            .order_by_desc(QuantFactorValueColumn::FeatureVectorId)
            .into_tuple::<(ModelRunId, FeatureVectorId, DateTime<Utc>)>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait::async_trait]
impl FactorRepository for PgFactorRepository {
    async fn register_definitions(
        &self,
        definitions: Vec<NewFactorDefinition>,
    ) -> Result<Vec<FactorRegistrationOutcome>, StorageError> {
        let definitions = Self::canonicalize_registration(definitions)?;
        if definitions.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let mut outcomes = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let insert = Entity::insert(definition.clone().into_active_model())
                // A factor revision has two database-enforced content identities:
                // its derived primary key and its canonical definition hash.
                // SeaORM's convenience method targets only the primary key,
                // which leaks the hash-index race as a 23505 under concurrent
                // exact retries. Untargeted Postgres `ON CONFLICT DO NOTHING`
                // covers every unique arbiter; the read-back below still proves
                // that the winning row is byte-for-byte the requested revision.
                .on_conflict(OnConflict::new().do_nothing().to_owned())
                .try_insert()
                .exec_without_returning(&txn)
                .await
                .map_err(StorageError::from)?;
            let inserted = match insert {
                TryInsertResult::Inserted(1) => true,
                TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => false,
                TryInsertResult::Inserted(rows) => {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_FACTOR),
                        format!(
                            "single factor registration affected {rows} rows; expected zero or one"
                        ),
                    ));
                }
                TryInsertResult::Empty => {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_FACTOR),
                        "non-empty factor registration produced an empty insert",
                    ));
                }
            };
            let row = Self::load_registered_definition(&txn, &definition).await?;
            let row = Self::verified_definition(row)?;
            Self::ensure_identical_definition(&row, &definition)?;
            let outcome = if inserted {
                FactorRegistrationOutcome::Inserted(row)
            } else {
                FactorRegistrationOutcome::AlreadyPresent(row)
            };
            outcomes.push(outcome);
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(outcomes)
    }

    async fn create_values(
        &self,
        values: Vec<NewFactorValue>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        let Some(first_value) = values.first() else {
            return Ok(Vec::new());
        };
        let model_run_id = first_value.model_run_id;
        Self::validate_value_batch(&values, model_run_id)?;
        // Insert per row inside one transaction rather than a single multi-row
        // `insert_many`: sea-query's batched VALUES drops the Postgres enum cast
        // for the *nullable* native-enum columns (`normalization_source` /
        // `indeterminate_reason`), binding them as `text` and failing the insert.
        // A single-row insert carries the enum cast correctly; the transaction
        // keeps the ledger write atomic.
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let context = Self::load_value_context(&txn, &values, model_run_id).await?;
        Self::validate_value_context(&values, &context)?;
        let inserted = Self::insert_values(&txn, values, &context).await?;
        notify_input_change(&txn, "factor").await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError> {
        let row = Entity::find_by_id(*factor_definition_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        row.map(Self::verified_definition).transpose()
    }

    async fn find_definitions_by_ids(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError> {
        if factor_definition_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = Entity::find()
            .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().copied()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter().map(Self::verified_definition).collect()
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
            .add_option(query.scope.map(|scope| Column::Scope.eq(scope)));
        let window = PageWindow::from_query(&query);
        let paginator = Entity::find()
            .filter(condition)
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::FactorDefinitionId)
            .paginate(&self.db, window.size());
        let total = paginator.num_items().await.map_err(StorageError::from)?;
        let rows = paginator
            .fetch_page(window.seaorm_index())
            .await
            .map_err(StorageError::from)?;
        let items = rows
            .into_iter()
            .map(Self::verified_definition)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::from_window(items, total, window))
    }

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        let rows = QuantFactorValueEntity::find()
            .filter(QuantFactorValueColumn::ModelRunId.eq(*model_run_id))
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
            .order_by_desc(QuantFactorValueColumn::CreatedAt)
            .order_by_desc(QuantFactorValueColumn::FactorValueId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Self::verified_values(&self.db, rows).await
    }

    async fn find_values_by_vectors(
        &self,
        feature_vector_ids: &[FeatureVectorId],
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        let rows = find_id_chunks::<QuantFactorValueEntity, _, _>(
            &self.db,
            feature_vector_ids,
            QuantFactorValueColumn::FeatureVectorId,
        )
        .await?;
        Self::verified_values(&self.db, rows).await
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
        let rows = QuantFactorValueEntity::find()
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().copied()),
            )
            .filter(QuantFactorValueColumn::DecisionAt.gte(from))
            .filter(QuantFactorValueColumn::DecisionAt.lt(until))
            .order_by_asc(QuantFactorValueColumn::DecisionAt)
            .order_by_asc(QuantFactorValueColumn::CreatedAt)
            .order_by_asc(QuantFactorValueColumn::FactorValueId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Self::verified_values(&self.db, rows).await
    }

    async fn latest_snapshot(
        &self,
        factor_definition_id: &FactorDefinitionId,
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotInfo>, StorageError> {
        let row = QuantFactorValueEntity::find()
            .find_also_related(QuantModelRunEntity)
            .filter(QuantFactorValueColumn::FactorDefinitionId.eq(*factor_definition_id))
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(QuantFactorValueColumn::ValueState.eq(FactorValueState::Scored))
            .filter(QuantFactorValueColumn::DecisionAt.lte(available_by))
            .filter(QuantFactorValueColumn::CreatedAt.lte(available_by))
            .filter(
                Expr::col((QuantFactorValueEntity, QuantFactorValueColumn::CreatedAt)).lte(
                    Expr::col((QuantModelRunEntity, QuantModelRunColumn::FinishedAt)),
                ),
            )
            .filter(QuantModelRunColumn::ModelVersionId.eq(*model_version_id))
            .filter(QuantModelRunColumn::RunKind.eq(ModelRunKind::LiveInference))
            .filter(QuantModelRunColumn::Status.eq(ModelRunStatus::Succeeded))
            .filter(QuantModelRunColumn::FinishedAt.lte(available_by))
            .order_by_desc(QuantFactorValueColumn::DecisionAt)
            .order_by_desc(Func::greatest([
                Expr::col((QuantFactorValueEntity, QuantFactorValueColumn::CreatedAt)),
                Expr::col((QuantModelRunEntity, QuantModelRunColumn::FinishedAt)),
            ]))
            .order_by_desc(QuantModelRunColumn::FinishedAt)
            .order_by_desc(QuantFactorValueColumn::FactorValueId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        let Some((value, Some(run))) = row else {
            return Ok(None);
        };
        let definition = Entity::find_by_id(*factor_definition_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: QUANT_FACTOR,
                id: factor_definition_id.to_string(),
            })?;
        let definition = Self::verified_definition(definition)?;
        let feature_vector = QuantFeatureVectorEntity::find_by_id(value.feature_vector_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, value.feature_vector_id))?;
        let value = Self::verified_value(value, &definition, &feature_vector, &run)?;
        let finished_at = Self::visible_run_finished_at(&run, model_version_id, available_by)?;
        let Some(raw_value) = value.raw_value else {
            return Ok(None);
        };
        let Some(normalized_score) = value.normalized_score else {
            return Ok(None);
        };
        let available_at = Ord::max(value.created_at, finished_at);
        let snapshot_hash = LatestSnapshotPreimage {
            factor_value_id: &value.factor_value_id,
            factor_definition_id: &value.factor_definition_id,
            feature_vector_id: &value.feature_vector_id,
            model_run_id: &run.model_run_id,
            definition_hash: &definition.definition_hash,
            model_version_id,
            market_id: &value.market_id,
            raw_value,
            normalized_value: normalized_score.inner(),
            confidence: value.confidence.inner(),
            observed_at: value.decision_at,
            available_at,
        }
        .content_hash()
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!("latest factor snapshot hash failed: {error}"),
            )
        })?;
        Ok(Some(LatestFactorSnapshotInfo {
            factor_value_id: value.factor_value_id,
            factor_definition_id: value.factor_definition_id,
            feature_vector_id: value.feature_vector_id,
            model_run_id: run.model_run_id,
            definition_hash: definition.definition_hash,
            model_version_id: *model_version_id,
            market_id: value.market_id,
            raw_value,
            normalized_value: normalized_score.inner(),
            confidence: value.confidence.inner(),
            observed_at: value.decision_at,
            available_at,
            snapshot_hash,
        }))
    }

    async fn latest_snapshot_bundle(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
        market_id: &MarketId,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<Option<LatestFactorSnapshotBundleInfo>, StorageError> {
        if factor_definition_ids.is_empty() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                "latest factor snapshot bundle requires at least one definition",
            ));
        }
        let requested = factor_definition_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if requested.len() != factor_definition_ids.len() {
            return Err(StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                "latest factor snapshot bundle contains duplicate definition ids",
            ));
        }

        let requested_count = i64::try_from(requested.len()).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!("factor snapshot definition count overflow: {error}"),
            )
        })?;
        let candidate = self
            .find_bundle_candidate(
                factor_definition_ids,
                market_id,
                model_version_id,
                available_by,
                requested_count,
            )
            .await?;
        let Some((model_run_id, feature_vector_id, observed_at)) = candidate else {
            return Ok(None);
        };
        let run = QuantModelRunEntity::find_by_id(model_run_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, model_run_id))?;
        let finished_at = Self::visible_run_finished_at(&run, model_version_id, available_by)?;

        let rows = QuantFactorValueEntity::find()
            .filter(QuantFactorValueColumn::ModelRunId.eq(run.model_run_id))
            .filter(QuantFactorValueColumn::FeatureVectorId.eq(feature_vector_id))
            .filter(QuantFactorValueColumn::MarketId.eq(market_id.clone()))
            .filter(QuantFactorValueColumn::DecisionAt.eq(observed_at))
            .filter(
                QuantFactorValueColumn::FactorDefinitionId
                    .is_in(factor_definition_ids.iter().copied()),
            )
            .filter(QuantFactorValueColumn::CreatedAt.lte(available_by))
            .order_by_asc(QuantFactorValueColumn::FactorDefinitionId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let found = rows
            .iter()
            .map(|row| row.factor_definition_id)
            .collect::<HashSet<_>>();
        if found != requested {
            return Ok(None);
        }
        if rows.len() != requested.len() {
            return Err(StorageError::state_conflict(
                QUANT_FACTOR,
                Some(&run.model_run_id),
                "factor snapshot run contains duplicate definition rows",
            ));
        }
        let latest_value_at = rows.iter().map(|row| row.created_at).max().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                "factor snapshot bundle unexpectedly has no values",
            )
        })?;
        let available_at = Ord::max(latest_value_at, finished_at);
        let values = Self::project_snapshot_values(
            &self.db,
            factor_definition_ids,
            market_id,
            rows,
            requested.len(),
            &run,
        )
        .await?;
        let snapshot_hash = LatestSnapshotBundlePreimage {
            model_run_id: &run.model_run_id,
            feature_vector_id: &feature_vector_id,
            model_version_id,
            market_id,
            observed_at,
            available_at,
            values: &values,
        }
        .content_hash()
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!("latest factor snapshot bundle hash failed: {error}"),
            )
        })?;
        Ok(Some(LatestFactorSnapshotBundleInfo {
            model_run_id: run.model_run_id,
            feature_vector_id,
            model_version_id: *model_version_id,
            market_id: market_id.clone(),
            observed_at,
            available_at,
            values,
            snapshot_hash,
        }))
    }
}

impl PgFactorRepository {
    fn validate_value_definition(
        value: &NewFactorValue,
        definition: &FactorDefinitionInfo,
    ) -> Result<(), StorageError> {
        let revision = FactorDefinitionRef::try_from(definition).map_err(|source| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "persisted factor-definition revision {} failed verification: {source}",
                    definition.factor_definition_id
                ),
            )
        })?;
        value.validate_against(&revision).map_err(|source| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "factor value {} (`{}`, raw {:?}, scale {:?}) does not match sealed definition {}: {source}",
                    value.factor_value_id,
                    definition.name,
                    value.raw_value,
                    value.raw_value.map(|raw| raw.scale()),
                    definition.factor_definition_id,
                ),
            )
        })
    }

    fn verified_value(
        row: Model,
        definition: &FactorDefinitionInfo,
        feature_vector: &QuantFeatureVectorModel,
        run: &QuantModelRunModel,
    ) -> Result<FactorValueInfo, StorageError> {
        let value = FactorValueInfo::from(row);
        let revision = FactorDefinitionRef::try_from(definition).map_err(|source| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "persisted factor-definition revision {} failed verification: {source}",
                    definition.factor_definition_id
                ),
            )
        })?;
        value.validate_against(&revision).map_err(|source| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "persisted factor value {} failed verification: {source}",
                    value.factor_value_id
                ),
            )
        })?;
        Self::validate_factor_value_lineage(
            value.factor_value_id,
            value.feature_vector_id,
            &value.market_id,
            value.decision_at,
            feature_vector,
        )?;
        Self::validate_factor_run_lineage(
            value.factor_value_id,
            value.model_run_id,
            value.decision_at,
            run,
        )?;
        Ok(value)
    }

    async fn verified_values(
        db: &impl ConnectionTrait,
        rows: Vec<Model>,
    ) -> Result<Vec<FactorValueInfo>, StorageError> {
        let definition_ids = rows
            .iter()
            .map(|row| row.factor_definition_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let definitions =
            find_id_chunks::<Entity, _, _>(db, &definition_ids, Column::FactorDefinitionId)
                .await?
                .into_iter()
                .map(Self::verified_definition)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|definition| (definition.factor_definition_id, definition))
                .collect::<HashMap<_, _>>();
        let feature_vector_ids = rows
            .iter()
            .map(|row| row.feature_vector_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let feature_vectors = find_id_chunks::<QuantFeatureVectorEntity, _, _>(
            db,
            &feature_vector_ids,
            QuantFeatureVectorColumn::FeatureVectorId,
        )
        .await?
        .into_iter()
        .map(|vector| (vector.feature_vector_id, vector))
        .collect::<HashMap<_, _>>();
        let model_run_ids = rows
            .iter()
            .map(|row| row.model_run_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let model_runs = find_id_chunks::<QuantModelRunEntity, _, _>(
            db,
            &model_run_ids,
            QuantModelRunColumn::ModelRunId,
        )
        .await?
        .into_iter()
        .map(|run| (run.model_run_id, run))
        .collect::<HashMap<_, _>>();
        rows.into_iter()
            .map(|row| {
                let definition = definitions.get(&row.factor_definition_id).ok_or_else(|| {
                    StorageError::not_found(QUANT_FACTOR, row.factor_definition_id)
                })?;
                let feature_vector = feature_vectors
                    .get(&row.feature_vector_id)
                    .ok_or_else(|| StorageError::not_found(QUANT_FACTOR, row.feature_vector_id))?;
                let run = model_runs
                    .get(&row.model_run_id)
                    .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, row.model_run_id))?;
                Self::verified_value(row, definition, feature_vector, run)
            })
            .collect()
    }

    fn validate_running_run(run: &QuantModelRunModel) -> Result<(), StorageError> {
        if run.status == ModelRunStatus::Running {
            return Ok(());
        }
        Err(StorageError::state_conflict(
            QUANT_MODEL_RUN,
            Some(&run.model_run_id),
            format!(
                "factor values can only be appended while the model run is Running; current status is {}",
                run.status
            ),
        ))
    }

    fn validate_factor_value_lineage(
        factor_value_id: FactorValueId,
        feature_vector_id: FeatureVectorId,
        market_id: &MarketId,
        decision_at: DateTime<Utc>,
        feature_vector: &QuantFeatureVectorModel,
    ) -> Result<(), StorageError> {
        if feature_vector.feature_vector_id == feature_vector_id
            && feature_vector.market_id == *market_id
            && feature_vector.decision_at.timestamp_micros() == decision_at.timestamp_micros()
        {
            return Ok(());
        }
        Err(StorageError::invariant_violation(
            Some(QUANT_FACTOR),
            format!(
                "factor value {factor_value_id} does not match feature vector {feature_vector_id} market/decision lineage"
            ),
        ))
    }

    fn validate_factor_run_lineage(
        factor_value_id: FactorValueId,
        model_run_id: ModelRunId,
        decision_at: DateTime<Utc>,
        run: &QuantModelRunModel,
    ) -> Result<(), StorageError> {
        let decision_micros = decision_at.timestamp_micros();
        let start_micros = run.window_start.timestamp_micros();
        let end_micros = run.window_end.timestamp_micros();
        let valid_decision = match run.run_kind {
            ModelRunKind::LiveInference | ModelRunKind::Shadow => {
                start_micros == end_micros && decision_micros == start_micros
            }
            ModelRunKind::Training
            | ModelRunKind::Backtest
            | ModelRunKind::Calibration
            | ModelRunKind::Cpcv => (start_micros..=end_micros).contains(&decision_micros),
        };
        if run.model_run_id == model_run_id && valid_decision {
            return Ok(());
        }
        Err(StorageError::invariant_violation(
            Some(QUANT_FACTOR),
            format!(
                "factor value {factor_value_id} does not match model run {model_run_id} decision-window lineage"
            ),
        ))
    }

    fn visible_run_finished_at(
        run: &QuantModelRunModel,
        model_version_id: &ModelVersionId,
        available_by: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, StorageError> {
        let Some(finished_at) = run.finished_at else {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "serving-visible model run {} has no finished_at",
                    run.model_run_id
                ),
            ));
        };
        if run.run_kind != ModelRunKind::LiveInference
            || run.status != ModelRunStatus::Succeeded
            || run.model_version_id != Some(*model_version_id)
            || finished_at > available_by
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "model run {} does not satisfy the serving visibility contract",
                    run.model_run_id
                ),
            ));
        }
        Ok(finished_at)
    }

    fn verified_definition(
        row: QuantFactorDefinitionModel,
    ) -> Result<FactorDefinitionInfo, StorageError> {
        let definition = FactorDefinitionInfo::from(row);
        FactorDefinitionRef::try_from(&definition).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FACTOR),
                format!(
                    "persisted factor-definition revision {} failed verification: {error}",
                    definition.factor_definition_id
                ),
            )
        })?;
        Ok(definition)
    }

    fn canonicalize_registration(
        mut definitions: Vec<NewFactorDefinition>,
    ) -> Result<Vec<NewFactorDefinition>, StorageError> {
        for definition in &definitions {
            FactorDefinitionRef::try_from(definition).map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_FACTOR),
                    format!("invalid factor-definition revision: {error}"),
                )
            })?;
        }
        definitions.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let mut names = HashSet::with_capacity(definitions.len());
        let mut ids = HashSet::with_capacity(definitions.len());
        let mut hashes = HashSet::with_capacity(definitions.len());
        for definition in &definitions {
            for (inserted, dimension, value) in [
                (
                    names.insert(definition.name.clone()),
                    "name",
                    definition.name.clone(),
                ),
                (
                    ids.insert(definition.factor_definition_id),
                    "id",
                    definition.factor_definition_id.to_string(),
                ),
                (
                    hashes.insert(definition.definition_hash),
                    "hash",
                    definition.definition_hash.to_string(),
                ),
            ] {
                if !inserted {
                    return Err(StorageError::invariant_violation(
                        Some(QUANT_FACTOR),
                        format!(
                            "duplicate factor-definition {dimension} in registration batch: {value}"
                        ),
                    ));
                }
            }
        }
        Ok(definitions)
    }

    async fn load_registered_definition(
        txn: &DatabaseTransaction,
        definition: &NewFactorDefinition,
    ) -> Result<QuantFactorDefinitionModel, StorageError> {
        let mut rows = Entity::find()
            .filter(
                Condition::any()
                    .add(Column::FactorDefinitionId.eq(definition.factor_definition_id))
                    .add(Column::DefinitionHash.eq(definition.definition_hash)),
            )
            .all(txn)
            .await
            .map_err(StorageError::from)?;
        if rows.len() != 1 {
            return Err(StorageError::state_conflict(
                QUANT_FACTOR,
                Some(&definition.factor_definition_id),
                format!(
                    "content-addressed registration resolved {} persisted revisions; expected exactly one",
                    rows.len()
                ),
            ));
        }
        rows.pop().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_FACTOR,
                Some(&definition.factor_definition_id),
                "content-addressed registered revision disappeared",
            )
        })
    }

    fn ensure_identical_definition(
        existing: &FactorDefinitionInfo,
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
        Err(StorageError::state_conflict(
            QUANT_FACTOR,
            Some(&requested.factor_definition_id),
            format!(
                "factor-definition identity collision for hash {}",
                requested.definition_hash
            ),
        ))
    }

    async fn project_snapshot_values(
        db: &DatabaseConnection,
        factor_definition_ids: &[FactorDefinitionId],
        market_id: &MarketId,
        rows: Vec<Model>,
        expected_definition_count: usize,
        run: &QuantModelRunModel,
    ) -> Result<Vec<LatestFactorSnapshotValueInfo>, StorageError> {
        let definitions = Entity::find()
            .filter(Column::FactorDefinitionId.is_in(factor_definition_ids.iter().copied()))
            .all(db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::verified_definition)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        if definitions.len() != expected_definition_count {
            return Err(StorageError::state_conflict(
                QUANT_FACTOR,
                Some(market_id),
                "factor snapshot bundle references a missing definition revision",
            ));
        }
        let feature_vector_ids = rows
            .iter()
            .map(|row| row.feature_vector_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let feature_vectors = find_id_chunks::<QuantFeatureVectorEntity, _, _>(
            db,
            &feature_vector_ids,
            QuantFeatureVectorColumn::FeatureVectorId,
        )
        .await?
        .into_iter()
        .map(|vector| (vector.feature_vector_id, vector))
        .collect::<HashMap<_, _>>();
        rows.into_iter()
            .map(|row| {
                let definition = definitions.get(&row.factor_definition_id).ok_or_else(|| {
                    StorageError::state_conflict(
                        QUANT_FACTOR,
                        Some(&row.factor_definition_id),
                        "factor snapshot definition disappeared",
                    )
                })?;
                let feature_vector =
                    feature_vectors.get(&row.feature_vector_id).ok_or_else(|| {
                        StorageError::state_conflict(
                            QUANT_FACTOR,
                            Some(&row.feature_vector_id),
                            "factor snapshot feature vector disappeared",
                        )
                    })?;
                let row = Self::verified_value(row, definition, feature_vector, run)?;
                Ok(LatestFactorSnapshotValueInfo {
                    factor_value_id: row.factor_value_id,
                    factor_definition_id: row.factor_definition_id,
                    definition_hash: definition.definition_hash,
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
}
