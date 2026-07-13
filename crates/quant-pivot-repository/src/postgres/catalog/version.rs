//! Atomic Gamma catalog ledger writer and bitemporal reader.

use crate::{
    postgres::catalog::{event::PgEventRepository, market::PgMarketRepository},
    traits::{CatalogVersionRepository, EventRepository, MarketRepository},
};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CatalogCommit, CatalogSnapshotInfo, CatalogSyncBatchInfo, CatalogSyncFailureStage,
        CatalogSyncStatus, CatalogWindowInfo, DecisionBoundary, DecisionSource,
        EventCatalogVersionInfo, EventRegistryInfo, MarketCatalogVersionInfo, NewCatalogSyncBatch,
        NewEventCatalogVersion, NewFailedCatalogSyncBatch, NewMarketCatalogVersion,
    },
    entities::{catalog_sync_batch, event_catalog_version, market_catalog_version},
    types::{CatalogSyncBatchId, EventCatalogVersionId, EventId, MarketId},
};
use sea_orm::{
    AccessMode, ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait,
    DatabaseConnection, DatabaseTransaction, DbBackend, DbErr, EntityTrait, IntoActiveModel,
    IsolationLevel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait, RuntimeErr,
    Statement, TransactionTrait, sea_query::Expr,
};
use std::collections::{BTreeMap, BTreeSet};

const CATALOG_VISIBILITY_LOCK: &str = "quant-pivot/catalog-ledger-visibility/v1";
const CATALOG_VISIBILITY_GUARD: chrono::Duration = chrono::Duration::seconds(2);
const MAX_FAILURE_DETAIL_BYTES: usize = 2_048;

/// Postgres implementation of the immutable catalog ledger.
pub struct PgCatalogVersionRepository {
    db: DatabaseConnection,
    visibility_lock: sea_orm::sqlx::postgres::PgAdvisoryLock,
}

impl PgCatalogVersionRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            visibility_lock: sea_orm::sqlx::postgres::PgAdvisoryLock::new(CATALOG_VISIBILITY_LOCK),
        }
    }

    async fn begin_catalog_read(&self) -> Result<DatabaseTransaction, StorageError> {
        let txn = self
            .db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
            .map_err(StorageError::from)?;
        let key = self.visibility_lock.key().as_bigint().ok_or_else(|| {
            StorageError::InvariantViolation {
                entity: Some("catalog_sync_batch"),
                detail: "catalog visibility lock must use the bigint advisory-lock keyspace"
                    .to_owned(),
            }
        })?;
        txn.execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT pg_advisory_xact_lock_shared($1)",
            [key.into()],
        ))
        .await
        .map_err(StorageError::from)?;
        Ok(txn)
    }

    async fn fail_abandoned_preparations(&self) -> Result<(), StorageError> {
        catalog_sync_batch::Entity::update_many()
            .col_expr(
                catalog_sync_batch::Column::Status,
                Expr::value(CatalogSyncStatus::Failed.as_str()),
            )
            .col_expr(
                catalog_sync_batch::Column::CommittedAt,
                Expr::value(Option::<chrono::DateTime<chrono::Utc>>::None),
            )
            .col_expr(
                catalog_sync_batch::Column::FailureStage,
                Expr::value(CatalogSyncFailureStage::Recovery.as_str()),
            )
            .col_expr(
                catalog_sync_batch::Column::FailureDetail,
                Expr::value("writer stopped before catalog visibility was finalized"),
            )
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Preparing.as_str()))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn fail_batch(
        &self,
        batch_id: &CatalogSyncBatchId,
        stage: CatalogSyncFailureStage,
        detail: &str,
    ) -> Result<catalog_sync_batch::Model, StorageError> {
        let model = catalog_sync_batch::Entity::find_by_id(batch_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "catalog_sync_batch",
                id: batch_id.to_string(),
            })?;
        if model.status == CatalogSyncStatus::Failed.as_str() {
            return Ok(model);
        }
        if model.status != CatalogSyncStatus::Preparing.as_str()
            && !(model.status == CatalogSyncStatus::Committed.as_str()
                && stage == CatalogSyncFailureStage::CommitVisibility)
        {
            return Err(StorageError::IllegalTransition {
                entity: "catalog_sync_batch",
                id: Some(batch_id.to_string()),
                from: model.status,
                to: CatalogSyncStatus::Failed.as_str().to_owned(),
            });
        }
        let mut active = model.into_active_model();
        active.status = Set(CatalogSyncStatus::Failed.as_str().to_owned());
        active.committed_at = Set(None);
        active.failure_stage = Set(Some(stage.as_str().to_owned()));
        active.failure_detail = Set(Some(bounded_failure_detail(detail)));
        active.update(&self.db).await.map_err(StorageError::from)
    }
}

#[async_trait::async_trait]
impl CatalogVersionRepository for PgCatalogVersionRepository {
    async fn commit(&self, commit: CatalogCommit) -> Result<CatalogSyncBatchInfo, StorageError> {
        let CatalogCommit {
            batch,
            current_events,
            event_versions,
            current_markets,
            market_versions,
        } = commit;

        let connection = self
            .db
            .get_postgres_connection_pool()
            .acquire()
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        let mut visibility_guard = self
            .visibility_lock
            .acquire(connection)
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        self.fail_abandoned_preparations().await?;

        let batch_id =
            persist_catalog_preparation(&self.db, batch, event_versions, market_versions).await?;

        let finalize_txn = self.db.begin().await.map_err(StorageError::from)?;
        let event_repo = PgEventRepository::with_txn(&finalize_txn);
        if !current_events.is_empty() {
            event_repo.upsert_batch(current_events).await?;
        }
        let market_repo = PgMarketRepository::with_txn(&finalize_txn);
        if !current_markets.is_empty() {
            market_repo.upsert_batch(current_markets).await?;
        }

        let visible_at = database_clock(&finalize_txn).await? + CATALOG_VISIBILITY_GUARD;
        finalize_version_visibility(&finalize_txn, &batch_id, visible_at).await?;
        let batch_model = finalize_batch(&finalize_txn, &batch_id, visible_at).await?;
        if let Err(error) = finalize_txn.commit().await {
            self.fail_batch(
                &batch_id,
                CatalogSyncFailureStage::Persist,
                &format!("catalog finalization transaction failed: {error}"),
            )
            .await?;
            return Err(StorageError::from(error));
        }

        let observed_after_commit =
            sea_orm::sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
                "SELECT clock_timestamp()",
            )
            .fetch_one(&mut *visibility_guard)
            .await
            .map_err(|error| StorageError::Database(DbErr::Query(RuntimeErr::SqlxError(error))))?;
        if observed_after_commit >= visible_at {
            self.fail_batch(
                &batch_id,
                CatalogSyncFailureStage::CommitVisibility,
                &format!(
                    "catalog commit could not be proven before logical visibility boundary; \
                     observed_after_commit={observed_after_commit}, visible_at={visible_at}"
                ),
            )
            .await?;
            visibility_guard.release_now().await.map_err(|error| {
                StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error)))
            })?;
            return Err(StorageError::InvariantViolation {
                entity: Some("catalog_sync_batch"),
                detail: "catalog commit missed its logical visibility boundary".to_owned(),
            });
        }

        let wait = (visible_at - observed_after_commit)
            .to_std()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some("catalog_sync_batch"),
                detail: format!("invalid catalog visibility wait: {error}"),
            })?;
        tokio::time::sleep(wait).await;
        visibility_guard
            .release_now()
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        Ok(batch_model.into())
    }

    async fn record_failure(
        &self,
        failure: NewFailedCatalogSyncBatch,
    ) -> Result<CatalogSyncBatchInfo, StorageError> {
        let connection = self
            .db
            .get_postgres_connection_pool()
            .acquire()
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        let visibility_guard = self
            .visibility_lock
            .acquire(connection)
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        self.fail_abandoned_preparations().await?;

        let existing =
            catalog_sync_batch::Entity::find_by_id(failure.catalog_sync_batch_id.clone())
                .one(&self.db)
                .await
                .map_err(StorageError::from)?;
        let model = if let Some(existing) = existing {
            if existing.status == CatalogSyncStatus::Failed.as_str() {
                existing
            } else {
                self.fail_batch(
                    &failure.catalog_sync_batch_id,
                    failure.failure_stage,
                    &failure.failure_detail,
                )
                .await?
            }
        } else {
            let detail = bounded_failure_detail(&failure.failure_detail);
            catalog_sync_batch::Entity::insert(catalog_sync_batch::ActiveModel {
                catalog_sync_batch_id: Set(failure.catalog_sync_batch_id),
                sync_kind: Set(failure.sync_kind),
                status: Set(CatalogSyncStatus::Failed.as_str().to_owned()),
                source_cursor: Set(failure.source_cursor),
                started_at: Set(failure.started_at),
                fetched_at: Set(failure.fetched_at),
                committed_at: Set(None),
                event_count: Set(0),
                market_count: Set(0),
                rejected_count: Set(0),
                batch_hash: Set(None),
                failure_stage: Set(Some(failure.failure_stage.as_str().to_owned())),
                failure_detail: Set(Some(detail)),
                created_at: sea_orm::ActiveValue::NotSet,
                updated_at: sea_orm::ActiveValue::NotSet,
            })
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?
        };
        visibility_guard
            .release_now()
            .await
            .map_err(|error| StorageError::Database(DbErr::Conn(RuntimeErr::SqlxError(error))))?;
        Ok(model.into())
    }

    async fn coverage_start(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        let result = catalog_sync_batch::Entity::find()
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .order_by_asc(catalog_sync_batch::Column::CommittedAt)
            .one(&txn)
            .await
            .map_err(StorageError::from)
            .map(|row| row.and_then(|model| model.committed_at));
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn watermark(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        let result = catalog_sync_batch::Entity::find()
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .order_by_desc(catalog_sync_batch::Column::CommittedAt)
            .one(&txn)
            .await
            .map_err(StorageError::from)
            .map(|row| row.and_then(|model| model.committed_at));
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<MarketCatalogVersionInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        let result = market_catalog_version::Entity::find()
            .join(
                JoinType::InnerJoin,
                market_catalog_version::Relation::CatalogSyncBatch.def(),
            )
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
            .filter(market_catalog_version::Column::MarketId.eq(market_id.clone()))
            .filter(
                market_catalog_version::Column::SourceEffectiveAt
                    .lte(boundary.cutoff_for(DecisionSource::Catalog)),
            )
            .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
            .order_by_desc(market_catalog_version::Column::SourceEffectiveAt)
            .order_by_desc(market_catalog_version::Column::AvailableAt)
            .order_by_desc(market_catalog_version::Column::MarketCatalogVersionId)
            .one(&txn)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into));
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn markets_at(
        &self,
        market_ids: &[MarketId],
        boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketCatalogVersionInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self.begin_catalog_read().await?;
        let result = market_catalog_version::Entity::find()
            .join(
                JoinType::InnerJoin,
                market_catalog_version::Relation::CatalogSyncBatch.def(),
            )
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
            .filter(market_catalog_version::Column::MarketId.is_in(market_ids.to_vec()))
            .filter(
                market_catalog_version::Column::SourceEffectiveAt
                    .lte(boundary.cutoff_for(DecisionSource::Catalog)),
            )
            .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
            .distinct_on([market_catalog_version::Column::MarketId])
            .order_by_asc(market_catalog_version::Column::MarketId)
            .order_by_desc(market_catalog_version::Column::SourceEffectiveAt)
            .order_by_desc(market_catalog_version::Column::AvailableAt)
            .order_by_desc(market_catalog_version::Column::MarketCatalogVersionId)
            .all(&txn)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect());
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn event_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<EventCatalogVersionInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        let result = event_catalog_version::Entity::find()
            .join(
                JoinType::InnerJoin,
                event_catalog_version::Relation::CatalogSyncBatch.def(),
            )
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
            .filter(event_catalog_version::Column::EventId.eq(event_id.clone()))
            .filter(
                event_catalog_version::Column::SourceEffectiveAt
                    .lte(boundary.cutoff_for(DecisionSource::Catalog)),
            )
            .filter(event_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
            .order_by_desc(event_catalog_version::Column::SourceEffectiveAt)
            .order_by_desc(event_catalog_version::Column::AvailableAt)
            .order_by_desc(event_catalog_version::Column::EventCatalogVersionId)
            .one(&txn)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into));
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn event_markets_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketCatalogVersionInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        let result = market_catalog_version::Entity::find()
            .join(
                JoinType::InnerJoin,
                market_catalog_version::Relation::CatalogSyncBatch.def(),
            )
            .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
            .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
            .filter(market_catalog_version::Column::EventId.eq(event_id.clone()))
            .filter(
                market_catalog_version::Column::SourceEffectiveAt
                    .lte(boundary.cutoff_for(DecisionSource::Catalog)),
            )
            .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
            .distinct_on([market_catalog_version::Column::MarketId])
            .order_by_asc(market_catalog_version::Column::MarketId)
            .order_by_desc(market_catalog_version::Column::SourceEffectiveAt)
            .order_by_desc(market_catalog_version::Column::AvailableAt)
            .order_by_desc(market_catalog_version::Column::MarketCatalogVersionId)
            .all(&txn)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect());
        txn.commit().await.map_err(StorageError::from)?;
        result
    }

    async fn snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogSnapshotInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;

        ensure_coverage(&txn, boundary).await?;
        let Some((market, event)) = joined_market_at(&txn, market_id, boundary).await? else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let event = event.ok_or_else(|| StorageError::InvariantViolation {
            entity: Some("market_catalog_version"),
            detail: format!(
                "market version {} references missing event version {}",
                market.market_catalog_version_id, market.event_catalog_version_id
            ),
        })?;
        let event_payload: EventRegistryInfo = serde_json::from_value(event.payload.clone())
            .map_err(|error| {
                StorageError::Codec(format!(
                    "event catalog version {} payload: {error}",
                    event.event_catalog_version_id
                ))
            })?;
        let member_ids = event_payload.market_ids;
        let event_markets =
            exact_event_markets_at(&txn, &event.event_catalog_version_id, &member_ids, boundary)
                .await?
                .into_iter()
                .map(Into::into)
                .collect();

        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(CatalogSnapshotInfo {
            market: market.into(),
            event: event.into(),
            event_markets,
        }))
    }

    async fn snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogSnapshotInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;

        let latest = latest_joined_markets(&txn, boundary).await?;
        let event_version_ids = latest
            .iter()
            .map(|(market, _)| market.event_catalog_version_id.clone())
            .collect::<BTreeSet<_>>();
        let member_rows = if event_version_ids.is_empty() {
            Vec::new()
        } else {
            market_catalog_version::Entity::find()
                .join(
                    JoinType::InnerJoin,
                    market_catalog_version::Relation::CatalogSyncBatch.def(),
                )
                .filter(
                    catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()),
                )
                .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
                .filter(
                    market_catalog_version::Column::EventCatalogVersionId.is_in(event_version_ids),
                )
                .filter(
                    market_catalog_version::Column::SourceEffectiveAt
                        .lte(boundary.cutoff_for(DecisionSource::Catalog)),
                )
                .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
                .order_by_asc(market_catalog_version::Column::EventCatalogVersionId)
                .order_by_asc(market_catalog_version::Column::MarketId)
                .all(&txn)
                .await
                .map_err(StorageError::from)?
        };
        let mut members_by_event = BTreeMap::<EventCatalogVersionId, Vec<_>>::new();
        for row in member_rows {
            members_by_event
                .entry(row.event_catalog_version_id.clone())
                .or_default()
                .push(row.into());
        }

        let mut snapshots = Vec::with_capacity(latest.len());
        for (market, event) in latest {
            let event = event.ok_or_else(|| StorageError::InvariantViolation {
                entity: Some("market_catalog_version"),
                detail: format!(
                    "market version {} references missing event version {}",
                    market.market_catalog_version_id, market.event_catalog_version_id
                ),
            })?;
            snapshots.push(CatalogSnapshotInfo {
                event_markets: members_by_event
                    .get(&event.event_catalog_version_id)
                    .cloned()
                    .unwrap_or_default(),
                market: market.into(),
                event: event.into(),
            });
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(snapshots)
    }

    async fn window_through(
        &self,
        market_ids: &[MarketId],
        end_boundary: &DecisionBoundary,
    ) -> Result<CatalogWindowInfo, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, end_boundary).await?;

        let seed_rows = joined_market_history(&txn, market_ids, end_boundary).await?;
        let mut member_ids = BTreeSet::new();
        for (market, event) in &seed_rows {
            member_ids.insert(market.market_id.clone());
            let event = event
                .as_ref()
                .ok_or_else(|| StorageError::InvariantViolation {
                    entity: Some("market_catalog_version"),
                    detail: format!(
                        "market version {} references missing event version {}",
                        market.market_catalog_version_id, market.event_catalog_version_id
                    ),
                })?;
            let payload: EventRegistryInfo = serde_json::from_value(event.payload.clone())
                .map_err(|error| {
                    StorageError::Codec(format!(
                        "event catalog version {} payload: {error}",
                        event.event_catalog_version_id
                    ))
                })?;
            member_ids.extend(payload.market_ids);
        }

        let all_rows = joined_market_history(
            &txn,
            &member_ids.into_iter().collect::<Vec<_>>(),
            end_boundary,
        )
        .await?;
        let mut markets = BTreeMap::new();
        let mut events = BTreeMap::new();
        for (market, event) in all_rows {
            let event = event.ok_or_else(|| StorageError::InvariantViolation {
                entity: Some("market_catalog_version"),
                detail: format!(
                    "market version {} references missing event version {}",
                    market.market_catalog_version_id, market.event_catalog_version_id
                ),
            })?;
            events.insert(event.event_catalog_version_id.clone(), event);
            markets.insert(market.market_catalog_version_id.clone(), market);
        }
        // Seed rows are included even when an event revision temporarily named
        // no materialized members.
        for (market, event) in seed_rows {
            let event = event.ok_or_else(|| StorageError::InvariantViolation {
                entity: Some("market_catalog_version"),
                detail: format!(
                    "market version {} references missing event version {}",
                    market.market_catalog_version_id, market.event_catalog_version_id
                ),
            })?;
            events.insert(event.event_catalog_version_id.clone(), event);
            markets.insert(market.market_catalog_version_id.clone(), market);
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(CatalogWindowInfo {
            market_versions: markets.into_values().map(Into::into).collect(),
            event_versions: events.into_values().map(Into::into).collect(),
        })
    }
}

async fn persist_catalog_preparation(
    db: &DatabaseConnection,
    batch: NewCatalogSyncBatch,
    event_versions: Vec<NewEventCatalogVersion>,
    market_versions: Vec<NewMarketCatalogVersion>,
) -> Result<CatalogSyncBatchId, StorageError> {
    let batch_id = batch.catalog_sync_batch_id.clone();
    let preparing = catalog_sync_batch::ActiveModel {
        catalog_sync_batch_id: Set(batch.catalog_sync_batch_id),
        sync_kind: Set(batch.sync_kind),
        status: Set(CatalogSyncStatus::Preparing.as_str().to_owned()),
        source_cursor: Set(batch.source_cursor),
        started_at: Set(batch.started_at),
        fetched_at: Set(Some(batch.fetched_at)),
        committed_at: Set(None),
        event_count: Set(batch.event_count),
        market_count: Set(batch.market_count),
        rejected_count: Set(batch.rejected_count),
        batch_hash: Set(Some(batch.batch_hash)),
        failure_stage: Set(None),
        failure_detail: Set(None),
        created_at: sea_orm::ActiveValue::NotSet,
        updated_at: sea_orm::ActiveValue::NotSet,
    };

    // Preparation is durable but deliberately invisible to every catalog
    // reader. Current projections remain untouched until finalization.
    let txn = db.begin().await.map_err(StorageError::from)?;
    catalog_sync_batch::Entity::insert(preparing)
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    if !event_versions.is_empty() {
        event_catalog_version::Entity::insert_many(
            event_versions
                .into_iter()
                .map(IntoActiveModel::into_active_model),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    }
    if !market_versions.is_empty() {
        market_catalog_version::Entity::insert_many(
            market_versions
                .into_iter()
                .map(IntoActiveModel::into_active_model),
        )
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    }
    txn.commit().await.map_err(StorageError::from)?;
    Ok(batch_id)
}

async fn database_clock<C>(db: &C) -> Result<chrono::DateTime<chrono::Utc>, StorageError>
where
    C: ConnectionTrait,
{
    db.query_one(Statement::from_string(
        DbBackend::Postgres,
        "SELECT clock_timestamp() AS current_time",
    ))
    .await
    .map_err(StorageError::from)?
    .ok_or_else(|| StorageError::InvariantViolation {
        entity: Some("catalog_sync_batch"),
        detail: "Postgres clock query returned no row".to_owned(),
    })?
    .try_get("", "current_time")
    .map_err(StorageError::from)
}

async fn finalize_version_visibility<C>(
    db: &C,
    batch_id: &CatalogSyncBatchId,
    visible_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), StorageError>
where
    C: ConnectionTrait,
{
    for table in ["event_catalog_version", "market_catalog_version"] {
        db.execute(Statement::from_sql_and_values(
            DbBackend::Postgres,
            format!(
                "UPDATE {table} \
                 SET available_at = $2, \
                     source_effective_at = CASE \
                         WHEN source_timestamp_quality = 'available_at_fallback' THEN $2 \
                         ELSE source_effective_at \
                     END \
                 WHERE catalog_sync_batch_id = $1"
            ),
            [batch_id.clone().into(), visible_at.into()],
        ))
        .await
        .map_err(StorageError::from)?;
    }
    Ok(())
}

async fn finalize_batch<C>(
    db: &C,
    batch_id: &CatalogSyncBatchId,
    visible_at: chrono::DateTime<chrono::Utc>,
) -> Result<catalog_sync_batch::Model, StorageError>
where
    C: ConnectionTrait,
{
    let model = catalog_sync_batch::Entity::find_by_id(batch_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: "catalog_sync_batch",
            id: batch_id.to_string(),
        })?;
    if model.status != CatalogSyncStatus::Preparing.as_str() {
        return Err(StorageError::IllegalTransition {
            entity: "catalog_sync_batch",
            id: Some(batch_id.to_string()),
            from: model.status,
            to: CatalogSyncStatus::Committed.as_str().to_owned(),
        });
    }
    let mut active = model.into_active_model();
    active.status = Set(CatalogSyncStatus::Committed.as_str().to_owned());
    active.committed_at = Set(Some(visible_at));
    active.update(db).await.map_err(StorageError::from)
}

fn bounded_failure_detail(detail: &str) -> String {
    if detail.len() <= MAX_FAILURE_DETAIL_BYTES {
        return detail.to_owned();
    }
    let mut end = MAX_FAILURE_DETAIL_BYTES;
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail[..end].to_owned()
}

async fn ensure_coverage<C>(db: &C, boundary: &DecisionBoundary) -> Result<(), StorageError>
where
    C: ConnectionTrait,
{
    let coverage_start = catalog_sync_batch::Entity::find()
        .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
        .order_by_asc(catalog_sync_batch::Column::CommittedAt)
        .one(db)
        .await
        .map_err(StorageError::from)?
        .and_then(|batch| batch.committed_at)
        .ok_or_else(|| {
            StorageError::StaleData(
                "catalog ledger has no successful synchronization coverage".to_owned(),
            )
        })?;
    if boundary.decision_at() < coverage_start {
        return Err(StorageError::StaleData(format!(
            "catalog replay decision {} predates coverage start {coverage_start}",
            boundary.decision_at()
        )));
    }
    Ok(())
}

async fn joined_market_at<C>(
    db: &C,
    market_id: &MarketId,
    boundary: &DecisionBoundary,
) -> Result<
    Option<(
        market_catalog_version::Model,
        Option<event_catalog_version::Model>,
    )>,
    StorageError,
>
where
    C: ConnectionTrait,
{
    market_catalog_version::Entity::find()
        .find_also_related(event_catalog_version::Entity)
        .join(
            JoinType::InnerJoin,
            market_catalog_version::Relation::CatalogSyncBatch.def(),
        )
        .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
        .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
        .filter(market_catalog_version::Column::MarketId.eq(market_id.clone()))
        .filter(
            market_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .filter(
            event_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(event_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .order_by_desc(market_catalog_version::Column::SourceEffectiveAt)
        .order_by_desc(market_catalog_version::Column::AvailableAt)
        .order_by_desc(market_catalog_version::Column::MarketCatalogVersionId)
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn latest_joined_markets<C>(
    db: &C,
    boundary: &DecisionBoundary,
) -> Result<
    Vec<(
        market_catalog_version::Model,
        Option<event_catalog_version::Model>,
    )>,
    StorageError,
>
where
    C: ConnectionTrait,
{
    market_catalog_version::Entity::find()
        .find_also_related(event_catalog_version::Entity)
        .join(
            JoinType::InnerJoin,
            market_catalog_version::Relation::CatalogSyncBatch.def(),
        )
        .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
        .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
        .filter(
            market_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .filter(
            event_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(event_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .distinct_on([market_catalog_version::Column::MarketId])
        .order_by_asc(market_catalog_version::Column::MarketId)
        .order_by_desc(market_catalog_version::Column::SourceEffectiveAt)
        .order_by_desc(market_catalog_version::Column::AvailableAt)
        .order_by_desc(market_catalog_version::Column::MarketCatalogVersionId)
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn exact_event_markets_at<C>(
    db: &C,
    event_version_id: &EventCatalogVersionId,
    market_ids: &[MarketId],
    boundary: &DecisionBoundary,
) -> Result<Vec<market_catalog_version::Model>, StorageError>
where
    C: ConnectionTrait,
{
    if market_ids.is_empty() {
        return Ok(Vec::new());
    }
    market_catalog_version::Entity::find()
        .join(
            JoinType::InnerJoin,
            market_catalog_version::Relation::CatalogSyncBatch.def(),
        )
        .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
        .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
        .filter(market_catalog_version::Column::MarketId.is_in(market_ids.to_vec()))
        .filter(market_catalog_version::Column::EventCatalogVersionId.eq(event_version_id.clone()))
        .filter(
            market_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .order_by_asc(market_catalog_version::Column::MarketId)
        .all(db)
        .await
        .map_err(StorageError::from)
}

async fn joined_market_history<C>(
    db: &C,
    market_ids: &[MarketId],
    boundary: &DecisionBoundary,
) -> Result<
    Vec<(
        market_catalog_version::Model,
        Option<event_catalog_version::Model>,
    )>,
    StorageError,
>
where
    C: ConnectionTrait,
{
    if market_ids.is_empty() {
        return Ok(Vec::new());
    }
    market_catalog_version::Entity::find()
        .find_also_related(event_catalog_version::Entity)
        .join(
            JoinType::InnerJoin,
            market_catalog_version::Relation::CatalogSyncBatch.def(),
        )
        .filter(catalog_sync_batch::Column::Status.eq(CatalogSyncStatus::Committed.as_str()))
        .filter(catalog_sync_batch::Column::CommittedAt.lte(boundary.decision_at()))
        .filter(market_catalog_version::Column::MarketId.is_in(market_ids.to_vec()))
        .filter(
            market_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(market_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .filter(
            event_catalog_version::Column::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
        .filter(event_catalog_version::Column::AvailableAt.lte(boundary.decision_at()))
        .order_by_asc(market_catalog_version::Column::MarketId)
        .order_by_asc(market_catalog_version::Column::SourceEffectiveAt)
        .order_by_asc(market_catalog_version::Column::AvailableAt)
        .order_by_asc(market_catalog_version::Column::MarketCatalogVersionId)
        .all(db)
        .await
        .map_err(StorageError::from)
}
