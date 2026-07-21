//! Atomic Gamma catalog object/change ledger writer and point-in-time reader.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        data_plane::{DecisionBoundary, DecisionSource},
        market::{
            CATALOG_OBJECT_SCHEMA_VERSION, CatalogBatchChainInfo, CatalogBatchCommit,
            CatalogBatchFailure, CatalogEventCandidate, CatalogEventChangeInfo,
            CatalogMarketCandidate, CatalogMarketChangeInfo, CatalogSnapshotInfo,
            CatalogSyncBatchInfo, CatalogWindowInfo, EventRegistryInfo, EventTags,
            MarketRegistryInfo, NewCatalogEventChange, NewCatalogEventObject,
            NewCatalogMarketChange, NewCatalogMarketObject, NewCatalogSyncBatch,
            NewCatalogSyncRejection, UpsertEvent, UpsertMarket,
        },
    },
    entities::{
        catalog_event_change::{Column, Entity, Model as CatalogEventChangeModel},
        catalog_event_object::{
            Column as CatalogEventObjectColumn, Entity as CatalogEventObjectEntity,
        },
        catalog_market_change::{
            Column as CatalogMarketChangeColumn, Entity as CatalogMarketChangeEntity,
            Model as CatalogMarketChangeModel,
        },
        catalog_market_object::{
            Column as CatalogMarketObjectColumn, Entity as CatalogMarketObjectEntity,
        },
        catalog_sync_batch::{
            ActiveModel, Column as CatalogSyncBatchColumn, Entity as CatalogSyncBatchEntity, Model,
        },
        catalog_sync_rejection::{
            Column as CatalogSyncRejectionColumn, Entity as CatalogSyncRejectionEntity,
        },
    },
    enums::catalog::{CatalogSyncKind, CatalogSyncStatus, CatalogTimestampQuality},
    hashing::CanonicalDigest,
    types::{
        CatalogEventChangeId, CatalogEventObjectId, CatalogMarketIds, CatalogMarketObjectId,
        CatalogSyncBatchId, EventId, HistoryCoverage, MarketId,
    },
};
use sea_orm::{
    AccessMode, ActiveValue, ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, DbErr, EntityTrait, FromQueryResult, IsolationLevel, QueryFilter,
    QueryOrder, QuerySelect, Select, TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::{
        catalog::{event::PgEventRepository, market::PgMarketRepository},
        primitives,
        write::upsert_many_chunked,
    },
    traits::{CatalogLedgerRepository, EventRepository, MarketRepository},
};

const CATALOG_WRITER_LOCK_ID: i64 = 7_460_991_152_318_744_201;
const MAX_FAILURE_DETAIL_BYTES: usize = 2_048;

fn catalog_candidate_error(detail: impl Display) -> StorageError {
    StorageError::invariant_violation(Some("catalog_sync_batch"), detail)
}

fn validate_and_normalize_candidates(
    batch_id: &CatalogSyncBatchId,
    events: &mut [CatalogEventCandidate],
    markets: &mut [CatalogMarketCandidate],
) -> Result<(), StorageError> {
    let mut event_objects = BTreeMap::new();
    for candidate in events {
        if candidate.object.schema_version != CATALOG_OBJECT_SCHEMA_VERSION {
            return Err(catalog_candidate_error(format!(
                "event {} uses unsupported object schema version {}",
                candidate.change.event_id, candidate.object.schema_version
            )));
        }
        let content_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/catalog-event-object",
            u32::try_from(candidate.object.schema_version).map_err(|_| {
                catalog_candidate_error("catalog event schema version must be non-negative")
            })?,
            &candidate.object.payload,
        )
        .map_err(|error| catalog_candidate_error(format!("hash catalog event object: {error}")))?;
        let object_id = CatalogEventObjectId::from_content_hash(&content_hash);
        let source = serde_json::from_value::<EventRegistryInfo>(
            candidate.object.payload.clone().into_inner(),
        )
        .map_err(|error| {
            catalog_candidate_error(format!("decode typed catalog event payload: {error}"))
        })?;
        if candidate.object.content_hash != content_hash
            || candidate.object.event_object_id != object_id
            || candidate.change.event_object_id != object_id
            || candidate.change.catalog_sync_batch_id != *batch_id
            || candidate.change.event_id != source.event_id
        {
            return Err(catalog_candidate_error(format!(
                "event {} object id, hash, batch, and typed payload identity disagree",
                source.event_id
            )));
        }
        if event_objects
            .insert(source.event_id.clone(), object_id)
            .is_some()
        {
            return Err(catalog_candidate_error(format!(
                "duplicate event {} in one catalog batch",
                source.event_id
            )));
        }
        candidate.projection = UpsertEvent {
            event_id: source.event_id,
            title: source.title,
            slug: source.slug,
            series_slug: source.series_slug,
            status: source.status,
            tags: EventTags(source.tags),
            neg_risk: source.neg_risk,
            catalog_market_ids: CatalogMarketIds(source.market_ids),
            end_date: source.end_date,
            content_hash,
        };
    }

    let mut market_ids = BTreeSet::new();
    for candidate in markets {
        if candidate.object.schema_version != CATALOG_OBJECT_SCHEMA_VERSION {
            return Err(catalog_candidate_error(format!(
                "market {} uses unsupported object schema version {}",
                candidate.projection.market_id, candidate.object.schema_version
            )));
        }
        let content_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/catalog-market-object",
            u32::try_from(candidate.object.schema_version).map_err(|_| {
                catalog_candidate_error("catalog market schema version must be non-negative")
            })?,
            &candidate.object.payload,
        )
        .map_err(|error| catalog_candidate_error(format!("hash catalog market object: {error}")))?;
        let object_id = CatalogMarketObjectId::from_content_hash(&content_hash);
        let source = serde_json::from_value::<MarketRegistryInfo>(
            candidate.object.payload.clone().into_inner(),
        )
        .map_err(|error| {
            catalog_candidate_error(format!("decode typed catalog market payload: {error}"))
        })?;
        let expected_event_object = event_objects.get(&source.event_id).ok_or_else(|| {
            catalog_candidate_error(format!(
                "market {} references event {} absent from the same full scan",
                source.market_id, source.event_id
            ))
        })?;
        if candidate.object.content_hash != content_hash
            || candidate.object.market_object_id != object_id
            || candidate.catalog_sync_batch_id != *batch_id
            || candidate.event_object_id != *expected_event_object
        {
            return Err(catalog_candidate_error(format!(
                "market {} object id, hash, batch, and event identity disagree",
                source.market_id
            )));
        }
        if !market_ids.insert(source.market_id.clone()) {
            return Err(catalog_candidate_error(format!(
                "duplicate market {} in one catalog batch",
                source.market_id
            )));
        }
        let mut projection = UpsertMarket::from_registry(&source).map_err(|error| {
            catalog_candidate_error(format!("normalize typed catalog market payload: {error}"))
        })?;
        projection.content_hash = content_hash;
        candidate.projection = projection;
    }
    Ok(())
}

/// `PostgreSQL` implementation of the immutable catalog object/change ledger.
pub struct PgCatalogLedgerRepository {
    db: DatabaseConnection,
}

impl PgCatalogLedgerRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn begin_catalog_read(&self) -> Result<DatabaseTransaction, StorageError> {
        self.db
            .begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait::async_trait]
impl CatalogLedgerRepository for PgCatalogLedgerRepository {
    async fn research_history_coverage(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<HistoryCoverage>, StorageError> {
        let event = Entity::find()
            .select_only()
            .column_as(Column::SourceEffectiveAt.min(), "earliest_event_time")
            .column_as(Column::SourceEffectiveAt.max(), "latest_event_time")
            .column_as(Column::EventChangeId.count(), "row_count")
            .filter(Column::SourceEffectiveAt.lte(as_of))
            .into_model::<HistoryRangeRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .unwrap_or_default();
        let market = CatalogMarketChangeEntity::find()
            .select_only()
            .column_as(
                CatalogMarketChangeColumn::SourceEffectiveAt.min(),
                "earliest_event_time",
            )
            .column_as(
                CatalogMarketChangeColumn::SourceEffectiveAt.max(),
                "latest_event_time",
            )
            .column_as(
                CatalogMarketChangeColumn::MarketChangeId.count(),
                "row_count",
            )
            .filter(CatalogMarketChangeColumn::SourceEffectiveAt.lte(as_of))
            .into_model::<HistoryRangeRow>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .unwrap_or_default();
        Ok(vec![
            history_coverage("catalog_event_change", &event)?,
            history_coverage("catalog_market_change", &market)?,
        ])
    }

    async fn commit(
        &self,
        commit: CatalogBatchCommit,
    ) -> Result<CatalogSyncBatchInfo, StorageError> {
        let CatalogBatchCommit {
            batch,
            mut events,
            mut markets,
        } = commit;
        let batch_id = batch.catalog_sync_batch_id.clone();
        if batch.rejected_count != 0 {
            return Err(StorageError::InvariantViolation {
                entity: Some("catalog_sync_batch"),
                detail: "a committed catalog batch cannot contain rejected input rows".to_owned(),
            });
        }
        validate_and_normalize_candidates(&batch_id, &mut events, &mut markets)?;

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_catalog_writer_lock(&txn).await?;
        let baseline_exists = CatalogSyncBatchEntity::find()
            .filter(CatalogSyncBatchColumn::SyncKind.eq(CatalogSyncKind::Baseline))
            .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_some();
        match (baseline_exists, batch.sync_kind) {
            (false, CatalogSyncKind::Reconcile) => {
                return Err(StorageError::state_conflict(
                    "catalog_sync_batch",
                    Some(&batch_id),
                    "the first committed catalog batch must be a baseline",
                ));
            }
            (true, CatalogSyncKind::Baseline) => {
                return Err(StorageError::state_conflict(
                    "catalog_sync_batch",
                    Some(&batch_id),
                    "catalog coverage already has a committed baseline",
                ));
            }
            (false, CatalogSyncKind::Baseline) | (true, CatalogSyncKind::Reconcile) => {}
        }
        let committed_at = primitives::statement_timestamp(&txn).await?;
        let batch_model = insert_committed_batch(&txn, batch, committed_at).await?;

        let event_repo = PgEventRepository::with_txn(&txn);
        let current_events = event_repo
            .find_by_ids(
                &events
                    .iter()
                    .map(|candidate| candidate.projection.event_id.clone())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .map(|event| (event.event_id, event.content_hash))
            .collect::<BTreeMap<_, _>>();

        let mut changed_event_ids = BTreeSet::new();
        let mut event_objects = Vec::new();
        let mut event_changes = Vec::new();
        let mut event_projections = Vec::new();
        let mut event_change_by_object = BTreeMap::new();
        let mut unchanged_event_objects = Vec::new();
        for mut candidate in events {
            normalize_event_fallback(&mut candidate.change, committed_at);
            let changed = current_events.get(&candidate.projection.event_id)
                != Some(&candidate.projection.content_hash);
            if changed {
                changed_event_ids.insert(candidate.projection.event_id.clone());
                event_change_by_object.insert(
                    candidate.object.event_object_id.clone(),
                    candidate.change.event_change_id.clone(),
                );
                event_objects.push(candidate.object);
                event_changes.push(candidate.change);
                event_projections.push(candidate.projection);
            } else {
                unchanged_event_objects.push(candidate.object.event_object_id);
            }
        }

        insert_event_objects(&txn, event_objects).await?;
        insert_event_changes(&txn, event_changes).await?;
        if !event_projections.is_empty() {
            event_repo.upsert_batch(event_projections).await?;
        }
        event_change_by_object
            .extend(latest_event_changes_by_object(&txn, unchanged_event_objects).await?);

        let market_repo = PgMarketRepository::with_txn(&txn);
        let current_markets = market_repo
            .find_by_ids(
                &markets
                    .iter()
                    .map(|candidate| candidate.projection.market_id.clone())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .map(|market| (market.market_id.clone(), market.content_hash.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut market_objects = Vec::new();
        let mut market_changes = Vec::new();
        let mut market_projections = Vec::new();
        for mut candidate in markets {
            let object_changed = current_markets.get(&candidate.projection.market_id)
                != Some(&candidate.projection.content_hash);
            let parent_event_changed = changed_event_ids.contains(&candidate.projection.event_id);
            if !object_changed && !parent_event_changed {
                continue;
            }
            let event_change_id = event_change_by_object
                .get(&candidate.event_object_id)
                .cloned()
                .ok_or_else(|| StorageError::InvariantViolation {
                    entity: Some("catalog_event_change"),
                    detail: format!(
                        "event object {} has no committed change for market {}",
                        candidate.event_object_id, candidate.projection.market_id
                    ),
                })?;
            if candidate.source_timestamp_quality == CatalogTimestampQuality::CommitTimeFallback {
                candidate.source_effective_at = committed_at;
            }
            let market_object_id = candidate.object.market_object_id.clone();
            market_objects.push(candidate.object);
            market_changes.push(NewCatalogMarketChange {
                market_change_id: candidate.market_change_id,
                catalog_sync_batch_id: candidate.catalog_sync_batch_id,
                event_change_id,
                market_object_id,
                market_id: candidate.projection.market_id.clone(),
                event_id: candidate.projection.event_id.clone(),
                source_effective_at: candidate.source_effective_at,
                source_timestamp_quality: candidate.source_timestamp_quality,
                source_created_at: candidate.source_created_at,
                change_type: candidate.change_type,
            });
            if object_changed {
                market_projections.push(candidate.projection);
            }
        }

        insert_market_objects(&txn, market_objects).await?;
        insert_market_changes(&txn, market_changes).await?;
        if !market_projections.is_empty() {
            market_repo.upsert_batch(market_projections).await?;
        }

        match txn.commit().await {
            Ok(()) => Ok(batch_model.into()),
            Err(commit_error) => recover_commit_outcome(&self.db, &batch_id, commit_error).await,
        }
    }

    async fn record_failure(
        &self,
        failure: CatalogBatchFailure,
    ) -> Result<CatalogSyncBatchInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        acquire_catalog_writer_lock(&txn).await?;
        if let Some(existing) =
            CatalogSyncBatchEntity::find_by_id(failure.catalog_sync_batch_id.clone())
                .one(&txn)
                .await
                .map_err(StorageError::from)?
        {
            if existing.status != CatalogSyncStatus::Failed {
                return Err(StorageError::IllegalTransition {
                    entity: "catalog_sync_batch",
                    id: Some(failure.catalog_sync_batch_id.to_string()),
                    from: existing.status.to_string(),
                    to: CatalogSyncStatus::Failed.to_string(),
                });
            }
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(existing.into());
        }

        let detail = bounded_failure_detail(&failure.failure_detail);
        let model = CatalogSyncBatchEntity::insert(ActiveModel {
            catalog_sync_batch_id: Set(failure.catalog_sync_batch_id),
            sync_kind: Set(failure.sync_kind),
            status: Set(CatalogSyncStatus::Failed),
            started_at: Set(failure.started_at),
            fetched_at: Set(failure.fetched_at),
            committed_at: Set(None),
            event_count: Set(0),
            market_count: Set(0),
            rejected_count: Set(i64::try_from(failure.rejections.len()).map_err(|_| {
                StorageError::InvariantViolation {
                    entity: Some("catalog_sync_rejection"),
                    detail: "rejection count exceeds i64".to_owned(),
                }
            })?),
            batch_hash: Set(None),
            failure_stage: Set(Some(failure.failure_stage)),
            failure_detail: Set(Some(detail)),
            created_at: ActiveValue::NotSet,
            updated_at: ActiveValue::NotSet,
        })
        .exec_with_returning(&txn)
        .await
        .map_err(StorageError::from)?;
        insert_rejections(&txn, failure.rejections).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(model.into())
    }

    async fn coverage_start(&self) -> Result<Option<DateTime<Utc>>, StorageError> {
        catalog_coverage_start(&self.db).await
    }

    async fn watermark(&self) -> Result<Option<DateTime<Utc>>, StorageError> {
        catalog_watermark(&self.db).await
    }

    async fn batch_chain(
        &self,
        window_start: DateTime<Utc>,
        pit_cutoff: DateTime<Utc>,
    ) -> Result<Option<CatalogBatchChainInfo>, StorageError> {
        if window_start > pit_cutoff {
            return Err(StorageError::InvariantViolation {
                entity: Some("catalog_sync_batch"),
                detail: "catalog proof window_start cannot be after pit_cutoff".to_owned(),
            });
        }
        let baseline = CatalogSyncBatchEntity::find()
            .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
            .filter(CatalogSyncBatchColumn::SyncKind.eq(CatalogSyncKind::Baseline))
            .filter(CatalogSyncBatchColumn::CommittedAt.lte(window_start))
            .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
            .order_by_desc(CatalogSyncBatchColumn::CatalogSyncBatchId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        let Some(baseline) = baseline else {
            return Ok(None);
        };
        let baseline_at = baseline.committed_at.ok_or_else(|| {
            StorageError::invariant_violation(
                Some("catalog_sync_batch"),
                "committed catalog baseline has no committed_at",
            )
        })?;
        let rows = CatalogSyncBatchEntity::find()
            .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
            .filter(CatalogSyncBatchColumn::CommittedAt.gte(baseline_at))
            .filter(CatalogSyncBatchColumn::CommittedAt.lte(pit_cutoff))
            .order_by_asc(CatalogSyncBatchColumn::CommittedAt)
            .order_by_asc(CatalogSyncBatchColumn::CatalogSyncBatchId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(Some(CatalogBatchChainInfo {
            batches: rows.into_iter().map(Into::into).collect(),
        }))
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogMarketChangeInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let row = market_at_txn(&txn, market_id, boundary).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(row)
    }

    async fn markets_at(
        &self,
        market_ids: &[MarketId],
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let rows = markets_at_txn(&txn, market_ids, boundary).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(rows)
    }

    async fn event_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogEventChangeInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let row = event_at_txn(&txn, event_id, boundary).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(row)
    }

    async fn event_markets_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let event = event_at_txn(&txn, event_id, boundary).await?;
        let rows = match event {
            Some(event) => exact_event_markets_at(&txn, &event.event_change_id, boundary).await?,
            None => Vec::new(),
        };
        txn.commit().await.map_err(StorageError::from)?;
        Ok(rows)
    }

    async fn snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogSnapshotInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let Some(market) = market_at_txn(&txn, market_id, boundary).await? else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let event = event_by_change_id(&txn, &market.event_change_id, boundary)
            .await?
            .ok_or_else(|| StorageError::InvariantViolation {
                entity: Some("catalog_market_change"),
                detail: format!(
                    "market change {} references unavailable event change {}",
                    market.market_change_id, market.event_change_id
                ),
            })?;
        let event_markets = exact_event_markets_at(&txn, &event.event_change_id, boundary).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(CatalogSnapshotInfo {
            market,
            event,
            event_markets,
        }))
    }

    async fn snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogSnapshotInfo>, StorageError> {
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, boundary).await?;
        let markets = all_markets_at(&txn, boundary).await?;
        let event_change_ids = markets
            .iter()
            .map(|market| market.event_change_id.clone())
            .collect::<BTreeSet<_>>();
        let event_cache = events_by_change_ids(&txn, &event_change_ids, boundary)
            .await?
            .into_iter()
            .map(|event| (event.event_change_id.clone(), event))
            .collect::<BTreeMap<_, _>>();
        let mut member_cache = BTreeMap::<CatalogEventChangeId, Vec<_>>::new();
        for member in event_members_by_change_ids(&txn, &event_change_ids, boundary).await? {
            member_cache
                .entry(member.event_change_id.clone())
                .or_default()
                .push(member);
        }
        let mut snapshots = Vec::with_capacity(markets.len());
        for market in markets {
            let event_change_id = market.event_change_id.clone();
            let event = event_cache.get(&event_change_id).ok_or_else(|| {
                StorageError::InvariantViolation {
                    entity: Some("catalog_market_change"),
                    detail: format!("missing event change {event_change_id}"),
                }
            })?;
            snapshots.push(CatalogSnapshotInfo {
                market,
                event: event.clone(),
                event_markets: member_cache
                    .get(&event_change_id)
                    .cloned()
                    .unwrap_or_default(),
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
        if market_ids.is_empty() {
            return Ok(CatalogWindowInfo {
                market_changes: Vec::new(),
                event_changes: Vec::new(),
            });
        }
        let txn = self.begin_catalog_read().await?;
        ensure_coverage(&txn, end_boundary).await?;
        let market_changes = market_history_through(&txn, market_ids, end_boundary).await?;
        let event_ids = market_changes
            .iter()
            .map(|market| market.event_change_id.clone())
            .collect::<BTreeSet<_>>();
        let event_changes = events_by_change_ids(&txn, &event_ids, end_boundary).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(CatalogWindowInfo {
            market_changes,
            event_changes,
        })
    }
}

#[derive(Debug, Default, FromQueryResult)]
struct HistoryRangeRow {
    earliest_event_time: Option<DateTime<Utc>>,
    latest_event_time: Option<DateTime<Utc>>,
    row_count: i64,
}

fn history_coverage(
    object: &'static str,
    row: &HistoryRangeRow,
) -> Result<HistoryCoverage, StorageError> {
    Ok(HistoryCoverage {
        object: object.to_owned(),
        time_column: "source_effective_at".to_owned(),
        earliest_event_time: row.earliest_event_time,
        latest_event_time: row.latest_event_time,
        row_count: u64::try_from(row.row_count).map_err(|error| {
            StorageError::invariant_violation(Some(object), format!("negative row count: {error}"))
        })?,
    })
}

async fn insert_event_objects(
    txn: &DatabaseTransaction,
    objects: Vec<NewCatalogEventObject>,
) -> Result<(), StorageError> {
    if objects.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<CatalogEventObjectEntity, NewCatalogEventObject>(
        txn,
        objects,
        OnConflict::column(CatalogEventObjectColumn::EventObjectId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn insert_event_changes(
    txn: &DatabaseTransaction,
    changes: Vec<NewCatalogEventChange>,
) -> Result<(), StorageError> {
    if changes.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<Entity, NewCatalogEventChange>(
        txn,
        changes,
        OnConflict::column(Column::EventChangeId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn insert_market_objects(
    txn: &DatabaseTransaction,
    objects: Vec<NewCatalogMarketObject>,
) -> Result<(), StorageError> {
    if objects.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<CatalogMarketObjectEntity, NewCatalogMarketObject>(
        txn,
        objects,
        OnConflict::column(CatalogMarketObjectColumn::MarketObjectId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn insert_market_changes(
    txn: &DatabaseTransaction,
    changes: Vec<NewCatalogMarketChange>,
) -> Result<(), StorageError> {
    if changes.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<CatalogMarketChangeEntity, NewCatalogMarketChange>(
        txn,
        changes,
        OnConflict::column(CatalogMarketChangeColumn::MarketChangeId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn insert_rejections(
    txn: &DatabaseTransaction,
    rejections: Vec<NewCatalogSyncRejection>,
) -> Result<(), StorageError> {
    if rejections.is_empty() {
        return Ok(());
    }
    upsert_many_chunked::<CatalogSyncRejectionEntity, NewCatalogSyncRejection>(
        txn,
        rejections,
        OnConflict::column(CatalogSyncRejectionColumn::CatalogSyncRejectionId)
            .do_nothing()
            .to_owned(),
    )
    .await?;
    Ok(())
}

async fn latest_event_changes_by_object(
    txn: &DatabaseTransaction,
    object_ids: Vec<CatalogEventObjectId>,
) -> Result<BTreeMap<CatalogEventObjectId, CatalogEventChangeId>, StorageError> {
    if object_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = Entity::find()
        .filter(Column::EventObjectId.is_in(object_ids))
        .distinct_on([Column::EventObjectId])
        .order_by_asc(Column::EventObjectId)
        .order_by_desc(Column::CreatedAt)
        .order_by_desc(Column::EventChangeId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    Ok(rows
        .into_iter()
        .map(|row| (row.event_object_id, row.event_change_id))
        .collect())
}

fn normalize_event_fallback(change: &mut NewCatalogEventChange, committed_at: DateTime<Utc>) {
    if change.source_timestamp_quality == CatalogTimestampQuality::CommitTimeFallback {
        change.source_effective_at = committed_at;
    }
}

async fn insert_committed_batch(
    txn: &DatabaseTransaction,
    batch: NewCatalogSyncBatch,
    committed_at: DateTime<Utc>,
) -> Result<Model, StorageError> {
    CatalogSyncBatchEntity::insert(ActiveModel {
        catalog_sync_batch_id: Set(batch.catalog_sync_batch_id),
        sync_kind: Set(batch.sync_kind),
        status: Set(CatalogSyncStatus::Committed),
        started_at: Set(batch.started_at),
        fetched_at: Set(Some(batch.fetched_at)),
        committed_at: Set(Some(committed_at)),
        event_count: Set(batch.event_count),
        market_count: Set(batch.market_count),
        rejected_count: Set(0),
        batch_hash: Set(Some(batch.batch_hash)),
        failure_stage: Set(None),
        failure_detail: Set(None),
        created_at: ActiveValue::NotSet,
        updated_at: ActiveValue::NotSet,
    })
    .exec_with_returning(txn)
    .await
    .map_err(StorageError::from)
}

async fn catalog_coverage_start(
    db: &impl ConnectionTrait,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    CatalogSyncBatchEntity::find()
        .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
        .filter(CatalogSyncBatchColumn::SyncKind.eq(CatalogSyncKind::Baseline))
        .order_by_asc(CatalogSyncBatchColumn::CommittedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.and_then(|model| model.committed_at))
}

async fn catalog_watermark(
    db: &impl ConnectionTrait,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    CatalogSyncBatchEntity::find()
        .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
        .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
        .one(db)
        .await
        .map_err(StorageError::from)
        .map(|row| row.and_then(|model| model.committed_at))
}

async fn ensure_coverage(
    txn: &DatabaseTransaction,
    boundary: &DecisionBoundary,
) -> Result<(), StorageError> {
    let coverage = catalog_coverage_start(txn).await?;
    match coverage {
        None => {
            return Err(StorageError::StateConflict {
                entity: "catalog_sync_batch",
                id: None,
                detail: "catalog has no committed baseline".to_owned(),
            });
        }
        Some(start) if start > boundary.decision_at() => {
            return Err(StorageError::StaleData(format!(
                "catalog decision_at={} predates coverage start={start}",
                boundary.decision_at()
            )));
        }
        Some(_) => {}
    }
    Ok(())
}

async fn market_at_txn(
    txn: &DatabaseTransaction,
    market_id: &MarketId,
    boundary: &DecisionBoundary,
) -> Result<Option<CatalogMarketChangeInfo>, StorageError> {
    let changes = visible_market_changes(boundary)
        .filter(CatalogMarketChangeColumn::MarketId.eq(market_id.clone()))
        .order_by_desc(CatalogMarketChangeColumn::SourceEffectiveAt)
        .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
        .order_by_desc(CatalogMarketChangeColumn::MarketChangeId)
        .limit(1)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    Ok(hydrate_market_changes(txn, changes).await?.pop())
}

async fn markets_at_txn(
    txn: &DatabaseTransaction,
    market_ids: &[MarketId],
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    let changes = visible_market_changes(boundary)
        .filter(CatalogMarketChangeColumn::MarketId.is_in(market_ids.iter().cloned()))
        .distinct_on([(
            CatalogMarketChangeEntity,
            CatalogMarketChangeColumn::MarketId,
        )])
        .order_by_asc(CatalogMarketChangeColumn::MarketId)
        .order_by_desc(CatalogMarketChangeColumn::SourceEffectiveAt)
        .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
        .order_by_desc(CatalogMarketChangeColumn::MarketChangeId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_market_changes(txn, changes).await
}

async fn event_at_txn(
    txn: &DatabaseTransaction,
    event_id: &EventId,
    boundary: &DecisionBoundary,
) -> Result<Option<CatalogEventChangeInfo>, StorageError> {
    let changes = visible_event_changes(boundary)
        .filter(Column::EventId.eq(event_id.clone()))
        .order_by_desc(Column::SourceEffectiveAt)
        .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
        .order_by_desc(Column::EventChangeId)
        .limit(1)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    Ok(hydrate_event_changes(txn, changes).await?.pop())
}

async fn event_by_change_id(
    txn: &DatabaseTransaction,
    event_change_id: &CatalogEventChangeId,
    boundary: &DecisionBoundary,
) -> Result<Option<CatalogEventChangeInfo>, StorageError> {
    let changes = visible_event_changes(boundary)
        .filter(Column::EventChangeId.eq(event_change_id.clone()))
        .limit(1)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    Ok(hydrate_event_changes(txn, changes).await?.pop())
}

async fn exact_event_markets_at(
    txn: &DatabaseTransaction,
    event_change_id: &CatalogEventChangeId,
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    let changes = visible_market_changes(boundary)
        .filter(CatalogMarketChangeColumn::EventChangeId.eq(event_change_id.clone()))
        .order_by_asc(CatalogMarketChangeColumn::MarketId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_market_changes(txn, changes).await
}

async fn events_by_change_ids(
    txn: &DatabaseTransaction,
    event_change_ids: &BTreeSet<CatalogEventChangeId>,
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogEventChangeInfo>, StorageError> {
    if event_change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let changes = visible_event_changes(boundary)
        .filter(Column::EventChangeId.is_in(event_change_ids.iter().cloned()))
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_event_changes(txn, changes).await
}

async fn event_members_by_change_ids(
    txn: &DatabaseTransaction,
    event_change_ids: &BTreeSet<CatalogEventChangeId>,
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    if event_change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let changes = visible_market_changes(boundary)
        .filter(CatalogMarketChangeColumn::EventChangeId.is_in(event_change_ids.iter().cloned()))
        .order_by_asc(CatalogMarketChangeColumn::EventChangeId)
        .order_by_asc(CatalogMarketChangeColumn::MarketId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_market_changes(txn, changes).await
}

async fn all_markets_at(
    txn: &DatabaseTransaction,
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    let changes = visible_market_changes(boundary)
        .distinct_on([(
            CatalogMarketChangeEntity,
            CatalogMarketChangeColumn::MarketId,
        )])
        .order_by_asc(CatalogMarketChangeColumn::MarketId)
        .order_by_desc(CatalogMarketChangeColumn::SourceEffectiveAt)
        .order_by_desc(CatalogSyncBatchColumn::CommittedAt)
        .order_by_desc(CatalogMarketChangeColumn::MarketChangeId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_market_changes(txn, changes).await
}

async fn market_history_through(
    txn: &DatabaseTransaction,
    market_ids: &[MarketId],
    boundary: &DecisionBoundary,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    let changes = visible_market_changes(boundary)
        .filter(CatalogMarketChangeColumn::MarketId.is_in(market_ids.iter().cloned()))
        .order_by_asc(CatalogMarketChangeColumn::MarketId)
        .order_by_asc(CatalogMarketChangeColumn::SourceEffectiveAt)
        .order_by_asc(CatalogSyncBatchColumn::CommittedAt)
        .order_by_asc(CatalogMarketChangeColumn::MarketChangeId)
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    hydrate_market_changes(txn, changes).await
}

fn visible_event_changes(boundary: &DecisionBoundary) -> Select<Entity> {
    Entity::find()
        .inner_join(CatalogSyncBatchEntity)
        .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
        .filter(CatalogSyncBatchColumn::CommittedAt.lte(boundary.decision_at()))
        .filter(Column::SourceEffectiveAt.lte(boundary.cutoff_for(DecisionSource::Catalog)))
}

fn visible_market_changes(boundary: &DecisionBoundary) -> Select<CatalogMarketChangeEntity> {
    CatalogMarketChangeEntity::find()
        .inner_join(CatalogSyncBatchEntity)
        .filter(CatalogSyncBatchColumn::Status.eq(CatalogSyncStatus::Committed))
        .filter(CatalogSyncBatchColumn::CommittedAt.lte(boundary.decision_at()))
        .filter(
            CatalogMarketChangeColumn::SourceEffectiveAt
                .lte(boundary.cutoff_for(DecisionSource::Catalog)),
        )
}

async fn hydrate_event_changes(
    db: &impl ConnectionTrait,
    changes: Vec<CatalogEventChangeModel>,
) -> Result<Vec<CatalogEventChangeInfo>, StorageError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let object_ids = changes
        .iter()
        .map(|change| change.event_object_id.clone())
        .collect::<BTreeSet<_>>();
    let batch_ids = changes
        .iter()
        .map(|change| change.catalog_sync_batch_id.clone())
        .collect::<BTreeSet<_>>();
    let objects = CatalogEventObjectEntity::find()
        .filter(CatalogEventObjectColumn::EventObjectId.is_in(object_ids))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|object| (object.event_object_id.clone(), object))
        .collect::<BTreeMap<_, _>>();
    let batches = load_batches(db, batch_ids).await?;
    changes
        .into_iter()
        .map(|change| {
            let object = objects.get(&change.event_object_id).ok_or_else(|| {
                StorageError::InvariantViolation {
                    entity: Some("catalog_event_change"),
                    detail: format!(
                        "event change {} references missing object {}",
                        change.event_change_id, change.event_object_id
                    ),
                }
            })?;
            let available_at = committed_at_for(&batches, &change.catalog_sync_batch_id)?;
            Ok(CatalogEventChangeInfo {
                event_change_id: change.event_change_id,
                catalog_sync_batch_id: change.catalog_sync_batch_id,
                event_object_id: change.event_object_id,
                event_id: change.event_id,
                source_effective_at: change.source_effective_at,
                source_timestamp_quality: change.source_timestamp_quality,
                available_at,
                change_type: change.change_type,
                content_hash: object.content_hash.clone(),
                schema_version: object.schema_version,
                payload: object.payload.clone(),
                created_at: change.created_at,
            })
        })
        .collect()
}

async fn hydrate_market_changes(
    db: &impl ConnectionTrait,
    changes: Vec<CatalogMarketChangeModel>,
) -> Result<Vec<CatalogMarketChangeInfo>, StorageError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let object_ids = changes
        .iter()
        .map(|change| change.market_object_id.clone())
        .collect::<BTreeSet<_>>();
    let batch_ids = changes
        .iter()
        .map(|change| change.catalog_sync_batch_id.clone())
        .collect::<BTreeSet<_>>();
    let objects = CatalogMarketObjectEntity::find()
        .filter(CatalogMarketObjectColumn::MarketObjectId.is_in(object_ids))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|object| (object.market_object_id.clone(), object))
        .collect::<BTreeMap<_, _>>();
    let batches = load_batches(db, batch_ids).await?;
    changes
        .into_iter()
        .map(|change| {
            let object = objects.get(&change.market_object_id).ok_or_else(|| {
                StorageError::InvariantViolation {
                    entity: Some("catalog_market_change"),
                    detail: format!(
                        "market change {} references missing object {}",
                        change.market_change_id, change.market_object_id
                    ),
                }
            })?;
            let available_at = committed_at_for(&batches, &change.catalog_sync_batch_id)?;
            Ok(CatalogMarketChangeInfo {
                market_change_id: change.market_change_id,
                catalog_sync_batch_id: change.catalog_sync_batch_id,
                event_change_id: change.event_change_id,
                market_object_id: change.market_object_id,
                market_id: change.market_id,
                event_id: change.event_id,
                source_effective_at: change.source_effective_at,
                source_timestamp_quality: change.source_timestamp_quality,
                source_created_at: change.source_created_at,
                available_at,
                change_type: change.change_type,
                content_hash: object.content_hash.clone(),
                schema_version: object.schema_version,
                payload: object.payload.clone(),
                created_at: change.created_at,
            })
        })
        .collect()
}

async fn load_batches(
    db: &impl ConnectionTrait,
    batch_ids: BTreeSet<CatalogSyncBatchId>,
) -> Result<BTreeMap<CatalogSyncBatchId, Model>, StorageError> {
    CatalogSyncBatchEntity::find()
        .filter(CatalogSyncBatchColumn::CatalogSyncBatchId.is_in(batch_ids))
        .all(db)
        .await
        .map_err(StorageError::from)
        .map(|batches| {
            batches
                .into_iter()
                .map(|batch| (batch.catalog_sync_batch_id.clone(), batch))
                .collect()
        })
}

fn committed_at_for(
    batches: &BTreeMap<CatalogSyncBatchId, Model>,
    batch_id: &CatalogSyncBatchId,
) -> Result<DateTime<Utc>, StorageError> {
    let batch = batches
        .get(batch_id)
        .ok_or_else(|| StorageError::InvariantViolation {
            entity: Some("catalog_sync_batch"),
            detail: format!("catalog change references missing batch {batch_id}"),
        })?;
    if batch.status != CatalogSyncStatus::Committed {
        return Err(StorageError::InvariantViolation {
            entity: Some("catalog_sync_batch"),
            detail: format!("catalog change references non-committed batch {batch_id}"),
        });
    }
    batch
        .committed_at
        .ok_or_else(|| StorageError::InvariantViolation {
            entity: Some("catalog_sync_batch"),
            detail: format!("committed catalog batch {batch_id} has no committed_at"),
        })
}

async fn acquire_catalog_writer_lock(txn: &DatabaseTransaction) -> Result<(), StorageError> {
    primitives::advisory_xact_lock(txn, CATALOG_WRITER_LOCK_ID).await
}

async fn recover_commit_outcome(
    db: &DatabaseConnection,
    batch_id: &CatalogSyncBatchId,
    commit_error: DbErr,
) -> Result<CatalogSyncBatchInfo, StorageError> {
    match CatalogSyncBatchEntity::find_by_id(batch_id.clone())
        .one(db)
        .await
    {
        Ok(Some(batch)) if batch.status == CatalogSyncStatus::Committed => {
            tracing::warn!(
                %batch_id,
                %commit_error,
                "catalog commit acknowledgement was uncertain; committed batch recovered by id"
            );
            Ok(batch.into())
        }
        Ok(Some(batch)) => Err(StorageError::state_conflict(
            "catalog_sync_batch",
            Some(batch_id),
            format!(
                "commit acknowledgement failed ({commit_error}) and batch resolved to {}",
                batch.status
            ),
        )),
        Ok(None) => Err(StorageError::from(commit_error)),
        Err(recovery_error) => Err(StorageError::Transaction(format!(
            "catalog commit acknowledgement failed ({commit_error}); outcome recovery failed ({recovery_error})"
        ))),
    }
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
