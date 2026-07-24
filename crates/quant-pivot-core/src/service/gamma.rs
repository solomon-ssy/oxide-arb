//! Gamma market catalog sync — full and incremental refresh into registry, cache, and DB.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    sync::Arc,
    time::Instant,
};

use chrono::{DateTime, Duration, Utc};
use num_traits::ToPrimitive;
use quant_pivot_api::gamma::{
    CatalogMarketReject, CatalogSourceTimestamps, FilteredPrelistingMarket, GammaCatalogBatch,
    GammaClient, RejectedMarket,
};
use quant_pivot_error::{QuantError, api::ApiError, market::MarketError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionClock,
        market::{
            CATALOG_OBJECT_SCHEMA_VERSION, CatalogBatchCommit, CatalogBatchFailure,
            CatalogEventCandidate, CatalogMarketCandidate, EventRegistryInfo, EventTags,
            MarketRegistryInfo, NewCatalogEventChange, NewCatalogEventObject,
            NewCatalogMarketObject, NewCatalogSyncBatch, NewCatalogSyncRejection, UpsertEvent,
            UpsertMarket,
        },
        ports::CatalogStatusPort,
        runtime::{CoreEvent, CoreEventPublisher},
    },
    enums::{
        catalog::{
            CatalogChangeType, CatalogEntityKind, CatalogRejectionReason, CatalogSyncFailureStage,
            CatalogSyncKind, CatalogTimestampQuality,
        },
        market::MarketStatus,
    },
    hashing::CanonicalDigest,
    types::{
        CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId, CatalogMarketIds,
        CatalogMarketObjectId, CatalogSyncBatchId, CatalogSyncRejectionId, ContentHash, EventId,
        MarketId,
    },
};
use quant_pivot_repository::traits::{CatalogLedgerRepository, MarketRepository};
use quant_pivot_storage::cache::{CacheKey, CacheManager};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    execution::settlement_discovery_wake::SettlementDiscoveryWake,
    governance::LinkageResolverService,
    ingest::{
        market_cache::MarketCache, market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
    service::{
        catalog_lifecycle::apply_catalog_deadline, catalog_readiness::CatalogReadiness,
        system_status_nudge::SystemStatusNudge, ws_subscription::WsSubscriptionCoordinator,
    },
};

/// Only publish `market.resolved` for settlements whose `resolved_at` falls
/// within this window — filters historical reconciliation during full sync.
const LIVE_RESOLUTION_WINDOW: Duration = Duration::hours(48);

/// Dependencies injected into [`GammaService`].
pub struct GammaServiceDeps {
    pub gamma_client: Arc<GammaClient>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub market_filter: Arc<MarketFilter>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub catalog_ledger_repo: Arc<dyn CatalogLedgerRepository>,
    pub cache: Arc<CacheManager>,
    pub metrics: Arc<MetricsHub>,
    pub catalog: Arc<CatalogReadiness>,
    pub ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    /// Runtime event bus — `market.resolved` WS frames are published from the
    /// authoritative persistence point (settled-status transitions).
    pub events: CoreEventPublisher,
    /// Wake the system-status broadcaster after catalog warmup completes.
    pub status_nudge: SystemStatusNudge,
    /// Best-effort settlement discovery wake. `PostgreSQL` remains the work source of truth.
    pub settlement_discovery_wake: SettlementDiscoveryWake,
    /// WS subscription look-ahead window (hours) from deploy config.
    pub subscription_window_hours: u64,
    /// Offline linkage resolver — runs after successful sync.
    pub linkage_resolver: Option<Arc<LinkageResolverService>>,
}

pub struct GammaService {
    gamma_client: Arc<GammaClient>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    market_filter: Arc<MarketFilter>,
    market_repo: Arc<dyn MarketRepository>,
    catalog_ledger_repo: Arc<dyn CatalogLedgerRepository>,
    cache: Arc<CacheManager>,
    metrics: Arc<MetricsHub>,
    catalog: Arc<CatalogReadiness>,
    ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    events: CoreEventPublisher,
    status_nudge: SystemStatusNudge,
    settlement_discovery_wake: SettlementDiscoveryWake,
    subscription_window_hours: u64,
    linkage_resolver: Option<Arc<LinkageResolverService>>,
    linkage_wake: Arc<Notify>,
}

impl GammaService {
    pub fn new(deps: GammaServiceDeps) -> Self {
        Self {
            gamma_client: deps.gamma_client,
            market_registry: deps.market_registry,
            market_cache: deps.market_cache,
            market_filter: deps.market_filter,
            market_repo: deps.market_repo,
            catalog_ledger_repo: deps.catalog_ledger_repo,
            cache: deps.cache,
            metrics: deps.metrics,
            catalog: deps.catalog,
            ws_subscription: deps.ws_subscription,
            events: deps.events,
            status_nudge: deps.status_nudge,
            settlement_discovery_wake: deps.settlement_discovery_wake,
            subscription_window_hours: deps.subscription_window_hours,
            linkage_resolver: deps.linkage_resolver,
            linkage_wake: Arc::new(Notify::new()),
        }
    }

    pub async fn sync(&self) -> Result<(), QuantError> {
        let timer = Instant::now();
        let result = self.sync_inner().await;
        let elapsed_ms = ToPrimitive::to_i64(&timer.elapsed().as_millis()).unwrap_or(i64::MAX);

        self.metrics.gamma_sync_duration_ms.set(elapsed_ms);
        self.metrics
            .gamma_last_sync_success
            .set(i64::from(result.is_ok()));

        if result.is_ok() {
            self.publish_catalog_ready();
            self.trigger_linkage_resolution();
        }

        result
    }

    fn trigger_linkage_resolution(&self) {
        if self.linkage_resolver.is_some() {
            self.linkage_wake.notify_one();
        }
    }

    pub async fn run_linkage_resolver(&self, shutdown: CancellationToken) {
        let Some(resolver) = self.linkage_resolver.as_ref() else {
            return;
        };
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                () = self.linkage_wake.notified() => {}
            }
            match resolver.resolve_changed_markets(&[]).await {
                Ok(summary) => {
                    tracing::info!(
                        examined = summary.examined,
                        appended = summary.appended,
                        unchanged = summary.unchanged,
                        resolved = summary.resolved,
                        unresolved = summary.unresolved,
                        "linkage resolver pass complete after gamma sync"
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, "linkage resolver pass failed after gamma sync");
                }
            }
        }
    }

    fn publish_catalog_ready(&self) {
        let synced_at = Utc::now();
        let markets =
            u64::try_from(self.market_registry.active_markets().len()).unwrap_or(u64::MAX);
        self.catalog.mark_ready(markets, synced_at);
        self.metrics.catalog_ready.set(1);
        self.status_nudge.nudge();
    }

    async fn sync_inner(&self) -> Result<(), QuantError> {
        let sync_kind = if self.catalog_ledger_repo.coverage_start().await?.is_some() {
            CatalogSyncKind::Reconcile
        } else {
            CatalogSyncKind::Baseline
        };
        let attempt = CatalogSyncAttempt {
            batch_id: CatalogSyncBatchId::from_v7(),
            sync_kind,
            started_at: Utc::now(),
        };
        let result = self.full_sync(&attempt).await;
        if let Err(failure) = result {
            let stage = catalog_failure_stage(&failure.error);
            if let Err(audit_error) = self
                .catalog_ledger_repo
                .record_failure(CatalogBatchFailure {
                    catalog_sync_batch_id: attempt.batch_id,
                    sync_kind: attempt.sync_kind,
                    started_at: attempt.started_at,
                    fetched_at: failure.fetched_at,
                    failure_stage: stage,
                    failure_detail: failure.error.to_string(),
                    rejections: failure.rejections,
                })
                .await
            {
                tracing::error!(
                    sync_error = %failure.error,
                    audit_error = %audit_error,
                    "gamma sync failed and its catalog attempt audit could not be persisted"
                );
                return Err(audit_error.into());
            }
            return Err(failure.error);
        }

        Ok(())
    }

    async fn full_sync(&self, attempt: &CatalogSyncAttempt) -> Result<(), CatalogSyncFailure> {
        let mut batch = self
            .gamma_client
            .full_sync_with_fees()
            .await
            .map_err(CatalogSyncFailure::from)?;
        let fetched_at = Utc::now();

        let filtered_count = batch.filtered.len();
        self.record_prelisting_filters(&batch.filtered);
        let rejected_count = batch.rejected.len();
        if rejected_count != 0 {
            self.record_rejections(&batch.rejected);
            return Err(CatalogSyncFailure {
                error: MarketError::CatalogInputRejected {
                    count: rejected_count,
                }
                .into(),
                fetched_at: Some(fetched_at),
                rejections: build_catalog_rejections(&attempt.batch_id, &batch.rejected),
            });
        }

        if batch.registry_markets.is_empty() {
            return Err(CatalogSyncFailure::at(
                MarketError::EmptyCatalog.into(),
                fetched_at,
            ));
        }

        reject_duplicate_catalog_ids(
            "event",
            sorted_ids(
                batch
                    .registry_events
                    .iter()
                    .map(|event| event.event_id.as_str()),
            ),
        )?;
        reject_duplicate_catalog_ids(
            "market",
            sorted_ids(
                batch
                    .registry_markets
                    .iter()
                    .map(|market| market.market_id.as_str()),
            ),
        )?;

        let seen_ids: HashSet<MarketId> = batch
            .registry_markets
            .iter()
            .map(|m| m.market_id.clone())
            .collect();

        let past_deadline_paused = apply_catalog_deadline(&mut batch.registry_markets, fetched_at);
        let mut event_mutations = HashMap::new();
        let mut market_mutations = HashMap::new();
        let tombstoned_count = if attempt.sync_kind == CatalogSyncKind::Reconcile {
            self.reconcile_missing_markets(
                &mut batch,
                &seen_ids,
                fetched_at,
                &mut event_mutations,
                &mut market_mutations,
            )
            .await?
        } else {
            0
        };
        coalesce_catalog_batch(&mut batch);
        self.preserve_manual_blocks(&mut batch.registry_markets)
            .await?;
        self.append_missing_registry_events(&mut batch.registry_events, &batch.registry_markets);

        let newly_settled = self
            .detect_settlement_transitions(&batch.registry_markets)
            .await?;
        let commit = build_catalog_commit(&CatalogCommitInput {
            batch_id: attempt.batch_id,
            sync_kind: attempt.sync_kind,
            started_at: attempt.started_at,
            fetched_at,
            registry_events: &batch.registry_events,
            event_source_timestamps: &batch.event_source_timestamps,
            registry_markets: &batch.registry_markets,
            market_source_timestamps: &batch.market_source_timestamps,
            event_mutations: &event_mutations,
            market_mutations: &market_mutations,
        })?;
        self.catalog_ledger_repo.commit(commit).await?;
        self.invalidate_projection_cache(&batch.registry_events, &batch.registry_markets)
            .await;

        self.market_registry
            .register_events(batch.registry_events.clone());
        self.market_registry
            .register_markets(batch.registry_markets.clone());
        self.publish_market_resolutions(newly_settled);
        self.market_cache.rebuild();
        self.sync_ws_subscriptions();

        if past_deadline_paused > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["past_deadline"])
                .inc_by(past_deadline_paused);
        }
        if tombstoned_count > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["gamma_tombstone"])
                .inc_by(u64::try_from(tombstoned_count).unwrap_or(u64::MAX));
        }

        let event_count = batch.registry_events.len();
        let market_count = batch.registry_markets.len();
        self.metrics
            .gamma_markets_total
            .set(ToPrimitive::to_i64(&market_count).unwrap_or(i64::MAX));

        tracing::info!(
            events = event_count,
            registered = market_count,
            filtered = filtered_count,
            rejected = rejected_count,
            tombstoned = tombstoned_count,
            sync_kind = %attempt.sync_kind,
            "gamma catalog reconciliation complete"
        );

        Ok(())
    }

    /// Count normalization rejects by reason (logging is per-row `debug!` at
    /// the catalog layer; the counter + sync summary are the operator surface).
    fn record_rejections(&self, rejected: &[RejectedMarket]) {
        for rejection in rejected {
            self.metrics
                .gamma_markets_rejected
                .with_label_values(&[rejection.reject.reason_label()])
                .inc();
        }
    }

    fn record_prelisting_filters(&self, filtered: &[FilteredPrelistingMarket]) {
        for market in filtered {
            self.metrics
                .gamma_markets_filtered
                .with_label_values(&[market.reason.as_str()])
                .inc();
        }
    }

    fn sync_ws_subscriptions(&self) {
        let Some(coordinator) = &self.ws_subscription else {
            return;
        };
        let stats = coordinator.sync_subscription(
            &self.market_registry,
            &self.market_filter,
            self.subscription_window_hours,
            &self.metrics,
        );
        tracing::info!(
            selected_tokens = stats.selected_tokens,
            tier1_markets = stats.tier1_markets(),
            tier1_tokens = stats.tier1_tokens(),
            tier2_markets = stats.tier2_markets(),
            tier2_tokens = stats.tier2_tokens(),
            candidates_future_detect = stats.candidates_future_detect,
            candidates_future_prewarm = stats.candidates_future_prewarm,
            candidates_excluded_past = stats.candidates_past_deadline,
            detection_window_coverage = stats.detection_window_coverage_ratio(),
            subscribed = coordinator.subscribed_count(),
            "CLOB websocket subscription ingest synced after Gamma catalog update"
        );
    }

    /// Load prior persisted statuses for the settled markets in this batch and
    /// resolve which of them are settling for the first time.
    async fn detect_settlement_transitions(
        &self,
        markets: &[MarketRegistryInfo],
    ) -> Result<Vec<(MarketId, bool)>, QuantError> {
        let ids: Vec<MarketId> = markets
            .iter()
            .filter(|market| market.status == MarketStatus::Settled)
            .map(|market| market.market_id.clone())
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let prior: HashMap<MarketId, MarketStatus> = self
            .market_repo
            .find_by_ids(&ids)
            .await?
            .into_iter()
            .map(|row| (row.market_id.clone(), row.status))
            .collect();
        let scan = settlement_transitions(markets, &prior, Utc::now());
        for market_id in &scan.ambiguous {
            tracing::warn!(
                %market_id,
                "suppressing market.resolved because the winning token is unavailable or ambiguous"
            );
            self.metrics
                .gamma_markets_rejected
                .with_label_values(&["ambiguous_settlement"])
                .inc();
        }
        Ok(scan.publishable)
    }

    fn publish_market_resolutions(&self, resolved: Vec<(MarketId, bool)>) {
        if !self.catalog.is_ready() {
            if !resolved.is_empty() {
                tracing::debug!(
                    count = resolved.len(),
                    "suppressing market.resolved during catalog warmup"
                );
            }
            return;
        }
        for (market_id, outcome) in resolved {
            tracing::info!(%market_id, outcome, "market settled; publishing market.resolved");
            self.events
                .publish(CoreEvent::MarketResolved { market_id, outcome });
        }
        self.settlement_discovery_wake.wake();
    }

    async fn preserve_manual_blocks(
        &self,
        registry_markets: &mut [MarketRegistryInfo],
    ) -> Result<(), QuantError> {
        let ids = registry_markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(());
        }
        let blocked = self
            .market_repo
            .find_by_ids(&ids)
            .await?
            .into_iter()
            .filter(|market| market.status == MarketStatus::ManuallyBlocked)
            .map(|market| market.market_id.to_string())
            .collect::<HashSet<_>>();
        if blocked.is_empty() {
            return Ok(());
        }
        for market in registry_markets {
            if blocked.contains(market.market_id.as_str()) {
                market.status = MarketStatus::ManuallyBlocked;
            }
        }
        Ok(())
    }

    async fn reconcile_missing_markets(
        &self,
        batch: &mut GammaCatalogBatch,
        seen_ids: &HashSet<MarketId>,
        fetched_at: DateTime<Utc>,
        event_mutations: &mut HashMap<EventId, CatalogMutation>,
        market_mutations: &mut HashMap<MarketId, CatalogMutation>,
    ) -> Result<usize, CatalogSyncFailure> {
        let missing_ids = self
            .market_repo
            .find_active()
            .await?
            .iter()
            .map(|market| market.market_id.clone())
            .filter(|market_id| !seen_ids.contains(market_id))
            .collect::<Vec<_>>();
        let results = self
            .gamma_client
            .fetch_markets_bounded(&missing_ids, 8)
            .await;
        let mut tombstoned = 0_usize;
        let boundary = DecisionClock::new(0).boundary(fetched_at)?;
        for (market_id, result) in results {
            match result {
                Ok(mut fetched) => {
                    if fetched.event.market_ids.is_empty()
                        && let Some(previous_event) =
                            self.market_registry.get_event(&fetched.event.event_id)
                    {
                        fetched.event.market_ids = previous_event.market_ids;
                    }
                    batch.registry_events.push(fetched.event.clone());
                    batch.registry_markets.push(fetched.registry.clone());
                    batch.event_source_timestamps.insert(
                        fetched.event.event_id.clone(),
                        fetched.event_source_timestamps,
                    );
                    batch.market_source_timestamps.insert(
                        fetched.registry.market_id.clone(),
                        fetched.source_timestamps,
                    );
                    event_mutations.insert(fetched.event.event_id, CatalogMutation::recheck());
                    market_mutations.insert(market_id, CatalogMutation::recheck());
                }
                Err(error) if is_not_found(&error) => {
                    let prior = self
                        .catalog_ledger_repo
                        .market_at(&market_id, &boundary)
                        .await?
                        .ok_or_else(|| MarketError::CatalogSerialization {
                            entity: "market",
                            id: market_id.to_string(),
                            reason: "active projection has no immutable catalog object".to_owned(),
                        })?;
                    let mut tombstone: MarketRegistryInfo = decode_catalog_payload(
                        "market",
                        market_id.as_str(),
                        prior.payload.into_inner(),
                    )?;
                    tombstone.status = MarketStatus::Delisted;
                    let event_id = tombstone.event_id.clone();
                    batch.registry_markets.push(tombstone);
                    let prior_event = self
                        .catalog_ledger_repo
                        .event_at(&event_id, &boundary)
                        .await?
                        .ok_or_else(|| MarketError::MissingEventVersion {
                            market_id: market_id.to_string(),
                            event_id: event_id.to_string(),
                        })?;
                    batch.registry_events.push(decode_catalog_payload(
                        "event",
                        event_id.as_str(),
                        prior_event.payload.into_inner(),
                    )?);
                    market_mutations.insert(market_id, CatalogMutation::tombstone());
                    tombstoned += 1;
                }
                Err(error) if error.is_retryable() => {
                    return Err(CatalogSyncFailure::at(error.into(), fetched_at));
                }
                Err(error) => {
                    return Err(CatalogSyncFailure::at(error.into(), fetched_at));
                }
            }
        }
        Ok(tombstoned)
    }

    /// Stale markets can belong to events absent from the new full response.
    /// Freeze the registry's last observed event state in the same batch so
    /// every market version has an immutable membership parent.
    fn append_missing_registry_events(
        &self,
        events: &mut Vec<EventRegistryInfo>,
        markets: &[MarketRegistryInfo],
    ) {
        let mut event_ids = events
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<HashSet<_>>();
        for event_id in markets.iter().map(|market| &market.event_id) {
            if event_ids.contains(event_id) {
                continue;
            }
            if let Some(event) = self.market_registry.get_event(event_id) {
                event_ids.insert(event_id.clone());
                events.push(event);
            }
        }
    }

    async fn invalidate_projection_cache(
        &self,
        events: &[EventRegistryInfo],
        markets: &[MarketRegistryInfo],
    ) {
        for event in events {
            self.cache
                .invalidate(&CacheKey::EventInfo {
                    event_id: event.event_id.clone(),
                })
                .await;
        }
        for market in markets {
            self.cache
                .invalidate(&CacheKey::MarketInfo {
                    market_id: market.market_id.clone(),
                })
                .await;
            self.cache
                .invalidate(&CacheKey::MarketMetadata {
                    market_id: market.market_id.clone(),
                })
                .await;
        }
    }
}

fn decode_catalog_payload<T: DeserializeOwned>(
    entity: &'static str,
    id: &str,
    payload: Value,
) -> Result<T, MarketError> {
    serde_json::from_value(payload).map_err(|error| MarketError::CatalogSerialization {
        entity,
        id: id.to_owned(),
        reason: error.to_string(),
    })
}

#[derive(Debug, Clone)]
struct CatalogSyncAttempt {
    batch_id: CatalogSyncBatchId,
    sync_kind: CatalogSyncKind,
    started_at: DateTime<Utc>,
}

struct CatalogSyncFailure {
    error: QuantError,
    fetched_at: Option<DateTime<Utc>>,
    rejections: Vec<NewCatalogSyncRejection>,
}

impl CatalogSyncFailure {
    const fn at(error: QuantError, fetched_at: DateTime<Utc>) -> Self {
        Self {
            error,
            fetched_at: Some(fetched_at),
            rejections: Vec::new(),
        }
    }
}

impl From<QuantError> for CatalogSyncFailure {
    fn from(error: QuantError) -> Self {
        Self {
            error,
            fetched_at: None,
            rejections: Vec::new(),
        }
    }
}

impl From<ApiError> for CatalogSyncFailure {
    fn from(error: ApiError) -> Self {
        Self::from(QuantError::from(error))
    }
}

impl From<MarketError> for CatalogSyncFailure {
    fn from(error: MarketError) -> Self {
        Self::from(QuantError::from(error))
    }
}

impl From<StorageError> for CatalogSyncFailure {
    fn from(error: StorageError) -> Self {
        Self::from(QuantError::from(error))
    }
}

#[derive(Debug, Clone, Copy)]
struct CatalogMutation {
    change_type: CatalogChangeType,
}

impl CatalogMutation {
    const fn sync() -> Self {
        Self {
            change_type: CatalogChangeType::GammaScanUpsert,
        }
    }

    const fn recheck() -> Self {
        Self {
            change_type: CatalogChangeType::GammaIdRecheckUpsert,
        }
    }

    const fn tombstone() -> Self {
        Self {
            change_type: CatalogChangeType::GammaConfirmedTombstone,
        }
    }
}

const fn catalog_failure_stage(error: &QuantError) -> CatalogSyncFailureStage {
    match error {
        QuantError::Api(_) => CatalogSyncFailureStage::Fetch,
        QuantError::Storage(_) => CatalogSyncFailureStage::Persist,
        _ => CatalogSyncFailureStage::Normalize,
    }
}

struct CatalogCommitInput<'a> {
    batch_id: CatalogSyncBatchId,
    sync_kind: CatalogSyncKind,
    started_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    registry_events: &'a [EventRegistryInfo],
    event_source_timestamps: &'a HashMap<EventId, CatalogSourceTimestamps>,
    registry_markets: &'a [MarketRegistryInfo],
    market_source_timestamps: &'a HashMap<MarketId, CatalogSourceTimestamps>,
    event_mutations: &'a HashMap<EventId, CatalogMutation>,
    market_mutations: &'a HashMap<MarketId, CatalogMutation>,
}

#[derive(Serialize)]
struct CatalogBatchDigest {
    events: Vec<(EventId, CatalogChangeType, ContentHash)>,
    markets: Vec<(MarketId, CatalogChangeType, ContentHash)>,
}

fn build_catalog_commit(input: &CatalogCommitInput<'_>) -> Result<CatalogBatchCommit, QuantError> {
    let sync_kind = input.sync_kind;
    let catalog_sync_batch_id = input.batch_id;
    let started_at = input.started_at;
    let fetched_at = input.fetched_at;
    let registry_events = input.registry_events;
    let event_source_timestamps = input.event_source_timestamps;
    let registry_markets = input.registry_markets;
    let market_source_timestamps = input.market_source_timestamps;
    let event_mutations = input.event_mutations;
    let market_mutations = input.market_mutations;
    let event_build = build_event_candidates(
        &catalog_sync_batch_id,
        registry_events,
        event_source_timestamps,
        event_mutations,
        fetched_at,
    )?;
    let market_build = build_market_candidates(
        &catalog_sync_batch_id,
        registry_markets,
        market_source_timestamps,
        market_mutations,
        &event_build.object_ids,
        fetched_at,
    )?;

    let batch_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/catalog-manifest",
        1,
        &CatalogBatchDigest {
            events: event_build.digests,
            markets: market_build.digests,
        },
    )?;
    let event_count = catalog_count("event", event_build.candidates.len())?;
    let market_count = catalog_count("market", market_build.candidates.len())?;

    Ok(CatalogBatchCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id,
            sync_kind,
            started_at,
            fetched_at,
            event_count,
            market_count,
            rejected_count: 0,
            batch_hash,
        },
        events: event_build.candidates,
        markets: market_build.candidates,
    })
}

struct EventCandidateBuild {
    object_ids: BTreeMap<EventId, CatalogEventObjectId>,
    candidates: Vec<CatalogEventCandidate>,
    digests: Vec<(EventId, CatalogChangeType, ContentHash)>,
}

fn build_event_candidates(
    batch_id: &CatalogSyncBatchId,
    events: &[EventRegistryInfo],
    source_timestamps: &HashMap<EventId, CatalogSourceTimestamps>,
    mutations: &HashMap<EventId, CatalogMutation>,
    available_at: DateTime<Utc>,
) -> Result<EventCandidateBuild, QuantError> {
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.event_id.cmp(&right.event_id));
    reject_duplicate_catalog_ids("event", ordered.iter().map(|event| event.event_id.as_str()))?;
    let mut object_ids = BTreeMap::new();
    let mut candidates = Vec::with_capacity(ordered.len());
    let mut digests = Vec::with_capacity(ordered.len());
    for event in ordered {
        let timestamps = resolve_catalog_timestamps(
            "event",
            event.event_id.as_str(),
            source_timestamps.get(&event.event_id).copied(),
            available_at,
        )?;
        let payload =
            serde_json::to_value(event).map_err(|error| MarketError::CatalogSerialization {
                entity: "event",
                id: event.event_id.to_string(),
                reason: error.to_string(),
            })?;
        let content_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-event-object", 1, event)?;
        let event_object_id = CatalogEventObjectId::from_content_hash(&content_hash);
        let mutation = mutations
            .get(&event.event_id)
            .copied()
            .unwrap_or_else(CatalogMutation::sync);
        object_ids.insert(event.event_id.clone(), event_object_id);
        digests.push((event.event_id.clone(), mutation.change_type, content_hash));
        candidates.push(CatalogEventCandidate {
            projection: UpsertEvent {
                event_id: event.event_id.clone(),
                title: event.title.clone(),
                slug: event.slug.clone(),
                series_slug: event.series_slug.clone(),
                status: event.status,
                tags: EventTags(event.tags.clone()),
                neg_risk: event.neg_risk,
                catalog_market_ids: CatalogMarketIds(event.market_ids.clone()),
                end_date: event.end_date,
                content_hash,
            },
            object: NewCatalogEventObject {
                event_object_id,
                content_hash,
                schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                payload: payload.into(),
            },
            change: NewCatalogEventChange {
                event_change_id: CatalogEventChangeId::from_v7(),
                catalog_sync_batch_id: *batch_id,
                event_object_id,
                event_id: event.event_id.clone(),
                source_effective_at: timestamps.source_effective_at,
                source_timestamp_quality: timestamps.quality,
                change_type: mutation.change_type,
            },
        });
    }
    Ok(EventCandidateBuild {
        object_ids,
        candidates,
        digests,
    })
}

struct MarketCandidateBuild {
    candidates: Vec<CatalogMarketCandidate>,
    digests: Vec<(MarketId, CatalogChangeType, ContentHash)>,
}

fn build_market_candidates(
    batch_id: &CatalogSyncBatchId,
    markets: &[MarketRegistryInfo],
    source_timestamps: &HashMap<MarketId, CatalogSourceTimestamps>,
    mutations: &HashMap<MarketId, CatalogMutation>,
    event_object_ids: &BTreeMap<EventId, CatalogEventObjectId>,
    available_at: DateTime<Utc>,
) -> Result<MarketCandidateBuild, QuantError> {
    let mut ordered = markets.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.market_id.cmp(&right.market_id));
    reject_duplicate_catalog_ids(
        "market",
        ordered.iter().map(|market| market.market_id.as_str()),
    )?;
    let mut candidates = Vec::with_capacity(ordered.len());
    let mut digests = Vec::with_capacity(ordered.len());
    for market in ordered {
        let event_object_id = event_object_ids
            .get(&market.event_id)
            .copied()
            .ok_or_else(|| MarketError::MissingEventVersion {
                market_id: market.market_id.to_string(),
                event_id: market.event_id.to_string(),
            })?;
        let timestamps = resolve_catalog_timestamps(
            "market",
            market.market_id.as_str(),
            source_timestamps.get(&market.market_id).copied(),
            available_at,
        )?;
        let payload =
            serde_json::to_value(market).map_err(|error| MarketError::CatalogSerialization {
                entity: "market",
                id: market.market_id.to_string(),
                reason: error.to_string(),
            })?;
        let content_hash =
            CanonicalDigest::content_hash_typed("quant-pivot/catalog-market-object", 1, market)?;
        let market_object_id = CatalogMarketObjectId::from_content_hash(&content_hash);
        let mutation = mutations
            .get(&market.market_id)
            .copied()
            .unwrap_or_else(CatalogMutation::sync);
        digests.push((market.market_id.clone(), mutation.change_type, content_hash));
        candidates.push(CatalogMarketCandidate {
            projection: UpsertMarket::from_registry(market)?,
            object: NewCatalogMarketObject {
                market_object_id,
                content_hash,
                schema_version: CATALOG_OBJECT_SCHEMA_VERSION,
                payload: payload.into(),
            },
            market_change_id: CatalogMarketChangeId::from_v7(),
            catalog_sync_batch_id: *batch_id,
            event_object_id,
            source_effective_at: timestamps.source_effective_at,
            source_timestamp_quality: timestamps.quality,
            source_created_at: timestamps.source_created_at,
            change_type: mutation.change_type,
        });
    }
    Ok(MarketCandidateBuild {
        candidates,
        digests,
    })
}

struct ResolvedCatalogTimestamps {
    source_effective_at: DateTime<Utc>,
    quality: CatalogTimestampQuality,
    source_created_at: Option<DateTime<Utc>>,
}

fn resolve_catalog_timestamps(
    entity: &'static str,
    id: &str,
    source: Option<CatalogSourceTimestamps>,
    available_at: DateTime<Utc>,
) -> Result<ResolvedCatalogTimestamps, MarketError> {
    let source_created_at = source.and_then(|timestamps| timestamps.created_at);
    let source_updated_at = source.and_then(|timestamps| timestamps.updated_at);
    for (field, timestamp) in [
        ("created_at", source_created_at),
        ("updated_at", source_updated_at),
    ] {
        if timestamp.is_some_and(|timestamp| timestamp > available_at) {
            return Err(MarketError::CatalogTimestampInFuture {
                entity,
                id: id.to_owned(),
                field,
                timestamp: timestamp.map_or_else(String::new, |value| value.to_rfc3339()),
                available_at: available_at.to_rfc3339(),
            });
        }
    }

    Ok(source_updated_at.map_or(
        ResolvedCatalogTimestamps {
            source_effective_at: available_at,
            quality: CatalogTimestampQuality::CommitTimeFallback,
            source_created_at,
        },
        |source_effective_at| ResolvedCatalogTimestamps {
            source_effective_at,
            quality: CatalogTimestampQuality::Source,
            source_created_at,
        },
    ))
}

fn reject_duplicate_catalog_ids<'a>(
    entity: &'static str,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), MarketError> {
    let mut previous: Option<&str> = None;
    for id in ids {
        if previous == Some(id) {
            return Err(MarketError::DuplicateCatalogEntity {
                entity,
                id: id.to_owned(),
            });
        }
        previous = Some(id);
    }
    Ok(())
}

fn catalog_count(entity: &'static str, count: usize) -> Result<i64, MarketError> {
    i64::try_from(count).map_err(|_| MarketError::CatalogCountOverflow { entity })
}

fn build_catalog_rejections(
    batch_id: &CatalogSyncBatchId,
    rejected: &[RejectedMarket],
) -> Vec<NewCatalogSyncRejection> {
    rejected
        .iter()
        .map(|rejection| NewCatalogSyncRejection {
            catalog_sync_rejection_id: CatalogSyncRejectionId::from_v7(),
            catalog_sync_batch_id: *batch_id,
            entity_kind: CatalogEntityKind::Market,
            source_id: (!rejection.condition_id.is_empty()).then(|| rejection.condition_id.clone()),
            reason_code: rejection_reason(&rejection.reject),
            detail: rejection.reject.to_string(),
            raw_payload: rejection.raw_payload.clone().map(Into::into),
        })
        .collect()
}

const fn rejection_reason(rejection: &CatalogMarketReject) -> CatalogRejectionReason {
    match rejection {
        CatalogMarketReject::EmptyConditionId => CatalogRejectionReason::EmptyConditionId,
        CatalogMarketReject::MissingClobTokenIds => CatalogRejectionReason::MissingClobTokenIds,
        CatalogMarketReject::NotBinary { .. } => CatalogRejectionReason::NotBinary,
        CatalogMarketReject::InvalidTokenPair { .. } => CatalogRejectionReason::InvalidTokenPair,
        CatalogMarketReject::UnsupportedTickSize { .. } => {
            CatalogRejectionReason::UnsupportedTickSize
        }
    }
}

const fn is_not_found(error: &ApiError) -> bool {
    matches!(error, ApiError::Gamma { status: 404, .. })
}

fn sorted_ids<'a>(ids: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn coalesce_catalog_batch(batch: &mut GammaCatalogBatch) {
    batch.registry_events = mem::take(&mut batch.registry_events)
        .into_iter()
        .map(|event| (event.event_id.clone(), event))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    batch.registry_markets = mem::take(&mut batch.registry_markets)
        .into_iter()
        .map(|market| (market.market_id.clone(), market))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
}

/// Result of scanning one committed catalog projection for live settlements.
#[derive(Debug, Default, PartialEq, Eq)]
struct SettlementTransitionScan {
    /// Unambiguous transitions, paired with whether the canonical primary
    /// (`token_yes`) leg won.
    publishable: Vec<(MarketId, bool)>,
    /// Recent transitions whose winning token cannot be proven. They remain
    /// settled in the catalog but must never emit a fabricated `false` event.
    ambiguous: Vec<MarketId>,
}

/// Markets in `batch` transitioning to [`MarketStatus::Settled`] from a
/// previously persisted non-settled row.
///
/// Requiring a prior row keeps `market.resolved` a true *transition* signal: a
/// fresh bootstrap ingesting thousands of historically settled markets
/// publishes nothing (no operator was watching them), while a watched market
/// settling live publishes exactly once — the upsert makes the row `Settled`,
/// so subsequent syncs skip it. Only settlements with a recent `resolved_at`
/// (within [`LIVE_RESOLUTION_WINDOW`]) pass — stale DB reconciliation during
/// full sync stays silent.
fn settlement_transitions(
    batch: &[MarketRegistryInfo],
    prior: &HashMap<MarketId, MarketStatus>,
    now: DateTime<Utc>,
) -> SettlementTransitionScan {
    let mut scan = SettlementTransitionScan::default();
    for market in batch.iter().filter(|market| {
        market.status == MarketStatus::Settled
            && prior
                .get(&market.market_id)
                .is_some_and(|status| *status != MarketStatus::Settled)
            && is_recently_resolved(market, now)
    }) {
        match primary_outcome_won(market) {
            Some(primary_won) => scan
                .publishable
                .push((market.market_id.clone(), primary_won)),
            None => scan.ambiguous.push(market.market_id.clone()),
        }
    }
    scan
}

/// Whether the settlement is recent enough to notify operators (live transition).
fn is_recently_resolved(market: &MarketRegistryInfo, now: DateTime<Utc>) -> bool {
    market.resolved_at.is_some_and(|resolved_at| {
        resolved_at <= now && now - resolved_at <= LIVE_RESOLUTION_WINDOW
    })
}

/// Resolve Gamma's winning outcome label back to one exact token, then compare
/// token identity. This remains correct for custom binary labels such as
/// `Over`/`Under`; a missing, duplicate, or unknown label is not `false`.
fn primary_outcome_won(market: &MarketRegistryInfo) -> Option<bool> {
    let winning_outcome = market.outcome.as_deref()?.trim();
    let mut matching_tokens = market
        .tokens
        .iter()
        .filter(|token| token.outcome.trim().eq_ignore_ascii_case(winning_outcome));
    let winner = matching_tokens.next()?;
    if matching_tokens.next().is_some() {
        return None;
    }
    if winner.token_id == market.token_yes {
        Some(true)
    } else if winner.token_id == market.token_no {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use quant_pivot_models::{
        domain::market::TokenInfo,
        enums::{
            catalog::CatalogFilterReasonSet,
            common::{CategorySet, TickSize},
        },
        types::{EventId, TokenId},
    };
    use rust_decimal::Decimal;

    use super::*;

    fn registry_market(
        id: &str,
        status: MarketStatus,
        outcome: Option<&str>,
        resolved_at: Option<DateTime<Utc>>,
    ) -> MarketRegistryInfo {
        registry_market_with_labels(id, status, outcome, resolved_at, ["Yes", "No"])
    }

    fn registry_market_with_labels(
        id: &str,
        status: MarketStatus,
        outcome: Option<&str>,
        resolved_at: Option<DateTime<Utc>>,
        labels: [&str; 2],
    ) -> MarketRegistryInfo {
        let token_yes = TokenId::new(format!("{id}-yes"));
        let token_no = TokenId::new(format!("{id}-no"));
        MarketRegistryInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            token_yes: token_yes.clone(),
            token_no: token_no.clone(),
            question: "Test?".into(),
            slug: "test".into(),
            description: None,
            categories: CategorySet::EMPTY,
            status,
            filter_reasons: CatalogFilterReasonSet::default(),
            outcome: outcome.map(str::to_owned),
            neg_risk: false,
            tick_size: TickSize::Hundredth,
            tokens: vec![
                TokenInfo {
                    token_id: token_yes,
                    outcome: labels[0].to_owned(),
                    neg_risk: false,
                },
                TokenInfo {
                    token_id: token_no,
                    outcome: labels[1].to_owned(),
                    neg_risk: false,
                },
            ],
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ONE,
            liquidity_usd: None,
            volume_24h: None,
            start_date: None,
            end_date: None,
            resolved_at,
            created_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn settlement_transitions_detects_markets() {
        let now = Utc::now();
        let batch = vec![
            // Active → Settled with YES winning: publish (outcome = true).
            registry_market(
                "m-live",
                MarketStatus::Settled,
                Some("Yes"),
                Some(now - chrono::Duration::hours(1)),
            ),
            // Already settled in DB: no re-publish.
            registry_market(
                "m-old",
                MarketStatus::Settled,
                Some("No"),
                Some(now - chrono::Duration::hours(1)),
            ),
            // Settled but never persisted before (bootstrap backfill): skip.
            registry_market(
                "m-new",
                MarketStatus::Settled,
                Some("Yes"),
                Some(now - chrono::Duration::hours(1)),
            ),
            // Not settled: never a candidate.
            registry_market("m-active", MarketStatus::Active, None, None),
        ];
        let prior = HashMap::from([
            (MarketId::new("m-live"), MarketStatus::Active),
            (MarketId::new("m-old"), MarketStatus::Settled),
            (MarketId::new("m-active"), MarketStatus::Active),
        ]);

        let scan = settlement_transitions(&batch, &prior, now);
        assert_eq!(scan.publishable, vec![(MarketId::new("m-live"), true)]);
        assert!(scan.ambiguous.is_empty());
    }

    #[test]
    fn settlement_transitions_maps_false() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m1",
            MarketStatus::Settled,
            Some("No"),
            Some(now - chrono::Duration::hours(1)),
        )];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Paused)]);
        assert_eq!(
            settlement_transitions(&batch, &prior, now).publishable,
            vec![(MarketId::new("m1"), false)]
        );
    }

    #[test]
    fn settlement_missing_not_false() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m1",
            MarketStatus::Settled,
            None,
            Some(now - chrono::Duration::hours(1)),
        )];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Active)]);
        let scan = settlement_transitions(&batch, &prior, now);
        assert!(scan.publishable.is_empty());
        assert_eq!(scan.ambiguous, vec![MarketId::new("m1")]);
    }

    #[test]
    fn settlement_transitions_resolves_identity() {
        let now = Utc::now();
        let resolved_at = Some(now - Duration::hours(1));
        let batch = vec![
            registry_market_with_labels(
                "m-over",
                MarketStatus::Settled,
                Some("Over"),
                resolved_at,
                ["Over", "Under"],
            ),
            registry_market_with_labels(
                "m-under",
                MarketStatus::Settled,
                Some("Under"),
                resolved_at,
                ["Over", "Under"],
            ),
        ];
        let prior = HashMap::from([
            (MarketId::new("m-over"), MarketStatus::Active),
            (MarketId::new("m-under"), MarketStatus::Active),
        ]);

        let scan = settlement_transitions(&batch, &prior, now);
        assert_eq!(
            scan.publishable,
            vec![
                (MarketId::new("m-over"), true),
                (MarketId::new("m-under"), false),
            ]
        );
        assert!(scan.ambiguous.is_empty());
    }

    #[test]
    fn settlement_transitions_stale_resolved() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m-stale",
            MarketStatus::Settled,
            Some("Yes"),
            Some(now - LIVE_RESOLUTION_WINDOW - chrono::Duration::hours(1)),
        )];
        let prior = HashMap::from([(MarketId::new("m-stale"), MarketStatus::Active)]);

        let scan = settlement_transitions(&batch, &prior, now);
        assert!(scan.publishable.is_empty());
        assert!(scan.ambiguous.is_empty());
    }

    #[test]
    fn settlement_transitions_keeps_resolved() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m-recent",
            MarketStatus::Settled,
            Some("Yes"),
            Some(now - chrono::Duration::hours(1)),
        )];
        let prior = HashMap::from([(MarketId::new("m-recent"), MarketStatus::Active)]);

        assert_eq!(
            settlement_transitions(&batch, &prior, now).publishable,
            vec![(MarketId::new("m-recent"), true)]
        );
    }

    #[test]
    fn settlement_empty_publishes_nothing() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m-bootstrap",
            MarketStatus::Settled,
            Some("Yes"),
            Some(now - chrono::Duration::hours(1)),
        )];

        let scan = settlement_transitions(&batch, &HashMap::new(), now);
        assert!(scan.publishable.is_empty());
        assert!(scan.ambiguous.is_empty());
    }

    #[test]
    fn settlement_missing_not_publishable() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m1",
            MarketStatus::Settled,
            Some("Yes"),
            None,
        )];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Active)]);

        let scan = settlement_transitions(&batch, &prior, now);
        assert!(scan.publishable.is_empty());
        assert!(scan.ambiguous.is_empty());
    }

    #[test]
    fn settlement_transitions_not_publishable() {
        let now = Utc::now();
        let batch = vec![registry_market(
            "m1",
            MarketStatus::Settled,
            Some("Yes"),
            Some(now + chrono::Duration::seconds(1)),
        )];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Active)]);

        let scan = settlement_transitions(&batch, &prior, now);
        assert!(scan.publishable.is_empty());
        assert!(scan.ambiguous.is_empty());
    }
}
