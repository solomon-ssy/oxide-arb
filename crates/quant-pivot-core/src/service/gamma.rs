//! Gamma market catalog sync — full and incremental refresh into registry, cache, and DB.

use crate::{
    governance::LinkageResolverService,
    ingest::{
        market_cache::MarketCache, market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
    service::{
        catalog_lifecycle::apply_past_deadline_to_sync_batch, catalog_readiness::CatalogReadiness,
        system_status_nudge::SystemStatusNudge, ws_subscription::WsSubscriptionCoordinator,
    },
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use quant_pivot_api::{
    fees::FeeCalculator,
    gamma::{CatalogSourceTimestamps, GammaClient, RejectedMarket},
    ws::TOKEN_INTERN,
};
use quant_pivot_error::{QuantError, market::MarketError};
use quant_pivot_models::{
    domain::{
        CATALOG_ORIGIN_GAMMA_SYNC, CatalogCommit, CatalogStatusPort, CatalogSyncFailureStage,
        CatalogSyncKind, CatalogTimestampQuality, CoreEvent, CoreEventPublisher,
        NewCatalogSyncBatch, NewEventCatalogVersion, NewFailedCatalogSyncBatch,
        NewMarketCatalogVersion, market,
        market::{EventRegistryInfo, MarketRegistryInfo, UpsertEvent, UpsertMarket},
    },
    enums::market::MarketStatus,
    hashing::CanonicalDigest,
    types::{
        CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId, MarketCatalogVersionId,
        MarketId,
    },
};
use quant_pivot_repository::traits::{CatalogVersionRepository, MarketRepository};
use quant_pivot_storage::cache::{CacheKey, CacheManager};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

const INCREMENTAL_FETCH_CONCURRENCY: usize = 10;
/// Only publish `market.resolved` for settlements whose `resolved_at` falls
/// within this window — filters historical reconciliation during full sync.
const LIVE_RESOLUTION_WINDOW: chrono::Duration = chrono::Duration::hours(48);

/// Dependencies injected into [`GammaService`].
pub struct GammaServiceDeps {
    pub gamma_client: Arc<GammaClient>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub market_filter: Arc<MarketFilter>,
    pub fee_calculator: Arc<FeeCalculator>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub catalog_version_repo: Arc<dyn CatalogVersionRepository>,
    pub cache: Arc<CacheManager>,
    pub metrics: Arc<MetricsHub>,
    pub catalog: Arc<CatalogReadiness>,
    pub ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    /// Runtime event bus — `market.resolved` WS frames are published from the
    /// authoritative persistence point (settled-status transitions).
    pub events: CoreEventPublisher,
    /// Wake the system-status broadcaster after catalog warmup completes.
    pub status_nudge: SystemStatusNudge,
    /// WS subscription look-ahead window (hours) from deploy config.
    pub subscription_window_hours: u64,
    /// Minimum seconds between full catalog refreshes (from `[market_data.gamma]`).
    pub full_sync_interval_secs: u64,
    /// Offline linkage resolver — runs after successful sync (Phase 11.2.2).
    pub linkage_resolver: Option<Arc<LinkageResolverService>>,
}

pub struct GammaService {
    gamma_client: Arc<GammaClient>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    market_filter: Arc<MarketFilter>,
    fee_calculator: Arc<FeeCalculator>,
    market_repo: Arc<dyn MarketRepository>,
    catalog_version_repo: Arc<dyn CatalogVersionRepository>,
    cache: Arc<CacheManager>,
    metrics: Arc<MetricsHub>,
    catalog: Arc<CatalogReadiness>,
    ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    events: CoreEventPublisher,
    status_nudge: SystemStatusNudge,
    subscription_window_hours: u64,
    full_sync_interval_secs: u64,
    linkage_resolver: Option<Arc<LinkageResolverService>>,
    last_sync_at: parking_lot::Mutex<Option<DateTime<Utc>>>,
}

impl GammaService {
    pub fn new(deps: GammaServiceDeps) -> Self {
        Self {
            gamma_client: deps.gamma_client,
            market_registry: deps.market_registry,
            market_cache: deps.market_cache,
            market_filter: deps.market_filter,
            fee_calculator: deps.fee_calculator,
            market_repo: deps.market_repo,
            catalog_version_repo: deps.catalog_version_repo,
            cache: deps.cache,
            metrics: deps.metrics,
            catalog: deps.catalog,
            ws_subscription: deps.ws_subscription,
            events: deps.events,
            status_nudge: deps.status_nudge,
            subscription_window_hours: deps.subscription_window_hours,
            full_sync_interval_secs: deps.full_sync_interval_secs.max(60),
            linkage_resolver: deps.linkage_resolver,
            last_sync_at: parking_lot::Mutex::new(None),
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
        let Some(resolver) = self.linkage_resolver.as_ref() else {
            return;
        };
        let resolver = Arc::clone(resolver);
        tokio::spawn(async move {
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
        });
    }

    fn publish_catalog_ready(&self) {
        let synced_at = (*self.last_sync_at.lock()).unwrap_or_else(Utc::now);
        let markets =
            u64::try_from(self.market_registry.active_markets().len()).unwrap_or(u64::MAX);
        self.catalog.mark_ready(markets, synced_at);
        self.metrics.catalog_ready.set(1);
        self.status_nudge.nudge();
    }

    async fn sync_inner(&self) -> Result<(), QuantError> {
        let now = Utc::now();
        let last = *self.last_sync_at.lock();
        let needs_full = last.is_none_or(|t| {
            (now - t).num_seconds()
                > i64::try_from(self.full_sync_interval_secs).unwrap_or(i64::MAX)
        });

        let (sync_kind, source_cursor) = if needs_full {
            (CatalogSyncKind::Full, None)
        } else if let Some(since) = last {
            (CatalogSyncKind::Incremental, Some(since))
        } else {
            return Ok(());
        };
        let attempt = CatalogSyncAttempt {
            batch_id: CatalogSyncBatchId::from_v7(),
            sync_kind,
            source_cursor,
            started_at: Utc::now(),
        };
        let result = match source_cursor {
            None => self.full_sync(&attempt).await,
            Some(since) => self.incremental_sync(&attempt, since).await,
        };
        if let Err(error) = result {
            let stage = catalog_failure_stage(&error);
            if let Err(audit_error) = self
                .catalog_version_repo
                .record_failure(NewFailedCatalogSyncBatch {
                    catalog_sync_batch_id: attempt.batch_id,
                    sync_kind: attempt.sync_kind.as_str().to_owned(),
                    source_cursor: attempt.source_cursor,
                    started_at: attempt.started_at,
                    fetched_at: None,
                    failure_stage: stage,
                    failure_detail: error.to_string(),
                })
                .await
            {
                tracing::error!(
                    sync_error = %error,
                    audit_error = %audit_error,
                    "gamma sync failed and its catalog attempt audit could not be persisted"
                );
                return Err(audit_error.into());
            }
            return Err(error);
        }

        *self.last_sync_at.lock() = Some(Utc::now());
        self.cache.invalidate(&CacheKey::ActiveMarkets).await;
        Ok(())
    }

    async fn full_sync(&self, attempt: &CatalogSyncAttempt) -> Result<(), QuantError> {
        let mut batch = self
            .gamma_client
            .full_sync_with_fees()
            .await
            .map_err(QuantError::from)?;
        let fetched_at = Utc::now();

        let event_count = batch.registry_events.len();
        let market_count = batch.registry_markets.len();
        let rejected_count = batch.rejected.len();

        if market_count == 0 {
            return Err(MarketError::EmptyCatalog.into());
        }

        let seen_ids: HashSet<MarketId> = batch
            .registry_markets
            .iter()
            .map(|m| m.market_id.clone())
            .collect();

        let now = Utc::now();
        let past_deadline_paused =
            apply_past_deadline_to_sync_batch(&mut batch.registry_markets, &mut batch.markets, now);
        self.preserve_manual_blocks(&mut batch.registry_markets, &mut batch.markets)
            .await?;

        let deactivated = self.collect_stale_markets(&seen_ids);
        let deactivated_count = deactivated.len();
        let deactivated_upserts = convert_registry_to_upsert(&deactivated);
        batch.registry_markets.extend(deactivated);
        batch.markets.extend(deactivated_upserts);
        self.append_missing_registry_events(&mut batch.registry_events, &batch.registry_markets);

        let newly_settled = self
            .detect_settlement_transitions(&batch.registry_markets)
            .await?;
        let commit = build_catalog_commit(CatalogCommitInput {
            batch_id: attempt.batch_id.clone(),
            sync_kind: CatalogSyncKind::Full,
            source_cursor: None,
            started_at: attempt.started_at,
            fetched_at,
            rejected_count: batch.rejected.len(),
            current_events: batch.events,
            registry_events: &batch.registry_events,
            event_source_timestamps: &batch.event_source_timestamps,
            current_markets: batch.markets,
            registry_markets: &batch.registry_markets,
            market_source_timestamps: &batch.market_source_timestamps,
        })?;
        self.catalog_version_repo.commit(commit).await?;
        self.invalidate_projection_cache(&batch.registry_events, &batch.registry_markets)
            .await;

        self.market_registry
            .register_events(batch.registry_events.clone());
        prewarm_token_intern(&batch.registry_markets);
        self.market_registry
            .register_markets(batch.registry_markets.clone());
        self.fee_calculator
            .ingest_market_fee_schedules(batch.fee_data);
        self.publish_market_resolutions(newly_settled);
        self.market_cache.rebuild();
        self.sync_ws_subscriptions();

        self.record_rejections(&batch.rejected);
        if past_deadline_paused > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["past_deadline"])
                .inc_by(past_deadline_paused);
        }
        if deactivated_count > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["stale_catalog"])
                .inc_by(u64::try_from(deactivated_count).unwrap_or(u64::MAX));
        }

        self.metrics
            .gamma_markets_total
            .set(ToPrimitive::to_i64(&market_count).unwrap_or(i64::MAX));

        tracing::info!(
            events = event_count,
            registered = market_count,
            rejected = rejected_count,
            deactivated = deactivated_count,
            "gamma full sync complete"
        );

        Ok(())
    }

    async fn incremental_sync(
        &self,
        attempt: &CatalogSyncAttempt,
        since: DateTime<Utc>,
    ) -> Result<(), QuantError> {
        let mut batch = self
            .gamma_client
            .incremental_sync_with_fees(since)
            .await
            .map_err(QuantError::from)?;
        let fetched_at = Utc::now();

        let embedded_ids: HashSet<MarketId> = batch
            .registry_markets
            .iter()
            .map(|m| m.market_id.clone())
            .collect();

        let missing_ids = collect_missing_market_ids(&batch.registry_events, &embedded_ids);
        let mut extra_registry = Vec::new();
        let mut extra_upserts = Vec::new();
        let mut fee_data = batch.fee_data;

        if !missing_ids.is_empty() {
            let fetched = self
                .gamma_client
                .fetch_markets_bounded(&missing_ids, INCREMENTAL_FETCH_CONCURRENCY)
                .await;

            for (market_id, result) in missing_ids.into_iter().zip(fetched) {
                match result {
                    Ok(market) => {
                        batch
                            .market_source_timestamps
                            .insert(market_id, market.source_timestamps);
                        extra_registry.push(market.registry);
                    }
                    Err(e) => {
                        tracing::warn!(
                            market_id = %market_id,
                            error = %e,
                            "failed to fetch market during incremental sync"
                        );
                    }
                }
            }

            fee_data.extend(market::collect_fee_data(&extra_registry));
            extra_upserts = convert_registry_to_upsert(&extra_registry);
        }

        let mut all_registry = batch.registry_markets;
        all_registry.extend(extra_registry);

        let now = Utc::now();
        let mut persist_upserts: Vec<UpsertMarket> = batch.markets;
        persist_upserts.extend(extra_upserts);
        let past_deadline_paused =
            apply_past_deadline_to_sync_batch(&mut all_registry, &mut persist_upserts, now);
        self.preserve_manual_blocks(&mut all_registry, &mut persist_upserts)
            .await?;

        let registered = all_registry.len();
        let newly_settled = self.detect_settlement_transitions(&all_registry).await?;
        let commit = build_catalog_commit(CatalogCommitInput {
            batch_id: attempt.batch_id.clone(),
            sync_kind: CatalogSyncKind::Incremental,
            source_cursor: Some(since),
            started_at: attempt.started_at,
            fetched_at,
            rejected_count: batch.rejected.len(),
            current_events: batch.events,
            registry_events: &batch.registry_events,
            event_source_timestamps: &batch.event_source_timestamps,
            current_markets: persist_upserts,
            registry_markets: &all_registry,
            market_source_timestamps: &batch.market_source_timestamps,
        })?;
        self.catalog_version_repo.commit(commit).await?;
        self.invalidate_projection_cache(&batch.registry_events, &all_registry)
            .await;

        self.market_registry
            .register_events(batch.registry_events.clone());
        prewarm_token_intern(&all_registry);
        self.market_registry.register_markets(all_registry);
        self.fee_calculator.ingest_market_fee_schedules(fee_data);
        self.publish_market_resolutions(newly_settled);
        self.market_cache.rebuild();
        self.sync_ws_subscriptions();

        self.record_rejections(&batch.rejected);
        if past_deadline_paused > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["past_deadline"])
                .inc_by(past_deadline_paused);
        }

        tracing::info!(
            events = batch.registry_events.len(),
            registered,
            rejected = batch.rejected.len(),
            "gamma incremental sync complete"
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
    }

    async fn preserve_manual_blocks(
        &self,
        registry_markets: &mut [MarketRegistryInfo],
        upsert_markets: &mut [UpsertMarket],
    ) -> Result<(), QuantError> {
        let ids = registry_markets
            .iter()
            .map(|market| market.market_id.clone())
            .chain(upsert_markets.iter().map(|market| market.market_id.clone()))
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
            .map(|market| market.market_id.as_str().to_owned())
            .collect::<HashSet<_>>();
        if blocked.is_empty() {
            return Ok(());
        }
        for market in registry_markets {
            if blocked.contains(market.market_id.as_str()) {
                market.status = MarketStatus::ManuallyBlocked;
            }
        }
        for market in upsert_markets {
            if blocked.contains(market.market_id.as_str()) {
                market.status = MarketStatus::ManuallyBlocked;
            }
        }
        Ok(())
    }

    /// Compute stale entries without mutating the live registry. The returned
    /// projection is published only after the ledger transaction commits.
    fn collect_stale_markets(&self, seen_ids: &HashSet<MarketId>) -> Vec<MarketRegistryInfo> {
        self.market_registry
            .active_markets()
            .iter()
            .filter(|market_id| !seen_ids.contains(*market_id))
            .filter_map(|market_id| self.market_registry.get_market(market_id))
            .map(|market| {
                let mut market = (*market).clone();
                market.status = MarketStatus::Paused;
                market
            })
            .collect()
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
        self.cache.invalidate(&CacheKey::ActiveMarkets).await;
    }
}

#[derive(Debug, Clone)]
struct CatalogSyncAttempt {
    batch_id: CatalogSyncBatchId,
    sync_kind: CatalogSyncKind,
    source_cursor: Option<DateTime<Utc>>,
    started_at: DateTime<Utc>,
}

const fn catalog_failure_stage(error: &QuantError) -> CatalogSyncFailureStage {
    match error {
        QuantError::Api(_) => CatalogSyncFailureStage::Fetch,
        QuantError::Storage(_) => CatalogSyncFailureStage::Persist,
        _ => CatalogSyncFailureStage::Prepare,
    }
}

struct CatalogCommitInput<'a> {
    batch_id: CatalogSyncBatchId,
    sync_kind: CatalogSyncKind,
    source_cursor: Option<DateTime<Utc>>,
    started_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    rejected_count: usize,
    current_events: Vec<UpsertEvent>,
    registry_events: &'a [EventRegistryInfo],
    event_source_timestamps: &'a HashMap<EventId, CatalogSourceTimestamps>,
    current_markets: Vec<UpsertMarket>,
    registry_markets: &'a [MarketRegistryInfo],
    market_source_timestamps: &'a HashMap<MarketId, CatalogSourceTimestamps>,
}

#[derive(Serialize)]
struct CatalogBatchDigest {
    sync_kind: CatalogSyncKind,
    source_cursor: Option<DateTime<Utc>>,
    fetched_at: DateTime<Utc>,
    rejected_count: usize,
    events: Vec<(EventId, ContentHash)>,
    markets: Vec<(MarketId, ContentHash)>,
}

fn build_catalog_commit(input: CatalogCommitInput<'_>) -> Result<CatalogCommit, QuantError> {
    let CatalogCommitInput {
        sync_kind,
        source_cursor,
        batch_id: catalog_sync_batch_id,
        started_at,
        fetched_at,
        rejected_count,
        current_events,
        registry_events,
        event_source_timestamps,
        current_markets,
        registry_markets,
        market_source_timestamps,
    } = input;
    let event_build = build_event_versions(
        &catalog_sync_batch_id,
        registry_events,
        event_source_timestamps,
        fetched_at,
    )?;
    let market_build = build_market_versions(
        &catalog_sync_batch_id,
        registry_markets,
        market_source_timestamps,
        &event_build.version_ids,
        fetched_at,
    )?;

    let batch_hash = CanonicalDigest::content_hash_json(&CatalogBatchDigest {
        sync_kind,
        source_cursor,
        fetched_at,
        rejected_count,
        events: event_build.digests,
        markets: market_build.digests,
    })?;
    let event_count = catalog_count("event", event_build.versions.len())?;
    let market_count = catalog_count("market", market_build.versions.len())?;
    let rejected_count = catalog_count("rejected", rejected_count)?;

    Ok(CatalogCommit {
        batch: NewCatalogSyncBatch {
            catalog_sync_batch_id,
            sync_kind: sync_kind.as_str().to_owned(),
            source_cursor,
            started_at,
            fetched_at,
            event_count,
            market_count,
            rejected_count,
            batch_hash,
        },
        current_events,
        event_versions: event_build.versions,
        current_markets,
        market_versions: market_build.versions,
    })
}

struct EventVersionBuild {
    version_ids: BTreeMap<EventId, EventCatalogVersionId>,
    versions: Vec<NewEventCatalogVersion>,
    digests: Vec<(EventId, ContentHash)>,
}

fn build_event_versions(
    batch_id: &CatalogSyncBatchId,
    events: &[EventRegistryInfo],
    source_timestamps: &HashMap<EventId, CatalogSourceTimestamps>,
    available_at: DateTime<Utc>,
) -> Result<EventVersionBuild, QuantError> {
    let mut ordered = events.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.event_id.cmp(&right.event_id));
    reject_duplicate_catalog_ids("event", ordered.iter().map(|event| event.event_id.as_str()))?;
    let mut version_ids = BTreeMap::new();
    let mut versions = Vec::with_capacity(ordered.len());
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
        let content_hash = CanonicalDigest::content_hash_json(event)?;
        let version_id = EventCatalogVersionId::from_v7();
        version_ids.insert(event.event_id.clone(), version_id.clone());
        digests.push((event.event_id.clone(), content_hash.clone()));
        versions.push(NewEventCatalogVersion {
            event_catalog_version_id: version_id,
            catalog_sync_batch_id: batch_id.clone(),
            event_id: event.event_id.clone(),
            source_effective_at: timestamps.source_effective_at,
            source_timestamp_quality: timestamps.quality.as_str().to_owned(),
            available_at,
            origin: CATALOG_ORIGIN_GAMMA_SYNC.to_owned(),
            content_hash,
            payload,
        });
    }
    Ok(EventVersionBuild {
        version_ids,
        versions,
        digests,
    })
}

struct MarketVersionBuild {
    versions: Vec<NewMarketCatalogVersion>,
    digests: Vec<(MarketId, ContentHash)>,
}

fn build_market_versions(
    batch_id: &CatalogSyncBatchId,
    markets: &[MarketRegistryInfo],
    source_timestamps: &HashMap<MarketId, CatalogSourceTimestamps>,
    event_version_ids: &BTreeMap<EventId, EventCatalogVersionId>,
    available_at: DateTime<Utc>,
) -> Result<MarketVersionBuild, QuantError> {
    let mut ordered = markets.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.market_id.cmp(&right.market_id));
    reject_duplicate_catalog_ids(
        "market",
        ordered.iter().map(|market| market.market_id.as_str()),
    )?;
    let mut versions = Vec::with_capacity(ordered.len());
    let mut digests = Vec::with_capacity(ordered.len());
    for market in ordered {
        let event_catalog_version_id = event_version_ids
            .get(&market.event_id)
            .cloned()
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
        let content_hash = CanonicalDigest::content_hash_json(market)?;
        digests.push((market.market_id.clone(), content_hash.clone()));
        versions.push(NewMarketCatalogVersion {
            market_catalog_version_id: MarketCatalogVersionId::from_v7(),
            catalog_sync_batch_id: batch_id.clone(),
            event_catalog_version_id,
            market_id: market.market_id.clone(),
            event_id: market.event_id.clone(),
            source_effective_at: timestamps.source_effective_at,
            source_timestamp_quality: timestamps.quality.as_str().to_owned(),
            source_created_at: timestamps.source_created_at,
            available_at,
            origin: CATALOG_ORIGIN_GAMMA_SYNC.to_owned(),
            content_hash,
            payload,
        });
    }
    Ok(MarketVersionBuild { versions, digests })
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
            quality: CatalogTimestampQuality::AvailableAtFallback,
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

fn prewarm_token_intern(markets: &[MarketRegistryInfo]) {
    let token_ids: Vec<&str> = markets
        .iter()
        .flat_map(|market| market.tokens.iter().map(|t| t.token_id.as_str()))
        .collect();
    if token_ids.is_empty() {
        return;
    }
    TOKEN_INTERN.prewarm_token_strs(&token_ids);
    tracing::debug!(
        count = token_ids.len(),
        "token intern pool prewarmed after gamma sync"
    );
}

/// Convert `MarketRegistryInfo` → `UpsertMarket` for individually-fetched and deactivated markets.
fn convert_registry_to_upsert(markets: &[MarketRegistryInfo]) -> Vec<UpsertMarket> {
    let mut out = Vec::with_capacity(markets.len());
    for m in markets {
        match UpsertMarket::try_from(m) {
            Ok(dto) => out.push(dto),
            Err(e) => {
                tracing::warn!(
                    market_id = %m.market_id,
                    error = %e,
                    "skipping market persistence: invalid token pair"
                );
            }
        }
    }
    out
}

fn collect_missing_market_ids(
    events: &[EventRegistryInfo],
    embedded: &HashSet<MarketId>,
) -> Vec<MarketId> {
    let mut missing = Vec::new();
    for event in events {
        for market_id in &event.market_ids {
            if !embedded.contains(market_id) {
                missing.push(market_id.clone());
            }
        }
    }
    missing.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    missing.dedup_by(|a, b| a.as_str() == b.as_str());
    missing
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{
        domain::market::TokenInfo,
        enums::{
            common::{CategorySet, TickSize},
            market::EventStatus,
        },
        types::{EventId, TokenId},
    };
    use rust_decimal::Decimal;

    #[test]
    fn collect_missing_dedupes_and_skips_embedded() {
        let events = vec![EventRegistryInfo {
            event_id: EventId::new("evt-1"),
            title: "t".into(),
            slug: "s".into(),
            series_slug: None,
            status: EventStatus::Active,
            market_ids: vec![
                MarketId::new("m1"),
                MarketId::new("m2"),
                MarketId::new("m1"),
            ],
            categories: CategorySet::EMPTY,
            tags: Vec::new(),
            neg_risk: false,
            end_date: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let embedded = HashSet::from([MarketId::new("m1")]);
        let missing = collect_missing_market_ids(&events, &embedded);
        assert_eq!(missing, vec![MarketId::new("m2")]);
    }

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
            fee_schedule: None,
            start_date: None,
            end_date: None,
            resolved_at,
            created_at: None,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn settlement_transitions_detects_only_newly_settled_known_markets() {
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
    fn settlement_transitions_maps_secondary_token_outcome_to_false() {
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
    fn settlement_transitions_missing_outcome_is_ambiguous_not_false() {
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
    fn settlement_transitions_resolves_custom_labels_by_token_identity() {
        let now = Utc::now();
        let resolved_at = Some(now - chrono::Duration::hours(1));
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
    fn settlement_transitions_suppresses_stale_resolved_at() {
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
    fn settlement_transitions_keeps_recent_resolved_at() {
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
    fn settlement_transitions_bootstrap_empty_prior_publishes_nothing() {
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
    fn settlement_transitions_missing_resolved_at_is_not_publishable() {
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
    fn settlement_transitions_future_resolved_at_is_not_publishable() {
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
