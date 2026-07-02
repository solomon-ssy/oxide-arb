//! Gamma market catalog sync — full and incremental refresh into registry, cache, and DB.

use crate::{
    observability::metrics_hub::MetricsHub,
    pipeline::{
        market_cache::MarketCache, market_filter::MarketFilter, market_registry::MarketRegistry,
    },
    service::{
        catalog_lifecycle::apply_past_deadline_to_sync_batch, catalog_readiness::CatalogReadiness,
        system_status_nudge::SystemStatusNudge, ws_subscription::WsSubscriptionCoordinator,
    },
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use quant_pivot_api::{
    fees::FeeCalculator,
    gamma::{GammaClient, RejectedMarket},
    ws::TOKEN_INTERN,
};
use quant_pivot_error::{QuantError, market::MarketError};
use quant_pivot_models::{
    domain::{
        CoreEvent, CoreEventPublisher, market,
        market::{EventRegistryInfo, MarketRegistryInfo, UpsertEvent, UpsertMarket},
    },
    enums::market::MarketStatus,
    types::MarketId,
};
use quant_pivot_repository::traits::{EventRepository, MarketRepository};
use quant_pivot_storage::cache::{CacheKey, CacheManager};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

const INCREMENTAL_FETCH_CONCURRENCY: usize = 10;

/// Dependencies injected into [`GammaService`].
pub struct GammaServiceDeps {
    pub gamma_client: Arc<GammaClient>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub market_filter: Arc<MarketFilter>,
    pub fee_calculator: Arc<FeeCalculator>,
    pub market_repo: Arc<dyn MarketRepository>,
    pub event_repo: Arc<dyn EventRepository>,
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
}

pub struct GammaService {
    gamma_client: Arc<GammaClient>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    market_filter: Arc<MarketFilter>,
    fee_calculator: Arc<FeeCalculator>,
    market_repo: Arc<dyn MarketRepository>,
    event_repo: Arc<dyn EventRepository>,
    cache: Arc<CacheManager>,
    metrics: Arc<MetricsHub>,
    catalog: Arc<CatalogReadiness>,
    ws_subscription: Option<Arc<WsSubscriptionCoordinator>>,
    events: CoreEventPublisher,
    status_nudge: SystemStatusNudge,
    subscription_window_hours: u64,
    full_sync_interval_secs: u64,
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
            event_repo: deps.event_repo,
            cache: deps.cache,
            metrics: deps.metrics,
            catalog: deps.catalog,
            ws_subscription: deps.ws_subscription,
            events: deps.events,
            status_nudge: deps.status_nudge,
            subscription_window_hours: deps.subscription_window_hours,
            full_sync_interval_secs: deps.full_sync_interval_secs.max(60),
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
        }

        result
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

        if needs_full {
            self.full_sync().await?;
        } else if let Some(since) = last {
            self.incremental_sync(since).await?;
        }

        *self.last_sync_at.lock() = Some(Utc::now());
        self.cache.invalidate(&CacheKey::ActiveMarkets).await;
        Ok(())
    }

    async fn full_sync(&self) -> Result<(), QuantError> {
        let mut batch = self
            .gamma_client
            .full_sync_with_fees()
            .await
            .map_err(QuantError::from)?;

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

        self.record_rejections(&batch.rejected);
        let now = Utc::now();
        let past_deadline_paused =
            apply_past_deadline_to_sync_batch(&mut batch.registry_markets, &mut batch.markets, now);
        if past_deadline_paused > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["past_deadline"])
                .inc_by(past_deadline_paused);
        }
        self.preserve_manual_blocks(&mut batch.registry_markets, &mut batch.markets)
            .await;

        self.market_registry.register_events(batch.registry_events);
        prewarm_token_intern(&batch.registry_markets);
        self.market_registry
            .register_markets(batch.registry_markets);
        self.fee_calculator
            .ingest_market_fee_schedules(batch.fee_data);

        let deactivated = self.market_registry.deactivate_stale(&seen_ids);
        let deactivated_count = deactivated.len();
        if deactivated_count > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["stale_catalog"])
                .inc_by(u64::try_from(deactivated_count).unwrap_or(u64::MAX));
        }
        let deactivated_upserts = convert_registry_to_upsert(&deactivated);

        let mut persist_batch = batch.markets;
        persist_batch.extend(deactivated_upserts);
        self.preserve_manual_blocks(&mut [], &mut persist_batch)
            .await;

        self.persist_events(&batch.events).await;
        self.persist_markets(&persist_batch).await;
        self.market_cache.rebuild();
        self.sync_ws_subscriptions();

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

    async fn incremental_sync(&self, since: DateTime<Utc>) -> Result<(), QuantError> {
        let batch = self
            .gamma_client
            .incremental_sync_with_fees(since)
            .await
            .map_err(QuantError::from)?;

        if batch.registry_events.is_empty() {
            return Ok(());
        }

        self.record_rejections(&batch.rejected);
        self.market_registry
            .register_events(batch.registry_events.clone());

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
                    Ok(market) => extra_registry.push(market),
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

        if all_registry.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let mut persist_upserts: Vec<UpsertMarket> = batch.markets;
        persist_upserts.extend(extra_upserts);
        let past_deadline_paused =
            apply_past_deadline_to_sync_batch(&mut all_registry, &mut persist_upserts, now);
        if past_deadline_paused > 0 {
            self.metrics
                .gamma_markets_paused
                .with_label_values(&["past_deadline"])
                .inc_by(past_deadline_paused);
        }
        self.preserve_manual_blocks(&mut all_registry, &mut persist_upserts)
            .await;

        prewarm_token_intern(&all_registry);
        let registered = all_registry.len();
        self.market_registry.register_markets(all_registry);
        self.fee_calculator.ingest_market_fee_schedules(fee_data);

        let persist_batch = persist_upserts;

        self.persist_events(&batch.events).await;
        self.persist_markets(&persist_batch).await;
        self.market_cache.rebuild();
        self.sync_ws_subscriptions();

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

    async fn persist_markets(&self, markets: &[UpsertMarket]) {
        if markets.is_empty() {
            return;
        }
        let newly_settled = self.detect_settlement_transitions(markets).await;
        match self.market_repo.upsert_batch(markets.to_vec()).await {
            Ok(n) => {
                tracing::debug!(count = n, "persisted markets");
                self.publish_market_resolutions(newly_settled);
            }
            Err(e) => tracing::warn!(error = %e, "failed to persist markets"),
        }
    }

    /// Load prior persisted statuses for the settled markets in this batch and
    /// resolve which of them are settling for the first time.
    async fn detect_settlement_transitions(
        &self,
        markets: &[UpsertMarket],
    ) -> Vec<(MarketId, bool)> {
        let ids: Vec<MarketId> = markets
            .iter()
            .filter(|market| market.status == MarketStatus::Settled)
            .map(|market| market.market_id.clone())
            .collect();
        if ids.is_empty() {
            return Vec::new();
        }
        let prior: HashMap<MarketId, MarketStatus> = match self.market_repo.find_by_ids(&ids).await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|row| (row.market_id.clone(), row.status))
                .collect(),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to load prior market statuses; skipping market.resolved publish"
                );
                return Vec::new();
            }
        };
        settlement_transitions(markets, &prior)
    }

    fn publish_market_resolutions(&self, resolved: Vec<(MarketId, bool)>) {
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
    ) {
        let ids = registry_markets
            .iter()
            .map(|market| market.market_id.clone())
            .chain(upsert_markets.iter().map(|market| market.market_id.clone()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return;
        }
        let blocked = match self.market_repo.find_by_ids(&ids).await {
            Ok(markets) => markets
                .into_iter()
                .filter(|market| market.status == MarketStatus::ManuallyBlocked)
                .map(|market| market.market_id.as_str().to_owned())
                .collect::<HashSet<_>>(),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to load current market statuses while preserving manual blocks"
                );
                return;
            }
        };
        if blocked.is_empty() {
            return;
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
    }

    async fn persist_events(&self, events: &[UpsertEvent]) {
        if events.is_empty() {
            return;
        }
        match self.event_repo.upsert_batch(events.to_vec()).await {
            Ok(n) => tracing::debug!(count = n, "persisted events"),
            Err(e) => tracing::warn!(error = %e, "failed to persist events"),
        }
    }
}

/// Markets in `batch` transitioning to [`MarketStatus::Settled`] from a
/// previously persisted non-settled row, paired with whether YES won.
///
/// Requiring a prior row keeps `market.resolved` a true *transition* signal: a
/// fresh bootstrap ingesting thousands of historically settled markets
/// publishes nothing (no operator was watching them), while a watched market
/// settling live publishes exactly once — the upsert makes the row `Settled`,
/// so subsequent syncs skip it.
fn settlement_transitions(
    batch: &[UpsertMarket],
    prior: &HashMap<MarketId, MarketStatus>,
) -> Vec<(MarketId, bool)> {
    batch
        .iter()
        .filter(|market| market.status == MarketStatus::Settled)
        .filter(|market| {
            prior
                .get(&market.market_id)
                .is_some_and(|status| *status != MarketStatus::Settled)
        })
        .map(|market| (market.market_id.clone(), yes_outcome_won(market)))
        .collect()
}

/// Whether the settled market resolved to YES (Gamma settlement carries the
/// winning outcome name; binary markets use "Yes"/"No").
fn yes_outcome_won(market: &UpsertMarket) -> bool {
    market
        .outcome
        .as_deref()
        .is_some_and(|outcome| outcome.eq_ignore_ascii_case("yes"))
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
        enums::common::{CategorySet, TickSize},
        types::{EventId, TokenId},
    };

    #[test]
    fn collect_missing_dedupes_and_skips_embedded() {
        let events = vec![EventRegistryInfo {
            event_id: EventId::new("evt-1"),
            title: "t".into(),
            slug: "s".into(),
            market_ids: vec![
                MarketId::new("m1"),
                MarketId::new("m2"),
                MarketId::new("m1"),
            ],
            categories: CategorySet::EMPTY,
            tags: Vec::new(),
            neg_risk: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let embedded = HashSet::from([MarketId::new("m1")]);
        let missing = collect_missing_market_ids(&events, &embedded);
        assert_eq!(missing, vec![MarketId::new("m2")]);
    }

    fn upsert(id: &str, status: MarketStatus, outcome: Option<&str>) -> UpsertMarket {
        UpsertMarket {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            question: "Test?".into(),
            slug: "test".into(),
            categories: CategorySet::EMPTY,
            status,
            outcome: outcome.map(str::to_owned),
            yes_token_id: TokenId::new(format!("{id}-yes")),
            no_token_id: TokenId::new(format!("{id}-no")),
            tick_size: TickSize::Hundredth,
            neg_risk: false,
            end_date: None,
            resolved_at: None,
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
        }
    }

    #[test]
    fn settlement_transitions_detects_only_newly_settled_known_markets() {
        let batch = vec![
            // Active → Settled with YES winning: publish (outcome = true).
            upsert("m-live", MarketStatus::Settled, Some("Yes")),
            // Already settled in DB: no re-publish.
            upsert("m-old", MarketStatus::Settled, Some("No")),
            // Settled but never persisted before (bootstrap backfill): skip.
            upsert("m-new", MarketStatus::Settled, Some("Yes")),
            // Not settled: never a candidate.
            upsert("m-active", MarketStatus::Active, None),
        ];
        let prior = HashMap::from([
            (MarketId::new("m-live"), MarketStatus::Active),
            (MarketId::new("m-old"), MarketStatus::Settled),
            (MarketId::new("m-active"), MarketStatus::Active),
        ]);

        let resolved = settlement_transitions(&batch, &prior);
        assert_eq!(resolved, vec![(MarketId::new("m-live"), true)]);
    }

    #[test]
    fn settlement_transitions_maps_no_outcome_to_false() {
        let batch = vec![upsert("m1", MarketStatus::Settled, Some("No"))];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Paused)]);
        assert_eq!(
            settlement_transitions(&batch, &prior),
            vec![(MarketId::new("m1"), false)]
        );
    }

    #[test]
    fn settlement_transitions_missing_outcome_defaults_to_false() {
        let batch = vec![upsert("m1", MarketStatus::Settled, None)];
        let prior = HashMap::from([(MarketId::new("m1"), MarketStatus::Active)]);
        assert_eq!(
            settlement_transitions(&batch, &prior),
            vec![(MarketId::new("m1"), false)]
        );
    }
}
