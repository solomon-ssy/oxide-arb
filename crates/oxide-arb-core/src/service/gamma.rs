//! Gamma market catalog sync — full and incremental refresh into registry, cache, and DB.

use crate::{
    observability::metrics_hub::MetricsHub,
    pipeline::{market_cache::MarketCache, market_registry::MarketRegistry},
    service::cache_invalidation::invalidate_post_gamma_sync,
};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_api::{fees::FeeCalculator, gamma::GammaClient, ws::TOKEN_INTERN};
use oxide_arb_error::{OxideError, market::MarketError};
use oxide_arb_models::{
    domain::{
        market,
        market::{EventRegistryInfo, MarketRegistryInfo, UpsertEvent, UpsertMarket},
    },
    types::MarketId,
};
use oxide_arb_repository::{
    postgres::{PgEventRepository, PgMarketRepository},
    traits::{EventRepository, MarketRepository},
};
use oxide_arb_storage::cache::TieredCache;
use std::{collections::HashSet, sync::Arc, time::Instant};

const INCREMENTAL_FETCH_CONCURRENCY: usize = 10;

/// Dependencies injected into [`GammaService`].
pub struct GammaServiceDeps {
    pub gamma_client: Arc<GammaClient>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub fee_calculator: Arc<FeeCalculator>,
    pub market_repo: Arc<PgMarketRepository>,
    pub event_repo: Arc<PgEventRepository>,
    pub cache: Arc<TieredCache>,
    pub metrics: Arc<MetricsHub>,
    /// Minimum seconds between full catalog refreshes (from `[market_data.gamma]`).
    pub full_sync_interval_secs: u64,
}

pub struct GammaService {
    gamma_client: Arc<GammaClient>,
    market_registry: Arc<MarketRegistry>,
    market_cache: Arc<MarketCache>,
    fee_calculator: Arc<FeeCalculator>,
    market_repo: Arc<PgMarketRepository>,
    event_repo: Arc<PgEventRepository>,
    cache: Arc<TieredCache>,
    metrics: Arc<MetricsHub>,
    full_sync_interval_secs: u64,
    last_sync_at: parking_lot::Mutex<Option<DateTime<Utc>>>,
}

impl GammaService {
    pub fn new(deps: GammaServiceDeps) -> Self {
        Self {
            gamma_client: deps.gamma_client,
            market_registry: deps.market_registry,
            market_cache: deps.market_cache,
            fee_calculator: deps.fee_calculator,
            market_repo: deps.market_repo,
            event_repo: deps.event_repo,
            cache: deps.cache,
            metrics: deps.metrics,
            full_sync_interval_secs: deps.full_sync_interval_secs.max(60),
            last_sync_at: parking_lot::Mutex::new(None),
        }
    }

    pub async fn sync(&self) -> Result<(), OxideError> {
        let timer = Instant::now();
        let result = self.sync_inner().await;
        let elapsed_ms = ToPrimitive::to_i64(&timer.elapsed().as_millis()).unwrap_or(i64::MAX);

        self.metrics.gamma_sync_duration_ms.set(elapsed_ms);
        self.metrics
            .gamma_last_sync_success
            .set(i64::from(result.is_ok()));

        result
    }

    async fn sync_inner(&self) -> Result<(), OxideError> {
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
        invalidate_post_gamma_sync(&self.cache).await;
        Ok(())
    }

    async fn full_sync(&self) -> Result<(), OxideError> {
        let batch = self
            .gamma_client
            .full_sync_with_fees()
            .await
            .map_err(OxideError::from)?;

        let event_count = batch.registry_events.len();
        let market_count = batch.registry_markets.len();

        if market_count == 0 {
            return Err(MarketError::EmptyCatalog.into());
        }

        let seen_ids: HashSet<MarketId> = batch
            .registry_markets
            .iter()
            .map(|m| m.market_id.clone())
            .collect();

        tracing::info!(
            events = event_count,
            markets = market_count,
            "gamma full sync fetched"
        );

        self.market_registry.register_events(batch.registry_events);
        prewarm_token_intern(&batch.registry_markets);
        self.market_registry
            .register_markets(batch.registry_markets);
        self.fee_calculator.ingest_gamma_markets(&batch.fee_data);

        let deactivated = self.market_registry.deactivate_stale(&seen_ids);
        let deactivated_upserts = convert_registry_to_upsert(&deactivated);

        let mut persist_batch = batch.markets;
        persist_batch.extend(deactivated_upserts);

        self.persist_events(&batch.events).await;
        self.persist_markets(&persist_batch).await;
        self.market_cache.rebuild();

        self.metrics
            .gamma_markets_total
            .set(ToPrimitive::to_i64(&market_count).unwrap_or(i64::MAX));

        tracing::info!(
            events = event_count,
            markets = market_count,
            "gamma full sync complete"
        );

        Ok(())
    }

    async fn incremental_sync(&self, since: DateTime<Utc>) -> Result<(), OxideError> {
        let batch = self
            .gamma_client
            .incremental_sync_with_fees(since)
            .await
            .map_err(OxideError::from)?;

        if batch.registry_events.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            events = batch.registry_events.len(),
            "gamma incremental sync fetched"
        );

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

        prewarm_token_intern(&all_registry);
        self.market_registry.register_markets(all_registry);
        self.fee_calculator.ingest_gamma_markets(&fee_data);

        let mut persist_batch = batch.markets;
        persist_batch.extend(extra_upserts);

        self.persist_events(&batch.events).await;
        self.persist_markets(&persist_batch).await;
        self.market_cache.rebuild();

        Ok(())
    }

    async fn persist_markets(&self, markets: &[UpsertMarket]) {
        if markets.is_empty() {
            return;
        }
        match self.market_repo.upsert_batch(markets.to_vec()).await {
            Ok(n) => tracing::debug!(count = n, "persisted markets"),
            Err(e) => tracing::warn!(error = %e, "failed to persist markets"),
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
    use oxide_arb_models::types::EventId;

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
            neg_risk: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let embedded = HashSet::from([MarketId::new("m1")]);
        let missing = collect_missing_market_ids(&events, &embedded);
        assert_eq!(missing, vec![MarketId::new("m2")]);
    }
}
