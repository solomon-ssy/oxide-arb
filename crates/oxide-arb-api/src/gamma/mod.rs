//! Polymarket Gamma API client for market/event discovery.

mod mapper;
mod sync;
pub mod types;

pub use mapper::{GammaCatalogBatch, collect_fee_sync};
pub use types::GammaResolution;

use crate::infra::retry::{self, RetryPolicy};
use chrono::{DateTime, Utc};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::config::GammaConfig;
use oxide_arb_models::domain::market::{EventRegistryInfo, MarketRegistryInfo};
use oxide_arb_models::types::{EventId, MarketId, TokenId};

use types::RawGammaMarket;

/// Gamma API client for market discovery and metadata sync.
///
/// All HTTP calls are wrapped with retry logic (exponential backoff).
pub struct GammaClient {
    config: GammaConfig,
    http: reqwest::Client,
}

impl GammaClient {
    pub fn new(config: GammaConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Full sync: paginate all active events + their markets.
    pub async fn full_sync(&self) -> Result<Vec<EventRegistryInfo>, ApiError> {
        sync::full_sync(&self.http, &self.config).await
    }

    /// Full sync returning both events and their embedded market entries.
    ///
    /// Unlike `full_sync()`, this preserves the per-market metadata needed
    /// by `MarketRegistry`, `MarketCache`, and `FeeCalculator`.
    pub async fn full_sync_detailed(
        &self,
    ) -> Result<(Vec<EventRegistryInfo>, Vec<MarketRegistryInfo>), ApiError> {
        let raw_events = sync::full_sync_raw(&self.http, &self.config).await?;
        let fee_data = mapper::collect_fee_sync(&raw_events);
        let mut events = Vec::new();
        let mut markets = Vec::new();
        for raw in raw_events {
            let event_id = EventId::new(&raw.id);
            let raw_markets = raw.markets.clone().unwrap_or_default();
            for rm in raw_markets {
                markets.push(mapper::map_market(rm, &event_id));
            }
            events.push(mapper::map_event(raw));
        }
        let _ = fee_data; // fee data extracted from same raw payload by caller
        Ok((events, markets))
    }

    /// Full sync returning a `GammaCatalogBatch` with persistence DTOs and registry views.
    pub async fn full_sync_with_fees(&self) -> Result<GammaCatalogBatch, ApiError> {
        let raw_events = sync::full_sync_raw(&self.http, &self.config).await?;
        Ok(mapper::parse_sync_payload(raw_events))
    }

    /// Incremental sync returning a `GammaCatalogBatch` with persistence DTOs and registry views.
    pub async fn incremental_sync_with_fees(
        &self,
        since: DateTime<Utc>,
    ) -> Result<GammaCatalogBatch, ApiError> {
        let raw_events = sync::incremental_sync_raw(&self.http, &self.config, since).await?;
        Ok(mapper::parse_sync_payload(raw_events))
    }

    /// Fetch markets not embedded in an incremental event payload, with bounded concurrency.
    pub async fn fetch_markets_bounded(
        &self,
        market_ids: &[MarketId],
        max_concurrency: usize,
    ) -> Vec<Result<MarketRegistryInfo, ApiError>> {
        use futures_util::stream::{self, StreamExt};

        if market_ids.is_empty() {
            return Vec::new();
        }

        let concurrency = max_concurrency.max(1);
        stream::iter(market_ids.iter().cloned())
            .map(|market_id| async move { self.get_market(&market_id).await })
            .buffer_unordered(concurrency)
            .collect()
            .await
    }

    /// Discover one liquid/active token for smoke tests (first page of active events).
    pub async fn discover_active_token(&self) -> Result<TokenId, ApiError> {
        sync::discover_active_token(&self.http, &self.config).await
    }

    /// Incremental sync: events changed since timestamp.
    pub async fn incremental_sync(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<EventRegistryInfo>, ApiError> {
        sync::incremental_sync(&self.http, &self.config, since).await
    }

    /// Fetch a single market by `condition_id`.
    pub async fn get_market(
        &self,
        condition_id: &MarketId,
    ) -> Result<MarketRegistryInfo, ApiError> {
        let http = self.http.clone();
        let base_url = self.config.base_url.clone();
        let cid = condition_id.as_str().to_owned();

        retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
            let http = http.clone();
            let base_url = base_url.clone();
            let cid = cid.clone();
            async move {
                let url = format!("{base_url}/markets/{cid}");
                let resp = http.get(&url).send().await.map_err(|e| ApiError::Gamma {
                    endpoint: format!("/markets/{cid}"),
                    status: e.status().map_or(0, |s| s.as_u16()),
                    body: e.to_string(),
                })?;

                if !resp.status().is_success() {
                    return Err(ApiError::Gamma {
                        endpoint: format!("/markets/{cid}"),
                        status: resp.status().as_u16(),
                        body: resp.text().await.unwrap_or_default(),
                    });
                }

                let raw: RawGammaMarket = resp.json().await.map_err(|e| ApiError::Deserialize {
                    context: format!("gamma get_market({cid})"),
                    detail: e.to_string(),
                })?;

                let event_id = EventId::new("unknown");
                Ok(mapper::map_market(raw, &event_id))
            }
        })
        .await
    }

    /// Check if market is resolved + its outcome.
    pub async fn get_resolution_status(
        &self,
        condition_id: &MarketId,
    ) -> Result<Option<GammaResolution>, ApiError> {
        let http = self.http.clone();
        let base_url = self.config.base_url.clone();
        let cid = condition_id.as_str().to_owned();

        retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
            let http = http.clone();
            let base_url = base_url.clone();
            let cid = cid.clone();
            async move {
                let url = format!("{base_url}/markets/{cid}");
                let resp = http.get(&url).send().await.map_err(|e| ApiError::Gamma {
                    endpoint: format!("/markets/{cid}"),
                    status: e.status().map_or(0, |s| s.as_u16()),
                    body: e.to_string(),
                })?;

                if !resp.status().is_success() {
                    return Err(ApiError::Gamma {
                        endpoint: format!("/markets/{cid}"),
                        status: resp.status().as_u16(),
                        body: resp.text().await.unwrap_or_default(),
                    });
                }

                let body: serde_json::Value =
                    resp.json().await.map_err(|e| ApiError::Deserialize {
                        context: format!("gamma resolution({cid})"),
                        detail: e.to_string(),
                    })?;

                let closed = body
                    .get("closed")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                if !closed {
                    return Ok(None);
                }

                let outcome = body
                    .get("outcome")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                let resolved_at = body
                    .get("resolved_at")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<DateTime<Utc>>().ok());

                Ok(Some(GammaResolution {
                    resolved: true,
                    outcome: outcome.clone(),
                    resolved_at,
                    winning_outcome: outcome,
                }))
            }
        })
        .await
    }
}
