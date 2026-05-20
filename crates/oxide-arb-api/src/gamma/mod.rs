//! Polymarket Gamma API client for market/event discovery.

mod mapper;
mod sync;
pub mod types;

pub use mapper::collect_fee_sync;
pub use types::GammaResolution;

use crate::infra::retry::{self, RetryPolicy};
use chrono::{DateTime, Utc};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::config::GammaConfig;
use oxide_arb_models::domain::market::{EventEntry, MarketEntry};
use oxide_arb_models::types::{EventId, MarketId};

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
    pub async fn full_sync(&self) -> Result<Vec<EventEntry>, ApiError> {
        sync::full_sync(&self.http, &self.config).await
    }

    /// Incremental sync: events changed since timestamp.
    pub async fn incremental_sync(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<EventEntry>, ApiError> {
        sync::incremental_sync(&self.http, &self.config, since).await
    }

    /// Fetch a single market by `condition_id`.
    pub async fn get_market(&self, condition_id: &MarketId) -> Result<MarketEntry, ApiError> {
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
