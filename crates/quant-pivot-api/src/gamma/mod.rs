//! Polymarket Gamma API client for market/event discovery.

mod catalog;
mod mapper;
mod resolution;
mod sync;
mod wire;

use catalog::{CatalogEvent, CatalogMarket};
pub use catalog::{CatalogMarketReject, FilteredPrelistingMarket, RejectedMarket};
use futures_util::stream::{self, StreamExt};
use mapper::{CatalogMarketMapCtx, CatalogMarketWithCtx};
pub use mapper::{CatalogSourceTimestamps, GammaCatalogBatch};
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    config::GammaConfig,
    domain::market::{EventRegistryInfo, MarketRegistryInfo},
    enums::{common::CategorySet, market::MarketStatus},
    types::{EventId, MarketId, TokenId},
};
use reqwest::Client;
pub use resolution::GammaResolution;
use wire::WireMarket;

use crate::infra::{
    http::retry_after_ms,
    retry::{self, RetryPolicy},
};

/// One individually fetched market together with unmodified upstream clocks.
pub struct FetchedCatalogMarket {
    pub event: EventRegistryInfo,
    pub event_source_timestamps: CatalogSourceTimestamps,
    pub registry: MarketRegistryInfo,
    pub source_timestamps: CatalogSourceTimestamps,
}

/// Gamma API client for market discovery and metadata sync.
///
/// All HTTP calls are wrapped with retry logic (exponential backoff).
pub struct GammaClient {
    config: GammaConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl GammaClient {
    pub fn new(config: GammaConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            retry_policy: RetryPolicy::gamma_default(),
        }
    }

    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    #[must_use]
    pub const fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Full sync: paginate all active events + their markets.
    pub async fn full_sync(&self) -> Result<Vec<EventRegistryInfo>, ApiError> {
        sync::full_sync(&self.http, &self.config, &self.retry_policy).await
    }

    /// Full sync returning both events and their embedded market entries.
    pub async fn full_sync_detailed(
        &self,
    ) -> Result<(Vec<EventRegistryInfo>, Vec<MarketRegistryInfo>), ApiError> {
        let catalog = sync::full_sync_raw(&self.http, &self.config, &self.retry_policy).await?;
        let batch = GammaCatalogBatch::from(catalog);
        Ok((batch.registry_events, batch.registry_markets))
    }

    /// Full sync returning a `GammaCatalogBatch` with persistence DTOs and registry views.
    pub async fn full_sync_with_fees(&self) -> Result<GammaCatalogBatch, ApiError> {
        let catalog = sync::full_sync_raw(&self.http, &self.config, &self.retry_policy).await?;
        Ok(GammaCatalogBatch::from(catalog))
    }

    /// Fetch markets by condition id with bounded concurrency.
    pub async fn fetch_markets_bounded(
        &self,
        market_ids: &[MarketId],
        max_concurrency: usize,
    ) -> Vec<(MarketId, Result<FetchedCatalogMarket, ApiError>)> {
        if market_ids.is_empty() {
            return Vec::new();
        }

        let concurrency = max_concurrency.max(1);
        stream::iter(market_ids.iter().cloned())
            .map(|market_id| async move {
                let result = self.market_at_source_time(&market_id).await;
                (market_id, result)
            })
            .buffer_unordered(concurrency)
            .collect()
            .await
    }

    /// Discover one liquid/active token for smoke tests (first page of active events).
    pub async fn discover_active_token(&self) -> Result<TokenId, ApiError> {
        sync::discover_active_token(&self.http, &self.config, &self.retry_policy).await
    }

    /// Discover up to `limit` active CLOB token ids (keyset walk).
    pub async fn discover_active_tokens(&self, limit: usize) -> Result<Vec<TokenId>, ApiError> {
        sync::discover_active_tokens(&self.http, &self.config, &self.retry_policy, limit).await
    }

    /// Fetch a single market by `condition_id`.
    ///
    /// The Gamma `/markets/{id}` path only accepts numeric Gamma ids, so the
    /// lookup goes through `GET /markets?condition_ids={cid}`. The response
    /// embeds the parent event, which supplies the real `event_id` and the
    /// tag-derived category memberships.
    pub async fn get_market(
        &self,
        condition_id: &MarketId,
    ) -> Result<MarketRegistryInfo, ApiError> {
        self.market_at_source_time(condition_id)
            .await
            .map(|fetched| fetched.registry)
    }

    async fn market_at_source_time(
        &self,
        condition_id: &MarketId,
    ) -> Result<FetchedCatalogMarket, ApiError> {
        let cid = condition_id.as_str();
        let Some(wire) = self.fetch_condition_market(cid).await? else {
            return Err(ApiError::Gamma {
                endpoint: format!("/markets?condition_ids={cid}"),
                status: 404,
                body: "no market for condition_id".into(),
                retry_after_ms: None,
            });
        };

        let parent = wire.events.as_deref().and_then(<[_]>::first).cloned();
        let Some(parent) = parent else {
            return Err(ApiError::Deserialize {
                context: format!("gamma get_market({cid})"),
                detail: "market payload has no parent event".into(),
            });
        };
        let event_id = EventId::new(&parent.id);
        let tags = parent.tag_slugs();
        let categories = CategorySet::from_slugs(tags.iter().map(String::as_str));
        let catalog_event = CatalogEvent::from(parent);
        let event_source_timestamps = CatalogSourceTimestamps {
            created_at: catalog_event.created_at,
            updated_at: catalog_event.updated_at,
        };
        let event = EventRegistryInfo::from(&catalog_event);

        let catalog = CatalogMarket::try_from(wire).map_err(|reason| ApiError::Deserialize {
            context: format!("gamma get_market({cid})"),
            detail: reason.to_string(),
        })?;
        let source_timestamps = CatalogSourceTimestamps {
            created_at: catalog.created_at,
            updated_at: catalog.updated_at,
        };
        CatalogMarketWithCtx {
            market: catalog,
            ctx: CatalogMarketMapCtx {
                event_id,
                categories,
            },
        }
        .try_into()
        .map(|registry| FetchedCatalogMarket {
            event,
            event_source_timestamps,
            registry,
            source_timestamps,
        })
        .map_err(|reason| ApiError::Deserialize {
            context: format!("gamma get_market({cid})"),
            detail: reason.to_string(),
        })
    }

    /// Check if a market is resolved and which leg won.
    ///
    /// Returns `Ok(None)` when the market is still open or unknown to Gamma;
    /// a `Some` with `winning_token_id: None` means closed-and-resolved but
    /// with ambiguous settlement prices (fail-closed).
    pub async fn get_resolution_status(
        &self,
        condition_id: &MarketId,
    ) -> Result<Option<GammaResolution>, ApiError> {
        let cid = condition_id.as_str();
        let Some(wire) = self.fetch_condition_market(cid).await? else {
            return Ok(None);
        };

        let catalog = CatalogMarket::try_from(wire).map_err(|reason| ApiError::Deserialize {
            context: format!("gamma resolution({cid})"),
            detail: reason.to_string(),
        })?;
        if catalog.status != MarketStatus::Settled {
            return Ok(None);
        }

        Ok(Some(GammaResolution {
            resolved: true,
            winning_token_id: catalog
                .settlement
                .as_ref()
                .map(|settlement| TokenId::new(&settlement.winning_token_id)),
            winning_outcome: catalog
                .settlement
                .map(|settlement| settlement.winning_outcome),
            resolved_at: catalog.resolved_at,
        }))
    }

    /// `GET /markets?condition_ids={cid}` with retry; `Ok(None)` = unknown market.
    async fn fetch_condition_market(&self, cid: &str) -> Result<Option<WireMarket>, ApiError> {
        let http = self.http.clone();
        let base_url = self.config.base_url.clone();
        let cid = cid.to_owned();

        retry::retry_with_policy(&self.retry_policy, || {
            let http = http.clone();
            let base_url = base_url.clone();
            let cid = cid.clone();
            async move {
                let endpoint = format!("/markets?condition_ids={cid}");
                let url = format!("{base_url}{endpoint}");
                let resp = http.get(&url).send().await.map_err(|e| ApiError::Gamma {
                    endpoint: endpoint.clone(),
                    status: e.status().map_or(0, |s| s.as_u16()),
                    body: e.to_string(),
                    retry_after_ms: None,
                })?;

                if !resp.status().is_success() {
                    let retry_after_ms = retry_after_ms(resp.headers());
                    return Err(ApiError::Gamma {
                        endpoint,
                        status: resp.status().as_u16(),
                        body: resp.text().await.unwrap_or_default(),
                        retry_after_ms,
                    });
                }

                let mut markets: Vec<WireMarket> =
                    resp.json().await.map_err(|e| ApiError::Deserialize {
                        context: format!("gamma markets?condition_ids={cid}"),
                        detail: e.to_string(),
                    })?;
                Ok((!markets.is_empty()).then(|| markets.swap_remove(0)))
            }
        })
        .await
    }
}
