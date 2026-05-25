//! Full and incremental sync orchestration with retry.

use crate::infra::retry::{self, RetryPolicy};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::api::ApiError;
use oxide_arb_models::config::GammaConfig;
use oxide_arb_models::domain::market::EventRegistryInfo;
use oxide_arb_models::types::TokenId;
use url::Url;

use super::mapper;
use super::types::RawGammaEvent;

fn parse_events_base(base_url: &str) -> Result<Url, ApiError> {
    Url::parse(&format!("{base_url}/events")).map_err(|e| ApiError::Gamma {
        endpoint: "/events".into(),
        status: 0,
        body: e.to_string(),
    })
}

fn events_page_url(base_url: &str, page_size: u32, offset: u32) -> Result<Url, ApiError> {
    let mut url = parse_events_base(base_url)?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("active", "true");
        pairs.append_pair("limit", &page_size.to_string());
        pairs.append_pair("offset", &offset.to_string());
    }
    Ok(url)
}

fn events_incremental_url(base_url: &str, since: DateTime<Utc>) -> Result<Url, ApiError> {
    let mut url = parse_events_base(base_url)?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("active", "true");
        // RFC3339 may contain '+' — must be query-encoded (not string-interpolated).
        pairs.append_pair("updated_since", &since.to_rfc3339());
    }
    Ok(url)
}

/// Paginate all active events, returning raw Gamma API DTOs.
pub async fn full_sync_raw(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<Vec<RawGammaEvent>, ApiError> {
    let mut all_raw = Vec::new();
    let mut offset = 0u32;
    let page_size = config.page_size;

    loop {
        let http = http.clone();
        let base_url = config.base_url.clone();

        let raw_events: Vec<RawGammaEvent> =
            retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
                let http = http.clone();
                let base_url = base_url.clone();
                async move {
                    let url = events_page_url(&base_url, page_size, offset)?;
                    let response = http.get(url).send().await.map_err(|e| ApiError::Gamma {
                        endpoint: "/events".into(),
                        status: e.status().map_or(0, |s| s.as_u16()),
                        body: e.to_string(),
                    })?;

                    if !response.status().is_success() {
                        return Err(ApiError::Gamma {
                            endpoint: "/events".into(),
                            status: response.status().as_u16(),
                            body: response.text().await.unwrap_or_default(),
                        });
                    }

                    response
                        .json::<Vec<RawGammaEvent>>()
                        .await
                        .map_err(|e| ApiError::Deserialize {
                            context: "gamma full_sync page".into(),
                            detail: e.to_string(),
                        })
                }
            })
            .await?;

        let page_len = raw_events.len();
        all_raw.extend(raw_events);

        if page_len < ToPrimitive::to_usize(&page_size).unwrap_or(usize::MAX) {
            break;
        }
        offset = offset.saturating_add(ToPrimitive::to_u32(&page_len).unwrap_or(u32::MAX));
    }

    Ok(all_raw)
}

pub async fn full_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<Vec<EventRegistryInfo>, ApiError> {
    let raw = full_sync_raw(http, config).await?;
    Ok(raw.into_iter().map(mapper::map_event).collect())
}

pub async fn incremental_sync_raw(
    http: &reqwest::Client,
    config: &GammaConfig,
    since: DateTime<Utc>,
) -> Result<Vec<RawGammaEvent>, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();

    retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
        let http = http.clone();
        let base_url = base_url.clone();
        async move {
            let url = events_incremental_url(&base_url, since)?;

            let response = http.get(url).send().await.map_err(|e| ApiError::Gamma {
                endpoint: "/events?updated_since".into(),
                status: e.status().map_or(0, |s| s.as_u16()),
                body: e.to_string(),
            })?;

            if !response.status().is_success() {
                return Err(ApiError::Gamma {
                    endpoint: "/events?updated_since".into(),
                    status: response.status().as_u16(),
                    body: response.text().await.unwrap_or_default(),
                });
            }

            response
                .json::<Vec<RawGammaEvent>>()
                .await
                .map_err(|e| ApiError::Deserialize {
                    context: "gamma incremental_sync".into(),
                    detail: e.to_string(),
                })
        }
    })
    .await
}

pub async fn incremental_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
    since: DateTime<Utc>,
) -> Result<Vec<EventRegistryInfo>, ApiError> {
    let raw = incremental_sync_raw(http, config, since).await?;
    Ok(raw.into_iter().map(mapper::map_event).collect())
}

/// Return the first active token from a single Gamma events page.
///
/// Used by network integration tests and startup smoke checks to avoid hard-coding
/// token IDs that go stale when markets close.
pub async fn discover_active_token(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<TokenId, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();
    let page_size = config.page_size.min(50);

    let raw_events: Vec<RawGammaEvent> =
        retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
            let http = http.clone();
            let base_url = base_url.clone();
            async move {
                let url = events_page_url(&base_url, page_size, 0)?;
                let response = http.get(url).send().await.map_err(|e| ApiError::Gamma {
                    endpoint: "/events".into(),
                    status: e.status().map_or(0, |s| s.as_u16()),
                    body: e.to_string(),
                })?;

                if !response.status().is_success() {
                    return Err(ApiError::Gamma {
                        endpoint: "/events".into(),
                        status: response.status().as_u16(),
                        body: response.text().await.unwrap_or_default(),
                    });
                }

                response
                    .json::<Vec<RawGammaEvent>>()
                    .await
                    .map_err(|e| ApiError::Deserialize {
                        context: "gamma discover_active_token".into(),
                        detail: e.to_string(),
                    })
            }
        })
        .await?;

    for ev in &raw_events {
        for market in ev.markets.as_deref().unwrap_or(&[]) {
            if market.closed.unwrap_or(false) || !market.active.unwrap_or(true) {
                continue;
            }
            if let Some(token) = market.tokens.as_deref().and_then(|t| t.first()) {
                return Ok(TokenId::new(&token.token_id));
            }
        }
    }

    Err(ApiError::Gamma {
        endpoint: "/events".into(),
        status: 0,
        body: "no active token found in first Gamma page".into(),
    })
}
