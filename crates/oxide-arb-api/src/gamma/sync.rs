//! Full and incremental sync orchestration with retry.

use super::{
    catalog::CatalogEvent,
    wire::{GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE, KeysetEventsPage, WireEvent},
};
use crate::infra::retry::{self, RetryPolicy};
use chrono::{DateTime, Utc};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::{
    config::GammaConfig, domain::market::EventRegistryInfo, enums::market::MarketStatus,
    types::TokenId,
};
use url::Url;

const EVENTS_KEYSET_PATH: &str = "/events/keyset";

fn parse_events_base(base_url: &str) -> Result<Url, ApiError> {
    Url::parse(&format!("{base_url}/events")).map_err(|e| ApiError::Gamma {
        endpoint: "/events".into(),
        status: 0,
        body: e.to_string(),
    })
}

fn parse_events_keyset_base(base_url: &str) -> Result<Url, ApiError> {
    Url::parse(&format!("{base_url}{EVENTS_KEYSET_PATH}")).map_err(|e| ApiError::Gamma {
        endpoint: EVENTS_KEYSET_PATH.into(),
        status: 0,
        body: e.to_string(),
    })
}

fn effective_keyset_page_size(page_size: u32) -> u32 {
    page_size.clamp(1, GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE)
}

fn events_keyset_url(
    base_url: &str,
    page_size: u32,
    after_cursor: Option<&str>,
) -> Result<Url, ApiError> {
    let mut url = parse_events_keyset_base(base_url)?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("closed", "false");
        pairs.append_pair("limit", &page_size.to_string());
        if let Some(cursor) = after_cursor {
            pairs.append_pair("after_cursor", cursor);
        }
    }
    Ok(url)
}

fn events_incremental_url(base_url: &str, since: DateTime<Utc>) -> Result<Url, ApiError> {
    let mut url = parse_events_base(base_url)?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("active", "true");
        pairs.append_pair("updated_since", &since.to_rfc3339());
    }
    Ok(url)
}

fn normalize_wire_events(events: Vec<WireEvent>) -> Vec<CatalogEvent> {
    events.into_iter().map(CatalogEvent::from).collect()
}

async fn fetch_events_keyset_page(
    http: &reqwest::Client,
    config: &GammaConfig,
    after_cursor: Option<&str>,
) -> Result<KeysetEventsPage, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();
    let page_size = effective_keyset_page_size(config.page_size);

    retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
        let http = http.clone();
        let base_url = base_url.clone();
        let after_cursor = after_cursor.map(str::to_owned);
        async move {
            let url = events_keyset_url(&base_url, page_size, after_cursor.as_deref())?;
            let response = http.get(url).send().await.map_err(|e| ApiError::Gamma {
                endpoint: EVENTS_KEYSET_PATH.into(),
                status: e.status().map_or(0, |s| s.as_u16()),
                body: e.to_string(),
            })?;

            if !response.status().is_success() {
                return Err(ApiError::Gamma {
                    endpoint: EVENTS_KEYSET_PATH.into(),
                    status: response.status().as_u16(),
                    body: response.text().await.unwrap_or_default(),
                });
            }

            response
                .json::<KeysetEventsPage>()
                .await
                .map_err(|e| ApiError::Deserialize {
                    context: "gamma full_sync keyset page".into(),
                    detail: e.to_string(),
                })
        }
    })
    .await
}

/// Paginate all open events via keyset cursor and normalize to catalog rows.
pub async fn full_sync_raw(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<Vec<CatalogEvent>, ApiError> {
    let mut catalog = Vec::new();
    let mut after_cursor: Option<String> = None;

    loop {
        let page = fetch_events_keyset_page(http, config, after_cursor.as_deref()).await?;
        let page_len = page.events.len();
        catalog.extend(normalize_wire_events(page.events));

        match page.next_cursor {
            None => break,
            Some(_) if page_len == 0 => break,
            Some(cursor) => after_cursor = Some(cursor),
        }
    }

    Ok(catalog)
}

pub async fn full_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<Vec<EventRegistryInfo>, ApiError> {
    let events = full_sync_raw(http, config).await?;
    Ok(events
        .into_iter()
        .map(|event| event.to_registry_info())
        .collect())
}

pub async fn incremental_sync_raw(
    http: &reqwest::Client,
    config: &GammaConfig,
    since: DateTime<Utc>,
) -> Result<Vec<CatalogEvent>, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();

    let wire_events: Vec<WireEvent> =
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
                    .json::<Vec<WireEvent>>()
                    .await
                    .map_err(|e| ApiError::Deserialize {
                        context: "gamma incremental_sync".into(),
                        detail: e.to_string(),
                    })
            }
        })
        .await?;

    Ok(normalize_wire_events(wire_events))
}

pub async fn incremental_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
    since: DateTime<Utc>,
) -> Result<Vec<EventRegistryInfo>, ApiError> {
    let events = incremental_sync_raw(http, config, since).await?;
    Ok(events
        .into_iter()
        .map(|event| event.to_registry_info())
        .collect())
}

/// Return the first active token from the first keyset page.
pub async fn discover_active_token(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<TokenId, ApiError> {
    let mut config = config.clone();
    config.page_size = effective_keyset_page_size(config.page_size.min(50));

    let page = fetch_events_keyset_page(http, &config, None).await?;
    let catalog = normalize_wire_events(page.events);

    for event in &catalog {
        for market in &event.markets {
            if market.status != MarketStatus::Active {
                continue;
            }
            if let Some(token) = market.tokens.first() {
                return Ok(TokenId::new(&token.token_id));
            }
        }
    }

    Err(ApiError::Gamma {
        endpoint: EVENTS_KEYSET_PATH.into(),
        status: 0,
        body: "no active token found in first Gamma keyset page".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::{effective_keyset_page_size, events_keyset_url};
    use crate::gamma::wire::GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE;

    #[test]
    fn keyset_page_size_is_clamped_to_gamma_max() {
        assert_eq!(effective_keyset_page_size(0), 1);
        assert_eq!(effective_keyset_page_size(100), 100);
        assert_eq!(
            effective_keyset_page_size(GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE + 1),
            GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE
        );
    }

    #[test]
    fn keyset_url_encodes_closed_filter_and_cursor() {
        let url = events_keyset_url("https://gamma-api.polymarket.com", 100, Some("cursor-1"))
            .expect("url");
        let query = url.query().expect("query");
        assert!(query.contains("closed=false"));
        assert!(query.contains("limit=100"));
        assert!(query.contains("after_cursor=cursor-1"));
    }

    #[test]
    fn keyset_first_page_omits_after_cursor() {
        let url = events_keyset_url("https://gamma-api.polymarket.com", 50, None).expect("url");
        let query = url.query().expect("query");
        assert!(!query.contains("after_cursor"));
    }
}
