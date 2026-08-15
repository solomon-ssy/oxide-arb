//! Authoritative active-event keyset scan orchestration with retry.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_util::StreamExt;
use quant_pivot_error::api::ApiError;
use quant_pivot_models::{
    config::GammaConfig, domain::market::EventRegistryInfo, enums::market::MarketStatus,
    types::TokenId,
};
use reqwest::{Client, Response, header::CONTENT_TYPE};
use url::Url;

use super::{
    catalog::CatalogEvent,
    wire::{GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE, KeysetEventsPage, WireEvent},
};
use crate::infra::{
    http::retry_after_ms,
    retry::{self, RetryPolicy},
};

const EVENTS_KEYSET_PATH: &str = "/events/keyset";
const MAX_KEYSET_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_KEYSET_RESPONSE_BYTES_U64: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
enum GammaScanScope {
    Active,
    Historical { cutoff: DateTime<Utc> },
}

struct GammaResponseBody {
    bytes: Vec<u8>,
    content_type: String,
    body_hash: String,
}

impl GammaResponseBody {
    fn summary(&self) -> String {
        format!(
            "content_type={}, body_length={}, body_hash={}",
            self.content_type,
            self.bytes.len(),
            self.body_hash
        )
    }
}

async fn read_bounded_keyset_body(response: Response) -> Result<GammaResponseBody, ApiError> {
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing>")
        .to_owned();
    if response
        .content_length()
        .is_some_and(|length| length > MAX_KEYSET_RESPONSE_BYTES_U64)
    {
        return Err(upstream_payload_error(
            &[],
            content_type,
            format!("declared content length exceeds {MAX_KEYSET_RESPONSE_BYTES} byte limit"),
            false,
        ));
    }

    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ApiError::Gamma {
            endpoint: EVENTS_KEYSET_PATH.into(),
            status: 502,
            body: format!("response body stream failed: {error}"),
            retry_after_ms: None,
        })?;
        let next_length = bytes.len().saturating_add(chunk.len());
        if next_length > MAX_KEYSET_RESPONSE_BYTES {
            return Err(upstream_payload_error(
                &bytes,
                content_type,
                format!("streamed body exceeds {MAX_KEYSET_RESPONSE_BYTES} byte limit"),
                false,
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let body_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    Ok(GammaResponseBody {
        bytes,
        content_type,
        body_hash,
    })
}

fn upstream_payload_error(
    bytes: &[u8],
    content_type: String,
    detail: String,
    retryable: bool,
) -> ApiError {
    ApiError::UpstreamPayload {
        context: "gamma full_sync keyset page".into(),
        content_type,
        body_length: bytes.len(),
        body_hash: format!("blake3:{}", blake3::hash(bytes).to_hex()),
        detail,
        retryable,
    }
}

fn parse_events_keyset_base(base_url: &str) -> Result<Url, ApiError> {
    Url::parse(&format!("{base_url}{EVENTS_KEYSET_PATH}")).map_err(|e| ApiError::Gamma {
        endpoint: EVENTS_KEYSET_PATH.into(),
        status: 0,
        body: e.to_string(),
        retry_after_ms: None,
    })
}

fn effective_keyset_page_size(page_size: u32) -> u32 {
    page_size.clamp(1, GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE)
}

fn events_keyset_url(
    base_url: &str,
    scope: GammaScanScope,
    page_size: u32,
    after_cursor: Option<&str>,
) -> Result<Url, ApiError> {
    let mut url = parse_events_keyset_base(base_url)?;
    {
        let mut pairs = url.query_pairs_mut();
        match scope {
            GammaScanScope::Active => {
                pairs.append_pair("active", "true");
                pairs.append_pair("closed", "false");
            }
            GammaScanScope::Historical { cutoff } => {
                pairs.append_pair("active", "false");
                pairs.append_pair("closed", "true");
                pairs.append_pair(
                    "end_date_min",
                    &cutoff.to_rfc3339_opts(SecondsFormat::Secs, true),
                );
            }
        }
        pairs.append_pair("limit", &page_size.to_string());
        if let Some(cursor) = after_cursor {
            pairs.append_pair("after_cursor", cursor);
        }
    }
    Ok(url)
}

fn normalize_wire_events(events: Vec<WireEvent>) -> Vec<CatalogEvent> {
    events.into_iter().map(CatalogEvent::from).collect()
}

async fn fetch_events_keyset_page(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
    request_count: &Arc<AtomicU32>,
    scope: GammaScanScope,
    page_size: u32,
    after_cursor: Option<&str>,
) -> Result<KeysetEventsPage, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();
    let page_size = effective_keyset_page_size(page_size);
    let max_requests = config.max_keyset_requests;
    let request_count = Arc::clone(request_count);

    retry::retry_with_policy(retry_policy, || {
        let http = http.clone();
        let base_url = base_url.clone();
        let after_cursor = after_cursor.map(str::to_owned);
        let request_count = Arc::clone(&request_count);
        async move {
            let request_number = request_count.fetch_add(1, Ordering::Relaxed) + 1;
            if request_number > max_requests {
                return Err(ApiError::Deserialize {
                    context: "gamma keyset pagination budget".into(),
                    detail: format!(
                        "HTTP request budget exhausted: max_keyset_requests={max_requests}"
                    ),
                });
            }
            let url = events_keyset_url(&base_url, scope, page_size, after_cursor.as_deref())?;
            let response = http.get(url).send().await.map_err(|e| ApiError::Gamma {
                endpoint: EVENTS_KEYSET_PATH.into(),
                status: e.status().map_or(0, |s| s.as_u16()),
                body: e.to_string(),
                retry_after_ms: None,
            })?;

            let status = response.status();
            if !status.is_success() {
                let retry_after_ms = retry_after_ms(response.headers());
                let body = read_bounded_keyset_body(response).await?;
                return Err(ApiError::Gamma {
                    endpoint: EVENTS_KEYSET_PATH.into(),
                    status: status.as_u16(),
                    body: body.summary(),
                    retry_after_ms,
                });
            }

            let body = read_bounded_keyset_body(response).await?;
            serde_json::from_slice::<KeysetEventsPage>(&body.bytes).map_err(|error| {
                let retryable = matches!(
                    error.classify(),
                    serde_json::error::Category::Eof | serde_json::error::Category::Syntax
                );
                ApiError::UpstreamPayload {
                    context: "gamma full_sync keyset page".into(),
                    content_type: body.content_type,
                    body_length: body.bytes.len(),
                    body_hash: body.body_hash,
                    detail: error.to_string(),
                    retryable,
                }
            })
        }
    })
    .await
}

async fn walk_keyset_pages<F>(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
    request_count: &Arc<AtomicU32>,
    scope: GammaScanScope,
    page_size: u32,
    mut visit: F,
) -> Result<(), ApiError>
where
    F: FnMut(Vec<CatalogEvent>) -> bool,
{
    let mut pages = 0_u32;
    let mut after_cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();

    loop {
        if pages >= config.max_keyset_pages {
            return Err(ApiError::Deserialize {
                context: "gamma keyset pagination budget".into(),
                detail: format!(
                    "page budget exhausted while continuation remained: max_keyset_pages={}",
                    config.max_keyset_pages
                ),
            });
        }
        let page = fetch_events_keyset_page(
            http,
            config,
            retry_policy,
            request_count,
            scope,
            page_size,
            after_cursor.as_deref(),
        )
        .await?;
        pages = pages.saturating_add(1);
        let page_len = page.events.len();
        if !visit(normalize_wire_events(page.events)) {
            return Ok(());
        }

        match page.next_cursor {
            None => return Ok(()),
            Some(cursor) if cursor.trim().is_empty() => {
                return Err(ApiError::Deserialize {
                    context: "gamma keyset pagination".into(),
                    detail: "empty continuation cursor".into(),
                });
            }
            Some(cursor) if page_len == 0 => {
                return Err(ApiError::Deserialize {
                    context: "gamma keyset pagination".into(),
                    detail: format!("empty page returned a continuation cursor `{cursor}`"),
                });
            }
            Some(cursor) => {
                if !seen_cursors.insert(cursor.clone()) {
                    return Err(ApiError::Deserialize {
                        context: "gamma keyset pagination".into(),
                        detail: format!("cursor loop detected at `{cursor}`"),
                    });
                }
                after_cursor = Some(cursor);
            }
        }
    }
}

/// Paginate all open events via keyset cursor and normalize to catalog rows.
pub async fn full_sync_raw(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
) -> Result<Vec<CatalogEvent>, ApiError> {
    let request_count = Arc::new(AtomicU32::new(0));
    let mut catalog = BTreeMap::new();
    let mut duplicate = None;
    walk_keyset_pages(
        http,
        config,
        retry_policy,
        &request_count,
        GammaScanScope::Active,
        config.page_size,
        |events| {
            duplicate = append_catalog(&mut catalog, events);
            duplicate.is_none()
        },
    )
    .await?;
    reject_catalog_duplicate(duplicate)?;
    let cutoff = Utc::now() - Duration::days(i64::from(config.historical_identity_days));
    let mut duplicate = None;
    walk_keyset_pages(
        http,
        config,
        retry_policy,
        &request_count,
        GammaScanScope::Historical { cutoff },
        config.page_size,
        |events| {
            duplicate = append_catalog(&mut catalog, events);
            duplicate.is_none()
        },
    )
    .await?;
    reject_catalog_duplicate(duplicate)?;

    Ok(catalog.into_values().collect())
}

fn append_catalog(
    catalog: &mut BTreeMap<String, CatalogEvent>,
    events: Vec<CatalogEvent>,
) -> Option<String> {
    for event in events {
        let event_id = event.id.clone();
        if catalog.insert(event_id.clone(), event).is_some() {
            return Some(event_id);
        }
    }
    None
}

fn reject_catalog_duplicate(duplicate: Option<String>) -> Result<(), ApiError> {
    duplicate.map_or(Ok(()), |event_id| {
        Err(ApiError::Deserialize {
            context: "gamma keyset identity".into(),
            detail: format!("duplicate event `{event_id}` across canonical catalog scan"),
        })
    })
}

pub async fn full_sync(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
) -> Result<Vec<EventRegistryInfo>, ApiError> {
    let events = full_sync_raw(http, config, retry_policy).await?;
    Ok(events
        .into_iter()
        .map(|event| EventRegistryInfo::from(&event))
        .collect())
}

/// Return the first active token found by the bounded keyset scan.
pub async fn discover_active_token(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
) -> Result<TokenId, ApiError> {
    discover_active_tokens(http, config, retry_policy, 1)
        .await?
        .pop()
        .ok_or_else(|| ApiError::Gamma {
            endpoint: EVENTS_KEYSET_PATH.into(),
            status: 0,
            body: "no active token found in Gamma keyset scan".into(),
            retry_after_ms: None,
        })
}

/// Collect up to `limit` active CLOB token ids by walking Gamma keyset pages.
pub async fn discover_active_tokens(
    http: &Client,
    config: &GammaConfig,
    retry_policy: &RetryPolicy,
    limit: usize,
) -> Result<Vec<TokenId>, ApiError> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut tokens = Vec::with_capacity(limit);
    let request_count = Arc::new(AtomicU32::new(0));
    walk_keyset_pages(
        http,
        config,
        retry_policy,
        &request_count,
        GammaScanScope::Active,
        config.page_size.min(50),
        |catalog| {
            for event in &catalog {
                for market in &event.markets {
                    if market.status != MarketStatus::Active {
                        continue;
                    }
                    for token in &market.tokens {
                        tokens.push(TokenId::new(&token.token_id));
                        if tokens.len() >= limit {
                            return false;
                        }
                    }
                }
            }
            true
        },
    )
    .await?;

    if tokens.is_empty() {
        return Err(ApiError::Gamma {
            endpoint: EVENTS_KEYSET_PATH.into(),
            status: 0,
            body: format!("no active tokens found while collecting limit={limit}"),
            retry_after_ms: None,
        });
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{GammaScanScope, effective_keyset_page_size, events_keyset_url};
    use crate::gamma::wire::GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE;

    #[test]
    fn keyset_page_size_max() {
        assert_eq!(effective_keyset_page_size(0), 1);
        assert_eq!(effective_keyset_page_size(100), 100);
        assert_eq!(
            effective_keyset_page_size(GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE + 1),
            GAMMA_EVENTS_KEYSET_MAX_PAGE_SIZE
        );
    }

    #[test]
    fn keyset_url_encodes_cursor() {
        let url = events_keyset_url(
            "https://gamma-api.polymarket.com",
            GammaScanScope::Active,
            100,
            Some("cursor-1"),
        )
        .expect("url");
        let query = url.query().expect("query");
        assert!(query.contains("active=true"));
        assert!(query.contains("closed=false"));
        assert!(query.contains("limit=100"));
        assert!(query.contains("after_cursor=cursor-1"));
    }

    #[test]
    fn keyset_omits_after_cursor() {
        let cutoff = Utc
            .with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
            .single()
            .expect("cutoff");
        let url = events_keyset_url(
            "https://gamma-api.polymarket.com",
            GammaScanScope::Historical { cutoff },
            50,
            None,
        )
        .expect("url");
        let query = url.query().expect("query");
        assert!(!query.contains("after_cursor"));
        assert!(query.contains("active=false"));
        assert!(query.contains("closed=true"));
        assert!(query.contains("end_date_min=2026-01-02T03%3A04%3A05Z"));
    }
}
