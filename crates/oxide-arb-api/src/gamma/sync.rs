//! Full and incremental sync orchestration with retry.

use crate::infra::retry::{self, RetryPolicy};
use chrono::{DateTime, Utc};
use oxide_arb_error::api::ApiError;
use oxide_arb_models::config::GammaConfig;
use oxide_arb_models::domain::market::EventEntry;

use super::mapper;
use super::types::RawGammaEvent;

pub async fn full_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
) -> Result<Vec<EventEntry>, ApiError> {
    let mut all_events = Vec::new();
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
                    let url =
                        format!("{base_url}/events?active=true&limit={page_size}&offset={offset}");

                    let response = http.get(&url).send().await.map_err(|e| ApiError::Gamma {
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
        for raw in raw_events {
            all_events.push(mapper::map_event(raw));
        }

        if page_len < page_size as usize {
            break;
        }
        offset += u32::try_from(page_len).unwrap_or(u32::MAX);
    }

    Ok(all_events)
}

pub async fn incremental_sync(
    http: &reqwest::Client,
    config: &GammaConfig,
    since: DateTime<Utc>,
) -> Result<Vec<EventEntry>, ApiError> {
    let http = http.clone();
    let base_url = config.base_url.clone();

    let raw_events: Vec<RawGammaEvent> =
        retry::retry_with_policy(&RetryPolicy::gamma_default(), || {
            let http = http.clone();
            let base_url = base_url.clone();
            async move {
                let url = format!(
                    "{}/events?active=true&updated_since={}",
                    base_url,
                    since.to_rfc3339()
                );

                let response = http.get(&url).send().await.map_err(|e| ApiError::Gamma {
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
        .await?;

    Ok(raw_events.into_iter().map(mapper::map_event).collect())
}
