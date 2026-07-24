//! Shared HTTP GET helpers with retry classification.

use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use quant_pivot_error::api::ApiError;
use reqwest::{
    Client, Response, StatusCode,
    header::{CONTENT_TYPE, HeaderMap, RETRY_AFTER},
};

use super::retry::{self, RetryPolicy};

async fn response_error(response: Response, url: String) -> ApiError {
    let status = response.status().as_u16();
    let retry_after = retry_after_ms(response.headers());
    let body = response.text().await.unwrap_or_default();
    if matches!(status, 418 | 429)
        && let Some(retry_after_ms) = retry_after
    {
        return ApiError::RateLimited {
            retry_after_ms,
            bucket: url,
        };
    }
    ApiError::Http {
        method: "GET",
        url,
        status,
        body,
        retryable: is_retryable_status(status),
    }
}

/// HTTP status codes worth retrying (rate limit + transient server errors).
#[must_use]
pub const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Parse RFC-compliant `Retry-After` seconds or HTTP-date into milliseconds.
#[must_use]
pub(crate) fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    let duration = if let Ok(seconds) = value.parse::<u64>() {
        Duration::from_secs(seconds)
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(SystemTime::now())
            .unwrap_or_default()
    };
    u64::try_from(duration.as_millis()).ok()
}

/// Perform one GET and return the response body text on success.
pub async fn get_text_with_retry(
    http: &Client,
    retry_policy: &RetryPolicy,
    url: &str,
) -> Result<String, ApiError> {
    retry::retry_with_policy(retry_policy, || {
        let http = http.clone();
        let url = url.to_owned();
        async move {
            let response = http
                .get(&url)
                .send()
                .await
                .map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                })?;
            let status = response.status();
            if status.is_success() {
                return response.text().await.map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                });
            }
            Err(response_error(response, url).await)
        }
    })
    .await
}

/// Perform one GET and return response bytes. `404` is represented as `None`
/// for immutable public-data partitions that may not have been published yet.
pub async fn get_optional_bytes(
    http: &Client,
    retry_policy: &RetryPolicy,
    url: &str,
) -> Result<Option<Vec<u8>>, ApiError> {
    retry::retry_with_policy(retry_policy, || {
        let http = http.clone();
        let url = url.to_owned();
        async move {
            let response = http
                .get(&url)
                .send()
                .await
                .map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                })?;
            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if status.is_success() {
                return response
                    .bytes()
                    .await
                    .map(|bytes| Some(bytes.to_vec()))
                    .map_err(|error| ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: 0,
                        body: error.to_string(),
                        retryable: true,
                    });
            }
            Err(response_error(response, url).await)
        }
    })
    .await
}

/// Perform one retrying GET while streaming into a caller-bounded byte buffer.
/// `404` is represented as `None` for unpublished immutable partitions.
pub async fn get_optional_bounded_bytes(
    http: &Client,
    retry_policy: &RetryPolicy,
    url: &str,
    context: &'static str,
    max_response_bytes: usize,
) -> Result<Option<Vec<u8>>, ApiError> {
    retry::retry_with_policy(retry_policy, || {
        let http = http.clone();
        let url = url.to_owned();
        async move {
            let response = http
                .get(&url)
                .send()
                .await
                .map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                })?;
            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                return Err(response_error(response, url).await);
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<missing>")
                .to_owned();
            let max_response_bytes_u64 = u64::try_from(max_response_bytes).unwrap_or(u64::MAX);
            if response
                .content_length()
                .is_some_and(|length| length > max_response_bytes_u64)
            {
                return Err(ApiError::UpstreamPayload {
                    context: context.to_owned(),
                    content_type,
                    body_length: 0,
                    body_hash: format!("blake3:{}", blake3::hash(&[]).to_hex()),
                    detail: format!(
                        "declared content length exceeds {max_response_bytes} byte limit"
                    ),
                    retryable: false,
                });
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: format!("response body stream failed: {error}"),
                    retryable: true,
                })?;
                let next_length = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                    ApiError::UpstreamPayload {
                        context: context.to_owned(),
                        content_type: content_type.clone(),
                        body_length: bytes.len(),
                        body_hash: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
                        detail: "streamed body length overflow".to_owned(),
                        retryable: false,
                    }
                })?;
                if next_length > max_response_bytes {
                    return Err(ApiError::UpstreamPayload {
                        context: context.to_owned(),
                        content_type,
                        body_length: next_length,
                        body_hash: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
                        detail: format!("streamed body exceeds {max_response_bytes} byte limit"),
                        retryable: false,
                    });
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some(bytes))
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::api::ApiError;
    use reqwest::{
        Client,
        header::{HeaderMap, HeaderValue, RETRY_AFTER},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::{get_optional_bounded_bytes, response_error, retry_after_ms};
    use crate::infra::retry::RetryPolicy;

    #[test]
    fn retry_after_seconds_milliseconds() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(retry_after_ms(&headers), Some(7_000));
    }

    #[test]
    fn invalid_retry_after_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("not-a-date"));
        assert_eq!(retry_after_ms(&headers), None);
    }

    #[tokio::test]
    async fn rate_preserves_after_backoff() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/limited"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "7")
                    .set_body_string("slow down"),
            )
            .mount(&server)
            .await;
        let url = format!("{}/limited", server.uri());
        let response = Client::new().get(&url).send().await.expect("response");
        let error = response_error(response, url.clone()).await;
        assert!(matches!(
            error,
            ApiError::RateLimited {
                retry_after_ms: 7_000,
                bucket,
            } if bucket == url
        ));
    }

    #[tokio::test]
    async fn optional_distinguishes_rejects_oversize() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/oversize"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0_u8; 8]))
            .mount(&server)
            .await;
        let http = Client::new();
        let policy = RetryPolicy::gamma_default();
        let missing = get_optional_bounded_bytes(
            &http,
            &policy,
            &format!("{}/missing", server.uri()),
            "test body",
            4,
        )
        .await
        .expect("missing response");
        assert!(missing.is_none());
        let oversize = get_optional_bounded_bytes(
            &http,
            &policy,
            &format!("{}/oversize", server.uri()),
            "test body",
            4,
        )
        .await;
        assert!(matches!(oversize, Err(ApiError::UpstreamPayload { .. })));
    }
}
