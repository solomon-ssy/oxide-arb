//! Shared HTTP GET helpers with retry classification.

use std::time::{Duration, SystemTime};

use quant_pivot_error::api::ApiError;
use reqwest::header::{HeaderMap, RETRY_AFTER};

use super::retry::{self, RetryPolicy};

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
    http: &reqwest::Client,
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
            let code = status.as_u16();
            Err(ApiError::Http {
                method: "GET",
                url: url.clone(),
                status: code,
                body: response.text().await.unwrap_or_default(),
                retryable: is_retryable_status(code),
            })
        }
    })
    .await
}

/// Perform one GET and return response bytes. `404` is represented as `None`
/// for immutable public-data partitions that may not have been published yet.
pub async fn get_optional_bytes_with_retry(
    http: &reqwest::Client,
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
            if status == reqwest::StatusCode::NOT_FOUND {
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
            let code = status.as_u16();
            Err(ApiError::Http {
                method: "GET",
                url: url.clone(),
                status: code,
                body: response.text().await.unwrap_or_default(),
                retryable: is_retryable_status(code),
            })
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::retry_after_ms;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    #[test]
    fn retry_after_seconds_are_converted_to_milliseconds() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(retry_after_ms(&headers), Some(7_000));
    }

    #[test]
    fn invalid_retry_after_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("not-a-date"));
        assert_eq!(retry_after_ms(&headers), None);
    }
}
