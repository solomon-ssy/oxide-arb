//! Shared HTTP GET helpers with retry classification.

use quant_pivot_error::api::ApiError;

use super::retry::{self, RetryPolicy};

/// HTTP status codes worth retrying (rate limit + transient server errors).
#[must_use]
pub const fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
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
