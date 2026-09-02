//! Map Polymarket SDK errors into domain [`ApiError`] with retry semantics.

use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Status};
use quant_pivot_error::api::{ApiError, ClobOrderError};
use reqwest::{Error as ReqwestError, Method, StatusCode};
use serde_json::Error as JsonError;

/// Local wrapper for orphan-safe [`From`] into [`ApiError`].
#[derive(Debug, Clone, Copy)]
pub struct SdkClobError<'a>(pub &'a SdkError);

impl SdkClobError<'_> {
    /// Preserve transport/status failures while classifying any remaining SDK
    /// read failure as malformed snapshot evidence.
    pub fn snapshot(self, context: &'static str) -> ApiError {
        match ApiError::from(self) {
            ApiError::Sdk(detail) => ClobOrderError::MalformedSnapshot { context, detail }.into(),
            error => error,
        }
    }
}

impl From<SdkClobError<'_>> for ApiError {
    fn from(err: SdkClobError<'_>) -> Self {
        map_sdk_error(err.0)
    }
}

fn map_sdk_error(err: &SdkError) -> ApiError {
    if let Some(status) = err.downcast_ref::<Status>() {
        return map_http_status(status);
    }
    if let Some(error) = err.downcast_ref::<ReqwestError>() {
        return ApiError::Http {
            method: "HTTP",
            url: error.url().map_or_else(String::new, ToString::to_string),
            status: error.status().map_or(0, |status| status.as_u16()),
            body: error.to_string(),
            retryable: error.is_timeout() || error.is_connect() || error.is_body(),
        };
    }
    if let Some(error) = err.downcast_ref::<JsonError>() {
        return ApiError::Deserialize {
            context: "Polymarket SDK response".to_owned(),
            detail: error.to_string(),
        };
    }

    match err.kind() {
        Kind::Geoblock => ApiError::Clob {
            endpoint: "clob".into(),
            code: "geoblock".into(),
            message: err.to_string(),
            retryable: false,
        },
        Kind::Status
        | Kind::Validation
        | Kind::WebSocket
        | Kind::Synchronization
        | Kind::Internal
        | _ => ApiError::Sdk(err.to_string()),
    }
}

fn map_http_status(status: &Status) -> ApiError {
    let code = status.status_code;

    if code == StatusCode::TOO_MANY_REQUESTS {
        return ApiError::RateLimited {
            retry_after_ms: parse_retry_after_ms(&status.message),
            bucket: format!("{} {}", status.method, status.path),
        };
    }

    ApiError::Http {
        method: http_method_label(&status.method),
        url: status.path.clone(),
        status: code.as_u16(),
        body: status.message.clone(),
        retryable: is_retryable_status(code),
    }
}

fn is_retryable_status(code: StatusCode) -> bool {
    code == StatusCode::TOO_MANY_REQUESTS
        || code == StatusCode::REQUEST_TIMEOUT
        || code.is_server_error()
}

const fn http_method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::DELETE => "DELETE",
        Method::PUT => "PUT",
        Method::PATCH => "PATCH",
        _ => "HTTP",
    }
}

/// Best-effort parse of `Retry-After` hints embedded in error text (seconds).
fn parse_retry_after_ms(message: &str) -> u64 {
    for token in message.split_whitespace() {
        if let Ok(secs) = token.parse::<u64>() {
            return secs.saturating_mul(1000);
        }
    }
    1000
}

#[cfg(test)]
mod tests {
    use reqwest::Method;

    use super::*;

    #[test]
    fn maps_429_rate_limited() {
        let err = SdkError::status(
            StatusCode::TOO_MANY_REQUESTS,
            Method::POST,
            "/order".into(),
            "retry after 2",
        );
        let api = ApiError::from(SdkClobError(&err));
        assert!(matches!(api, ApiError::RateLimited { .. }));
        assert!(api.is_retryable());
        assert_eq!(api.retry_after_ms(), Some(2000));
    }

    #[test]
    fn maps_503_retryable_http() {
        let err = SdkError::status(
            StatusCode::SERVICE_UNAVAILABLE,
            Method::GET,
            "/book".into(),
            "unavailable",
        );
        let api = ApiError::from(SdkClobError(&err));
        assert!(matches!(
            api,
            ApiError::Http {
                retryable: true,
                ..
            }
        ));
        assert!(api.is_retryable());
    }

    #[test]
    fn maps_400_not_retryable() {
        let err = SdkError::status(
            StatusCode::BAD_REQUEST,
            Method::POST,
            "/order".into(),
            "invalid",
        );
        let api = ApiError::from(SdkClobError(&err));
        assert!(!api.is_retryable());
    }

    #[test]
    fn snapshot_preserves_transport() {
        let err = SdkError::status(
            StatusCode::SERVICE_UNAVAILABLE,
            Method::GET,
            "/book".into(),
            "unavailable",
        );
        let api = SdkClobError(&err).snapshot("order book");
        assert!(matches!(
            api,
            ApiError::Http {
                status: 503,
                retryable: true,
                ..
            }
        ));
    }

    #[test]
    fn snapshot_types_malformed() {
        let err = SdkError::validation("invalid order book response");
        let api = SdkClobError(&err).snapshot("order book");
        assert!(matches!(
            api,
            ApiError::ClobOrder(ClobOrderError::MalformedSnapshot {
                context: "order book",
                ..
            })
        ));
    }
}
