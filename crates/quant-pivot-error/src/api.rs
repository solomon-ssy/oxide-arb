//! API layer errors — HTTP, CLOB, Gamma, and SDK interaction failures.

use thiserror::Error;

/// Errors from Polymarket API interactions (CLOB REST, Gamma, SDK).
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("HTTP {method} {url}: status {status} — {body}")]
    Http {
        method: &'static str,
        url: String,
        status: u16,
        body: String,
        retryable: bool,
    },

    #[error("Rate limited: retry after {retry_after_ms}ms (bucket: {bucket})")]
    RateLimited { retry_after_ms: u64, bucket: String },

    #[error("Gamma API {endpoint}: status {status} — {body}")]
    Gamma {
        endpoint: String,
        status: u16,
        body: String,
        retry_after_ms: Option<u64>,
    },

    #[error("CLOB API {endpoint}: code={code} message={message}")]
    Clob {
        endpoint: String,
        code: String,
        message: String,
        retryable: bool,
    },

    #[error("Deserialization failed ({context}): {detail}")]
    Deserialize { context: String, detail: String },

    #[error(
        "Upstream payload invalid ({context}): content_type={content_type}, body_length={body_length}, body_hash={body_hash} — {detail}"
    )]
    UpstreamPayload {
        context: String,
        content_type: String,
        body_length: usize,
        body_hash: String,
        detail: String,
        retryable: bool,
    },

    #[error("Timeout after {elapsed_ms}ms: {operation}")]
    Timeout { operation: String, elapsed_ms: u64 },

    #[error("SDK error: {0}")]
    Sdk(String),
}

impl ApiError {
    /// Whether this error is safe to retry.
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Http { retryable, .. }
            | Self::Clob { retryable, .. }
            | Self::UpstreamPayload { retryable, .. } => *retryable,
            Self::RateLimited { .. } | Self::Timeout { .. } => true,
            Self::Gamma { status, .. } => *status == 0 || *status == 429 || *status >= 500,
            Self::Deserialize { .. } | Self::Sdk(_) => false,
        }
    }

    /// Suggested wait before retrying (milliseconds), if the API specified one.
    pub const fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::RateLimited { retry_after_ms, .. }
            | Self::Gamma {
                retry_after_ms: Some(retry_after_ms),
                ..
            } => Some(*retry_after_ms),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gamma_5xx_and_429_are_retryable() {
        assert!(
            ApiError::Gamma {
                endpoint: "/events".into(),
                status: 503,
                body: "down".into(),
                retry_after_ms: None,
            }
            .is_retryable()
        );
        assert!(
            ApiError::Gamma {
                endpoint: "/events".into(),
                status: 429,
                body: "rate".into(),
                retry_after_ms: Some(1_000),
            }
            .is_retryable()
        );
        assert!(
            ApiError::Gamma {
                endpoint: "/events".into(),
                status: 0,
                body: "transport failure".into(),
                retry_after_ms: None,
            }
            .is_retryable()
        );
        assert!(
            !ApiError::Gamma {
                endpoint: "/events".into(),
                status: 404,
                body: "missing".into(),
                retry_after_ms: None,
            }
            .is_retryable()
        );
    }

    #[test]
    fn upstream_payload_retryability_is_explicit() {
        let payload = |retryable| ApiError::UpstreamPayload {
            context: "gamma keyset".into(),
            content_type: "application/json".into(),
            body_length: 7,
            body_hash: "blake3:abc".into(),
            detail: "syntax".into(),
            retryable,
        };

        assert!(payload(true).is_retryable());
        assert!(!payload(false).is_retryable());
    }
}
