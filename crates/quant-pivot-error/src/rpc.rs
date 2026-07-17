//! Blockchain RPC errors — Polygon JSON-RPC and contract interactions.

use std::{error::Error, fmt};

/// Errors from on-chain RPC calls (Polygon node, CTF contract, etc.).
pub enum RpcError {
    CallFailed { method: String, reason: String },

    ConnectionFailed(String),

    Timeout { method: String, elapsed_ms: u64 },

    AbiDecode { contract: String, reason: String },

    InsufficientFunds(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CallFailed { method, reason } => write!(
                formatter,
                "RPC call {method} failed: {}",
                redact_urls(reason)
            ),
            Self::ConnectionFailed(reason) => {
                write!(formatter, "RPC connection failed: {}", redact_urls(reason))
            }
            Self::Timeout { method, elapsed_ms } => {
                write!(
                    formatter,
                    "RPC timeout after {elapsed_ms}ms calling {method}"
                )
            }
            Self::AbiDecode { contract, reason } => write!(
                formatter,
                "Contract ABI decode error on {contract}: {}",
                redact_urls(reason)
            ),
            Self::InsufficientFunds(reason) => write!(
                formatter,
                "Insufficient gas or funds: {}",
                redact_urls(reason)
            ),
        }
    }
}

// Error values are frequently logged with `?error`. Keep Debug on the same
// fail-closed rendering path so switching tracing formatters cannot reveal a
// provider API key embedded in a URL path or query string.
impl fmt::Debug for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for RpcError {}

fn redact_urls(message: &str) -> String {
    let mut redacted = String::with_capacity(message.len());
    let mut remaining = message;
    while let Some((offset, scheme)) = next_http_scheme(remaining) {
        redacted.push_str(&remaining[..offset]);
        let url = &remaining[offset..];
        let token_end = url
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(character, ')' | ']' | '}' | '"' | '\'' | ',' | ';')
            })
            .unwrap_or(url.len());
        let token = &url[..token_end];
        let authority_start = scheme.len();
        let authority_end = token[authority_start..]
            .find(['/', '?', '#'])
            .map_or(token.len(), |index| authority_start + index);
        let authority = &token[authority_start..authority_end];
        let host = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);

        redacted.push_str(scheme);
        if host.is_empty() {
            redacted.push_str("<redacted>");
        } else {
            redacted.push_str(host);
        }
        if authority_end < token.len() || authority.contains('@') {
            redacted.push_str("/<redacted>");
        }
        remaining = &url[token_end..];
    }
    redacted.push_str(remaining);
    redacted
}

fn next_http_scheme(message: &str) -> Option<(usize, &'static str)> {
    let http = message.find("http://").map(|offset| (offset, "http://"));
    let https = message.find("https://").map(|offset| (offset, "https://"));
    match (http, https) {
        (Some(http), Some(https)) => Some(if http.0 <= https.0 { http } else { https }),
        (Some(http), None) => Some(http),
        (None, Some(https)) => Some(https),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{RpcError, redact_urls};

    #[test]
    fn display_redacts_rpc_path_query_and_userinfo() {
        let error = RpcError::CallFailed {
            method: "eth_getLogs".to_owned(),
            reason:
                "request failed for https://alice:secret@polygon.example/v2/api-key?token=secret"
                    .to_owned(),
        };

        let rendered = error.to_string();
        assert_eq!(
            rendered,
            "RPC call eth_getLogs failed: request failed for https://polygon.example/<redacted>"
        );
        assert!(!rendered.contains("api-key"));
        assert!(!rendered.contains("secret"));
        assert_eq!(format!("{error:?}"), rendered);
    }

    #[test]
    fn redaction_preserves_punctuation_and_multiple_urls() {
        assert_eq!(
            redact_urls(
                "primary (https://one.example/key), fallback http://two.example/path failed"
            ),
            "primary (https://one.example/<redacted>), fallback http://two.example/<redacted> failed"
        );
    }

    #[test]
    fn host_only_url_is_not_modified() {
        assert_eq!(
            redact_urls("provider https://polygon.example failed"),
            "provider https://polygon.example failed"
        );
    }
}
