//! End-to-end latency trace markers for WS → book apply.

use std::time::Instant;

/// Monotonic latency markers from WS ingress through HTTP dispatch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LatencyTrace {
    pub ws_ingress: Option<Instant>,
    pub book_applied: Option<Instant>,
    pub scan_started: Option<Instant>,
    pub scan_emitted: Option<Instant>,
    pub dispatch_started: Option<Instant>,
    pub http_sent: Option<Instant>,
}

impl LatencyTrace {
    #[must_use]
    pub fn from_ingress(mono: Instant) -> Self {
        Self {
            ws_ingress: Some(mono),
            ..Self::default()
        }
    }
}
