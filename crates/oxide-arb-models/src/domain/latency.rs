//! End-to-end latency trace markers for WS → scan → dispatch → HTTP.

use std::time::Instant;

/// Monotonic timestamps captured at each hot-path stage (not serialized).
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

    pub fn mark_scan_started(&mut self) {
        self.scan_started = Some(Instant::now());
    }

    pub fn mark_scan_emitted(&mut self) {
        self.scan_emitted = Some(Instant::now());
    }

    pub fn mark_dispatch_started(&mut self) {
        self.dispatch_started = Some(Instant::now());
    }

    pub fn mark_http_sent(&mut self) {
        self.http_sent = Some(Instant::now());
    }

    /// Merge YES/NO token traces: earliest ingress, latest book apply.
    #[must_use]
    pub fn merge_pair(a: Option<&Self>, b: Option<&Self>) -> Self {
        Self {
            ws_ingress: earliest_instant([
                a.and_then(|t| t.ws_ingress),
                b.and_then(|t| t.ws_ingress),
            ]),
            book_applied: latest_instant([
                a.and_then(|t| t.book_applied),
                b.and_then(|t| t.book_applied),
            ]),
            ..Self::default()
        }
    }
}

fn earliest_instant(values: [Option<Instant>; 2]) -> Option<Instant> {
    match (values[0], values[1]) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn latest_instant(values: [Option<Instant>; 2]) -> Option<Instant> {
    match (values[0], values[1]) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
