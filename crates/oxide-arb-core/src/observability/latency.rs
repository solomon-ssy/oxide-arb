//! Latency segment observers wired to Prometheus histograms.

use std::sync::Arc;
use std::time::Instant;

use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::domain::latency::LatencyTrace;

use crate::observability::metrics_hub::MetricsHub;

fn duration_us(start: Instant, end: Instant) -> f64 {
    end.duration_since(start).as_secs_f64() * 1_000_000.0
}

pub fn observe_ws_to_scan(trace: &LatencyTrace, metrics: &MetricsHub) {
    if let (Some(ws), Some(scan)) = (trace.ws_ingress, trace.scan_emitted) {
        if scan >= ws {
            metrics.latency_ws_to_scan_us.observe(duration_us(ws, scan));
        }
    }
}

pub fn observe_scan_to_dispatch(trace: &LatencyTrace, metrics: &MetricsHub) {
    if let (Some(scan), Some(dispatch)) = (trace.scan_emitted, trace.dispatch_started) {
        if dispatch >= scan {
            metrics
                .latency_scan_to_dispatch_us
                .observe(duration_us(scan, dispatch));
        }
    }
}

pub fn observe_tick_to_http(trace: &LatencyTrace, metrics: &MetricsHub) {
    if let (Some(dispatch), Some(http)) = (trace.dispatch_started, trace.http_sent) {
        if http >= dispatch {
            metrics
                .latency_tick_to_http_us
                .observe(duration_us(dispatch, http));
        }
    }
}

/// Stamp `dispatch_started` on a scored opportunity before fast-lane routing.
#[must_use]
pub fn stamp_dispatch_started(scored: Arc<ScoredOpportunity>) -> Arc<ScoredOpportunity> {
    match Arc::try_unwrap(scored) {
        Ok(mut inner) => {
            inner.trace.mark_dispatch_started();
            Arc::new(inner)
        }
        Err(arc) => {
            let mut inner = (*arc).clone();
            inner.trace.mark_dispatch_started();
            Arc::new(inner)
        }
    }
}
