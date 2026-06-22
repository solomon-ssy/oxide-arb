//! Phase 0 operational metrics — ingest plane, catalog sync, hotset, shutdown.
//!
//! Endgame detection / execution / risk / settlement / control-factor series
//! were removed in Phase 0. Hotset metric names stay until Phase 2 renames them
//! to quant universe ingest policy (see `07-implementation-phases.md`).

use prometheus::{
    Encoder, Gauge, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

macro_rules! register_counter {
    ($registry:expr, $name:expr, $help:expr) => {{
        let counter = IntCounter::new($name, $help).unwrap();
        $registry.register(Box::new(counter.clone())).unwrap();
        counter
    }};
}

macro_rules! register_gauge_int {
    ($registry:expr, $name:expr, $help:expr) => {{
        let gauge = IntGauge::new($name, $help).unwrap();
        $registry.register(Box::new(gauge.clone())).unwrap();
        gauge
    }};
}

macro_rules! register_gauge_float {
    ($registry:expr, $name:expr, $help:expr) => {{
        let gauge = Gauge::new($name, $help).unwrap();
        $registry.register(Box::new(gauge.clone())).unwrap();
        gauge
    }};
}

macro_rules! register_counter_vec {
    ($registry:expr, $name:expr, $help:expr, $labels:expr) => {{
        let counter_vec = IntCounterVec::new(Opts::new($name, $help), $labels).unwrap();
        $registry.register(Box::new(counter_vec.clone())).unwrap();
        counter_vec
    }};
}

macro_rules! register_gauge_vec {
    ($registry:expr, $name:expr, $help:expr, $labels:expr) => {{
        let gauge_vec = IntGaugeVec::new(Opts::new($name, $help), $labels).unwrap();
        $registry.register(Box::new(gauge_vec.clone())).unwrap();
        gauge_vec
    }};
}

/// Central Prometheus registry for Phase 0 live paths only.
pub struct MetricsHub {
    pub registry: Registry,

    // ── Data pipeline (CLOB WS → BookStore) ─────────────────────────────
    pub ws_events_received: IntCounter,
    pub book_snapshots_applied: IntCounter,
    pub price_changes_applied: IntCounter,
    pub ws_events_dropped: IntCounter,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub ws_shard_connected: IntGaugeVec,
    pub book_store_token_count: IntGauge,

    // ── Backpressure (book apply coalesce / coalescer dedup) ────────────
    pub book_apply_dropped: IntCounter,
    pub book_apply_coalesced_total: IntCounter,
    pub coalescer_dropped: IntCounter,
    pub backpressure_events: IntCounterVec,

    // ── Gamma catalog sync ──────────────────────────────────────────────
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,
    pub gamma_markets_rejected: IntCounterVec,
    pub gamma_markets_paused: IntCounterVec,
    pub catalog_ready: IntGauge,

    // ── WS engine hotset (renamed in Phase 2) ───────────────────────────
    pub hotset_candidates_total: IntCounterVec,
    pub hotset_selected_total: IntCounterVec,
    pub hotset_detection_window_coverage_ratio: Gauge,

    // ── Infra ───────────────────────────────────────────────────────────
    pub async_writer_dropped: IntCounterVec,
    pub shutdown_stage_progress_remaining: IntGaugeVec,
    pub shutdown_stage_timeouts: IntCounterVec,
}

struct PipelineMetrics {
    ws_events_received: IntCounter,
    book_snapshots_applied: IntCounter,
    price_changes_applied: IntCounter,
    ws_events_dropped: IntCounter,
    markets_resolved_ws: IntCounter,
    shard_status_changes: IntCounter,
    ws_shard_connected: IntGaugeVec,
    book_store_token_count: IntGauge,
}

struct BackpressureMetrics {
    book_apply_dropped: IntCounter,
    book_apply_coalesced_total: IntCounter,
    coalescer_dropped: IntCounter,
    backpressure_events: IntCounterVec,
}

struct GammaMetrics {
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    gamma_markets_rejected: IntCounterVec,
    gamma_markets_paused: IntCounterVec,
    catalog_ready: IntGauge,
}

struct HotsetMetrics {
    candidates_total: IntCounterVec,
    selected_total: IntCounterVec,
    detection_window_coverage_ratio: Gauge,
}

struct InfraMetrics {
    async_writer_dropped: IntCounterVec,
    shutdown_stage_progress_remaining: IntGaugeVec,
    shutdown_stage_timeouts: IntCounterVec,
}

fn register_pipeline_metrics(registry: &Registry) -> PipelineMetrics {
    PipelineMetrics {
        ws_events_received: register_counter!(
            registry,
            "quant_pivot_pipeline_ws_events_received_total",
            "WebSocket events received"
        ),
        book_snapshots_applied: register_counter!(
            registry,
            "quant_pivot_pipeline_book_snapshots_applied_total",
            "Book snapshots applied"
        ),
        price_changes_applied: register_counter!(
            registry,
            "quant_pivot_pipeline_price_changes_applied_total",
            "Price-change events applied"
        ),
        ws_events_dropped: register_counter!(
            registry,
            "quant_pivot_pipeline_ws_events_dropped_total",
            "WebSocket events dropped when output channel full"
        ),
        markets_resolved_ws: register_counter!(
            registry,
            "quant_pivot_pipeline_markets_resolved_ws_total",
            "Markets resolved from WS"
        ),
        shard_status_changes: register_counter!(
            registry,
            "quant_pivot_pipeline_shard_status_changes_total",
            "Shard status transitions"
        ),
        ws_shard_connected: register_gauge_vec!(
            registry,
            "quant_pivot_pipeline_ws_shard_connected",
            "Per-shard CLOB WebSocket connection state (1 = connected)",
            &["shard"]
        ),
        book_store_token_count: register_gauge_int!(
            registry,
            "quant_pivot_pipeline_book_store_token_count",
            "Tokens tracked in book store"
        ),
    }
}

fn register_backpressure_metrics(registry: &Registry) -> BackpressureMetrics {
    BackpressureMetrics {
        book_apply_dropped: register_counter!(
            registry,
            "quant_pivot_pipeline_book_apply_dropped_total",
            "WS events dropped when book apply queue full (non-coalescable)"
        ),
        book_apply_coalesced_total: register_counter!(
            registry,
            "quant_pivot_book_apply_coalesced_total",
            "Book apply events coalesced under backpressure"
        ),
        coalescer_dropped: register_counter!(
            registry,
            "quant_pivot_backpressure_coalescer_dropped_total",
            "Coalescer notify events dropped under backpressure"
        ),
        backpressure_events: register_counter_vec!(
            registry,
            "quant_pivot_backpressure_events_total",
            "Backpressure actions by site",
            &["site", "action"]
        ),
    }
}

fn register_gamma_metrics(registry: &Registry) -> GammaMetrics {
    GammaMetrics {
        gamma_sync_duration_ms: register_gauge_int!(
            registry,
            "quant_pivot_gamma_sync_duration_ms",
            "Last Gamma sync duration in milliseconds"
        ),
        gamma_markets_total: register_gauge_int!(
            registry,
            "quant_pivot_gamma_markets_total",
            "Active markets registered after last full sync"
        ),
        gamma_last_sync_success: register_gauge_int!(
            registry,
            "quant_pivot_gamma_last_sync_success",
            "1 when the last Gamma sync succeeded, 0 otherwise"
        ),
        gamma_markets_rejected: register_counter_vec!(
            registry,
            "quant_pivot_gamma_markets_rejected_total",
            "Markets dropped during Gamma catalog normalization, by reason",
            &["reason"]
        ),
        gamma_markets_paused: register_counter_vec!(
            registry,
            "quant_pivot_gamma_markets_paused_total",
            "Markets transitioned to paused during catalog sync, by reason",
            &["reason"]
        ),
        catalog_ready: register_gauge_int!(
            registry,
            "quant_pivot_catalog_ready",
            "1 once the first successful Gamma catalog sync completed"
        ),
    }
}

fn register_hotset_metrics(registry: &Registry) -> HotsetMetrics {
    HotsetMetrics {
        candidates_total: register_counter_vec!(
            registry,
            "quant_pivot_hotset_candidates_total",
            "Engine hotset candidate markets by kind (Phase 2: rename to universe ingest)",
            &["kind"]
        ),
        selected_total: register_counter_vec!(
            registry,
            "quant_pivot_hotset_selected_total",
            "Engine hotset selected markets by kind (Phase 2: rename to universe ingest)",
            &["kind"]
        ),
        detection_window_coverage_ratio: register_gauge_float!(
            registry,
            "quant_pivot_hotset_detection_window_coverage_ratio",
            "Share of eligible markets covered by the detection window (Phase 2 rename)"
        ),
    }
}

fn register_infra_metrics(registry: &Registry) -> InfraMetrics {
    InfraMetrics {
        async_writer_dropped: register_counter_vec!(
            registry,
            "quant_pivot_system_async_writer_dropped_total",
            "Async writer batches dropped by writer name",
            &["writer"]
        ),
        shutdown_stage_progress_remaining: register_gauge_vec!(
            registry,
            "quant_pivot_shutdown_stage_progress_remaining",
            "Tasks remaining during staged shutdown",
            &["stage"]
        ),
        shutdown_stage_timeouts: register_counter_vec!(
            registry,
            "quant_pivot_shutdown_stage_timeouts_total",
            "Shutdown stage drain timeouts",
            &["stage"]
        ),
    }
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = Registry::new();
        let pipeline = register_pipeline_metrics(&registry);
        let backpressure = register_backpressure_metrics(&registry);
        let gamma = register_gamma_metrics(&registry);
        let hotset = register_hotset_metrics(&registry);
        let infra = register_infra_metrics(&registry);

        Self {
            registry,
            ws_events_received: pipeline.ws_events_received,
            book_snapshots_applied: pipeline.book_snapshots_applied,
            price_changes_applied: pipeline.price_changes_applied,
            ws_events_dropped: pipeline.ws_events_dropped,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            ws_shard_connected: pipeline.ws_shard_connected,
            book_store_token_count: pipeline.book_store_token_count,
            book_apply_dropped: backpressure.book_apply_dropped,
            book_apply_coalesced_total: backpressure.book_apply_coalesced_total,
            coalescer_dropped: backpressure.coalescer_dropped,
            backpressure_events: backpressure.backpressure_events,
            gamma_sync_duration_ms: gamma.gamma_sync_duration_ms,
            gamma_markets_total: gamma.gamma_markets_total,
            gamma_last_sync_success: gamma.gamma_last_sync_success,
            gamma_markets_rejected: gamma.gamma_markets_rejected,
            gamma_markets_paused: gamma.gamma_markets_paused,
            catalog_ready: gamma.catalog_ready,
            hotset_candidates_total: hotset.candidates_total,
            hotset_selected_total: hotset.selected_total,
            hotset_detection_window_coverage_ratio: hotset.detection_window_coverage_ratio,
            async_writer_dropped: infra.async_writer_dropped,
            shutdown_stage_progress_remaining: infra.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: infra.shutdown_stage_timeouts,
        }
    }

    pub fn record_shutdown_timeout(&self, stage: &str, abandoned: usize) {
        self.shutdown_stage_timeouts
            .with_label_values(&[stage])
            .inc();
        self.set_shutdown_stage_remaining(stage, abandoned);
    }

    pub fn set_shutdown_stage_remaining(&self, stage: &str, remaining: usize) {
        self.shutdown_stage_progress_remaining
            .with_label_values(&[stage])
            .set(i64::try_from(remaining).unwrap_or(i64::MAX));
    }

    /// Gather all registered metrics in Prometheus text exposition format.
    pub fn gather_prometheus_text(&self) -> Result<(String, Vec<u8>), String> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(4 * 1024);
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|error| format!("prometheus encode failed: {error}"))?;
        Ok((encoder.format_type().to_string(), buffer))
    }
}

impl Default for MetricsHub {
    fn default() -> Self {
        Self::new()
    }
}
