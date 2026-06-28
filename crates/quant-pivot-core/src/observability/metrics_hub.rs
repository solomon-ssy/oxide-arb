//! Operational metrics for the quant-pivot runtime.
//!
//! Covers the ingest plane, catalog sync, subscription ingest, fact writers, and
//! shutdown. Legacy Endgame detection / execution / risk / settlement /
//! control-factor series do not exist here.

use std::time::Duration;

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use quant_pivot_storage::write::AsyncWriterObservability;

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

macro_rules! register_histogram_vec {
    ($registry:expr, $name:expr, $help:expr, $labels:expr, $buckets:expr) => {{
        let histogram_vec = HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            $labels,
        )
        .unwrap();
        $registry.register(Box::new(histogram_vec.clone())).unwrap();
        histogram_vec
    }};
}

macro_rules! register_gauge_vec {
    ($registry:expr, $name:expr, $help:expr, $labels:expr) => {{
        let gauge_vec = IntGaugeVec::new(Opts::new($name, $help), $labels).unwrap();
        $registry.register(Box::new(gauge_vec.clone())).unwrap();
        gauge_vec
    }};
}

/// Lag buckets in seconds for fact ingest histograms (1 ms … 60 s).
const FACT_LAG_BUCKETS_SECS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Report-pipeline wall-clock buckets in seconds (0.1 s … 10 min).
const REPORT_RUN_BUCKETS_SECS: &[f64] = &[
    0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

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

    // ── Backpressure (book apply latest-wins coalesce) ──────────────────
    pub book_apply_dropped: IntCounter,
    pub book_apply_coalesced_total: IntCounter,
    pub backpressure_events: IntCounterVec,

    // ── Gamma catalog sync ──────────────────────────────────────────────
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,
    pub gamma_markets_rejected: IntCounterVec,
    pub gamma_markets_paused: IntCounterVec,
    pub catalog_ready: IntGauge,

    // ── WS engine subscription ingest ────────────────────────────────────
    pub subscription_candidates_total: IntCounterVec,
    pub subscription_selected_total: IntCounterVec,
    pub subscription_window_coverage_ratio: Gauge,

    // ── Data quality ──────────────────────────────────────────────────────
    pub data_quality_tokens: IntGaugeVec,
    pub fact_lag_worst_ms: IntGauge,
    pub fact_lag_seconds: HistogramVec,

    // ── Infra / async writers ─────────────────────────────────────────────
    pub async_writer_dropped: IntCounterVec,
    pub async_writer_queue_depth: IntGaugeVec,
    pub async_writer_flush_failed: IntCounterVec,
    pub shutdown_stage_progress_remaining: IntGaugeVec,
    pub shutdown_stage_timeouts: IntCounterVec,

    // ── Report lifecycle ─────────────────────────────────────────────────
    pub report_generated_total: IntCounterVec,
    pub report_recommendations_total: IntCounterVec,
    pub report_publish_failures_total: IntCounterVec,
    pub report_skipped_empty_total: IntCounter,

    // ── Report scheduler (04.3) ───────────────────────────────────────────
    pub report_schedule_fires_total: IntCounterVec,
    pub report_schedule_skipped_overlap_total: IntCounterVec,
    pub report_schedule_run_duration_seconds: HistogramVec,
    pub report_schedule_active_jobs: IntGauge,
    pub report_expire_swept_total: IntCounter,

    // ── Execution governance (05.1) ───────────────────────────────────────
    /// `1` when the operational kill-switch blocks new auto entries (any
    /// non-`closed` state), `0` when `closed`.
    pub auto_execution_halted: IntGauge,

    // ── Execution admission (05.3) ────────────────────────────────────────
    /// Admission denials by the check id that determined the `Deny` outcome.
    pub admission_denied: IntCounterVec,

    // ── Entry execution (05.4) ────────────────────────────────────────────
    /// Entry orders successfully submitted to the venue (write-ahead committed).
    pub execution_orders_submitted: IntCounter,
    /// Venue fills observed on submission (full or partial).
    pub execution_fills: IntCounter,
    /// Execution-breaker kill-switch trips by triggering dimension.
    pub execution_breaker_trips: IntCounterVec,

    // ── Reconciliation (05.5) ─────────────────────────────────────────────
    /// Reconciliations that resolved to a terminal `Unresolvable` verdict
    /// (capital impaired, kill-switch latched until an operator resolves).
    pub reconciliation_unresolvable: IntCounter,
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

struct SubscriptionMetrics {
    candidates_total: IntCounterVec,
    selected_total: IntCounterVec,
    detection_window_coverage_ratio: Gauge,
}

struct InfraMetrics {
    async_writer_dropped: IntCounterVec,
    async_writer_queue_depth: IntGaugeVec,
    async_writer_flush_failed: IntCounterVec,
    shutdown_stage_progress_remaining: IntGaugeVec,
    shutdown_stage_timeouts: IntCounterVec,
}

struct ReportMetrics {
    generated: IntCounterVec,
    recommendations: IntCounterVec,
    publish_failures: IntCounterVec,
    schedule_fires: IntCounterVec,
    schedule_skipped_overlap: IntCounterVec,
    schedule_run_duration: HistogramVec,
    schedule_active_jobs: IntGauge,
    expire_swept: IntCounter,
    skipped_empty: IntCounter,
}

/// Execution / risk / governance counters (Phase 05.1–05.5).
struct ExecutionMetrics {
    auto_execution_halted: IntGauge,
    admission_denied: IntCounterVec,
    execution_orders_submitted: IntCounter,
    execution_fills: IntCounter,
    execution_breaker_trips: IntCounterVec,
    reconciliation_unresolvable: IntCounter,
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

fn register_subscription_metrics(registry: &Registry) -> SubscriptionMetrics {
    SubscriptionMetrics {
        candidates_total: register_counter_vec!(
            registry,
            "quant_pivot_subscription_candidates_total",
            "WS subscription-ingest candidate markets by kind",
            &["kind"]
        ),
        selected_total: register_counter_vec!(
            registry,
            "quant_pivot_subscription_selected_total",
            "WS subscription-ingest selected markets by kind",
            &["kind"]
        ),
        detection_window_coverage_ratio: register_gauge_float!(
            registry,
            "quant_pivot_subscription_window_coverage_ratio",
            "Share of eligible markets covered by the subscription look-ahead window"
        ),
    }
}

fn register_infra_metrics(registry: &Registry) -> InfraMetrics {
    InfraMetrics {
        async_writer_dropped: register_counter_vec!(
            registry,
            "quant_pivot_system_async_writer_dropped_total",
            "Async writer items dropped by writer name",
            &["writer"]
        ),
        async_writer_queue_depth: register_gauge_vec!(
            registry,
            "quant_pivot_system_async_writer_queue_depth",
            "Pending items in the async writer channel",
            &["writer"]
        ),
        async_writer_flush_failed: register_counter_vec!(
            registry,
            "quant_pivot_system_async_writer_flush_failed_total",
            "Async writer batch flush failures by writer name",
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

fn register_report_metrics(registry: &Registry) -> ReportMetrics {
    ReportMetrics {
        generated: register_counter_vec!(
            registry,
            "quant_pivot_report_generated_total",
            "Recommendation reports generated by kind and terminal status",
            &["kind", "status"]
        ),
        recommendations: register_counter_vec!(
            registry,
            "quant_pivot_report_recommendations_total",
            "Recommendations published by report kind and report status",
            &["kind", "status"]
        ),
        publish_failures: register_counter_vec!(
            registry,
            "quant_pivot_report_publish_failures_total",
            "Report post-commit publish failures by stage",
            &["stage"]
        ),
        schedule_fires: register_counter_vec!(
            registry,
            "quant_pivot_report_schedule_fires_total",
            "Report schedule fires by schedule id and outcome (published/empty/error)",
            &["schedule_id", "outcome"]
        ),
        schedule_skipped_overlap: register_counter_vec!(
            registry,
            "quant_pivot_report_schedule_skipped_overlap_total",
            "Report schedule fires skipped because the prior run was still in flight",
            &["schedule_id"]
        ),
        schedule_run_duration: register_histogram_vec!(
            registry,
            "quant_pivot_report_schedule_run_duration_seconds",
            "Report pipeline wall-clock duration per scheduled fire",
            &["schedule_id"],
            REPORT_RUN_BUCKETS_SECS
        ),
        schedule_active_jobs: register_gauge_int!(
            registry,
            "quant_pivot_report_schedule_active_jobs",
            "Report schedules currently registered with the scheduler"
        ),
        expire_swept: register_counter!(
            registry,
            "quant_pivot_report_expire_swept_total",
            "Reports transitioned to expired by the TTL sweep"
        ),
        skipped_empty: register_counter!(
            registry,
            "quant_pivot_report_skipped_empty_total",
            "Empty reports suppressed by publish_empty_reports=false"
        ),
    }
}

fn register_execution_metrics(registry: &Registry) -> ExecutionMetrics {
    ExecutionMetrics {
        auto_execution_halted: register_gauge_int!(
            registry,
            "quant_auto_execution_halted",
            "1 when the operational kill-switch blocks new auto entries, 0 otherwise"
        ),
        admission_denied: register_counter_vec!(
            registry,
            "quant_admission_denied_total",
            "Execution admission denials by the check id that determined the deny",
            &["check_id"]
        ),
        execution_orders_submitted: register_counter!(
            registry,
            "quant_execution_orders_submitted_total",
            "Entry orders submitted to the venue (write-ahead committed)"
        ),
        execution_fills: register_counter!(
            registry,
            "quant_execution_fills_total",
            "Venue fills observed on submission (full or partial)"
        ),
        execution_breaker_trips: register_counter_vec!(
            registry,
            "quant_execution_breaker_trips_total",
            "Execution-breaker kill-switch trips by triggering dimension",
            &["dimension"]
        ),
        reconciliation_unresolvable: register_counter!(
            registry,
            "quant_reconciliation_unresolvable_total",
            "Reconciliations resolved to a terminal unresolvable verdict"
        ),
    }
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = Registry::new();
        let pipeline = register_pipeline_metrics(&registry);
        let backpressure = register_backpressure_metrics(&registry);
        let gamma = register_gamma_metrics(&registry);
        let subscription = register_subscription_metrics(&registry);
        let infra = register_infra_metrics(&registry);
        let report = register_report_metrics(&registry);
        let data_quality_tokens = register_gauge_vec!(
            &registry,
            "quant_pivot_data_quality_tokens",
            "Live book tokens by data-quality status",
            &["status"]
        );
        let fact_lag_worst_ms = register_gauge_int!(
            &registry,
            "quant_pivot_fact_lag_worst_ms",
            "Peak ingest-side fact lag in the last observation window (milliseconds)"
        );
        let fact_lag_seconds = register_histogram_vec!(
            &registry,
            "quant_pivot_fact_lag_seconds",
            "Ingest-side fact lag (ingestion_time - event_time) by stream",
            &["stream"],
            FACT_LAG_BUCKETS_SECS
        );
        let execution = register_execution_metrics(&registry);

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
            backpressure_events: backpressure.backpressure_events,
            gamma_sync_duration_ms: gamma.gamma_sync_duration_ms,
            gamma_markets_total: gamma.gamma_markets_total,
            gamma_last_sync_success: gamma.gamma_last_sync_success,
            gamma_markets_rejected: gamma.gamma_markets_rejected,
            gamma_markets_paused: gamma.gamma_markets_paused,
            catalog_ready: gamma.catalog_ready,
            subscription_candidates_total: subscription.candidates_total,
            subscription_selected_total: subscription.selected_total,
            subscription_window_coverage_ratio: subscription.detection_window_coverage_ratio,
            data_quality_tokens,
            fact_lag_worst_ms,
            fact_lag_seconds,
            async_writer_dropped: infra.async_writer_dropped,
            async_writer_queue_depth: infra.async_writer_queue_depth,
            async_writer_flush_failed: infra.async_writer_flush_failed,
            shutdown_stage_progress_remaining: infra.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: infra.shutdown_stage_timeouts,
            report_generated_total: report.generated,
            report_recommendations_total: report.recommendations,
            report_publish_failures_total: report.publish_failures,
            report_skipped_empty_total: report.skipped_empty,
            report_schedule_fires_total: report.schedule_fires,
            report_schedule_skipped_overlap_total: report.schedule_skipped_overlap,
            report_schedule_run_duration_seconds: report.schedule_run_duration,
            report_schedule_active_jobs: report.schedule_active_jobs,
            report_expire_swept_total: report.expire_swept,
            auto_execution_halted: execution.auto_execution_halted,
            admission_denied: execution.admission_denied,
            execution_orders_submitted: execution.execution_orders_submitted,
            execution_fills: execution.execution_fills,
            execution_breaker_trips: execution.execution_breaker_trips,
            reconciliation_unresolvable: execution.reconciliation_unresolvable,
        }
    }

    /// Count one entry order submitted to the venue.
    pub fn inc_execution_order_submitted(&self) {
        self.execution_orders_submitted.inc();
    }

    /// Count one venue fill observed on submission.
    pub fn inc_execution_fill(&self) {
        self.execution_fills.inc();
    }

    /// Count one execution-breaker kill-switch trip for a dimension.
    pub fn inc_execution_breaker_trip(&self, dimension: &str) {
        self.execution_breaker_trips
            .with_label_values(&[dimension])
            .inc();
    }

    /// Count one reconciliation that resolved to `Unresolvable`.
    pub fn inc_reconciliation_unresolvable(&self) {
        self.reconciliation_unresolvable.inc();
    }

    /// Publish whether the kill-switch currently blocks new auto entries.
    pub fn set_auto_execution_halted(&self, halted: bool) {
        self.auto_execution_halted.set(i64::from(halted));
    }

    /// Observe one fact-lag sample for a named stream.
    pub fn observe_fact_lag_ms(&self, stream: &str, lag_ms: u64) {
        let whole_secs = lag_ms / 1_000;
        let frac_ms = u32::try_from(lag_ms % 1_000).unwrap_or(u32::MAX);
        let lag_secs =
            f64::from(u32::try_from(whole_secs).unwrap_or(u32::MAX)) + f64::from(frac_ms) / 1_000.0;
        self.fact_lag_seconds
            .with_label_values(&[stream])
            .observe(lag_secs);
    }

    /// Publish the peak fact lag for the elapsed observation window.
    pub fn set_fact_lag_worst_ms(&self, lag_ms: u64) {
        self.fact_lag_worst_ms
            .set(i64::try_from(lag_ms).unwrap_or(i64::MAX));
    }

    /// Build observability handles for one named async writer.
    #[must_use]
    pub fn async_writer_observability(&self, writer: &'static str) -> AsyncWriterObservability {
        AsyncWriterObservability {
            queue_depth: Some(self.async_writer_queue_depth.with_label_values(&[writer])),
            flush_failed: Some(self.async_writer_flush_failed.with_label_values(&[writer])),
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

    /// Record one scheduled report fire: increments the per-outcome counter and
    /// observes the pipeline wall-clock duration (also for failures, capturing
    /// time-to-failure).
    pub fn record_report_schedule_fire(&self, schedule_id: &str, outcome: &str, elapsed: Duration) {
        self.report_schedule_fires_total
            .with_label_values(&[schedule_id, outcome])
            .inc();
        self.report_schedule_run_duration_seconds
            .with_label_values(&[schedule_id])
            .observe(elapsed.as_secs_f64());
    }

    /// Increment the skip-if-running counter for an overlapping fire.
    pub fn inc_report_schedule_skipped_overlap(&self, schedule_id: &str) {
        self.report_schedule_skipped_overlap_total
            .with_label_values(&[schedule_id])
            .inc();
    }

    /// Publish the number of report schedules currently registered.
    pub fn set_report_schedule_active_jobs(&self, count: usize) {
        self.report_schedule_active_jobs
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    /// Count reports expired by the TTL sweep in one pass.
    pub fn inc_report_expire_swept(&self, swept: u64) {
        self.report_expire_swept_total.inc_by(swept);
    }

    /// Publish per-status live-book counts for data-quality observability.
    pub fn set_data_quality_tokens(&self, status: &str, count: u64) {
        self.data_quality_tokens
            .with_label_values(&[status])
            .set(i64::try_from(count).unwrap_or(i64::MAX));
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
