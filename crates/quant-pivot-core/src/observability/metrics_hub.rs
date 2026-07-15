//! Operational metrics for the quant-pivot runtime.
//!
//! Covers the ingest plane, catalog sync, subscription ingest, fact writers, and
//! shutdown. Legacy Endgame detection / execution / risk / settlement /
//! control-factor series do not exist here.

use std::sync::Arc;

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use quant_pivot_storage::write::AsyncWriterObservability;

/// Convert an integer millisecond lag into fractional seconds for histograms.
fn lag_secs_from_ms(lag_ms: u64) -> f64 {
    let whole_secs = lag_ms / 1_000;
    let frac_ms = u32::try_from(lag_ms % 1_000).unwrap_or(u32::MAX);
    f64::from(u32::try_from(whole_secs).unwrap_or(u32::MAX)) + f64::from(frac_ms) / 1_000.0
}

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
    pub ws_session_backpressure_invalidations: IntCounter,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub ws_shard_connected: IntGaugeVec,
    pub book_store_token_count: IntGauge,

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
    pub ingest_pipeline_lag_worst_ms: IntGauge,
    pub ingest_pipeline_lag_seconds: HistogramVec,

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

    // ── Durable report coordinator (11.8) ─────────────────────────────────
    pub report_run_total: IntCounterVec,
    pub report_run_duration_seconds: HistogramVec,
    pub report_run_queue_latency_seconds: HistogramVec,
    pub report_schedule_gap_total: IntCounterVec,
    pub report_schedule_lateness_seconds: HistogramVec,
    pub report_run_active: IntGauge,
    pub report_run_queued: IntGauge,
    pub report_prepared_backlog: IntGauge,
    pub report_current_age_seconds: IntGaugeVec,
    pub report_expire_swept_total: IntCounter,
    pub report_fact_settlement_claim_lost_total: IntCounterVec,
    pub report_fact_worker_errors_total: IntCounterVec,

    // ── Training/serving feature parity (11.6) ────────────────────────
    /// Runs by controlled kind/status labels.
    pub feature_parity_runs_total: IntCounterVec,
    /// Stage comparisons by controlled stage/status/reason labels.
    pub feature_parity_comparisons_total: IntCounterVec,
    /// `1` while deterministic parity blocks risk-increasing actions.
    pub feature_parity_latch_open: IntGauge,
    /// Containment attempts by terminal outcome (`completed`/`failed`).
    pub feature_parity_containment_total: IntCounterVec,

    // ── Execution governance (05.1) ───────────────────────────────────────
    /// `1` when the operational kill-switch blocks new auto entries (any
    /// non-`closed` state), `0` when `closed`.
    pub auto_execution_halted: IntGauge,

    // ── Execution admission (05.3) ────────────────────────────────────────
    /// Admission denials by the check id that determined the `Deny` outcome.
    pub admission_denied: IntCounterVec,
    /// Order intents created by runtime mode and intent kind.
    pub order_intents_created: IntCounterVec,
    /// Order intents approved by runtime mode and intent kind.
    pub order_intents_approved: IntCounterVec,
    /// Order intents rejected by runtime mode and intent kind.
    pub order_intents_rejected: IntCounterVec,

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
    /// Exit-monitor triggers by exit reason (05.6).
    pub exit_triggers: IntCounterVec,
    /// Exit signal re-inference outcomes (06.0).
    pub exit_signal_reinference: IntCounterVec,
    /// Opportunistic-Sell scorer evaluation outcomes (06.1).
    pub opportunistic_sell_eval: IntCounterVec,
}

struct PipelineMetrics {
    ws_events_received: IntCounter,
    book_snapshots_applied: IntCounter,
    price_changes_applied: IntCounter,
    ws_session_backpressure_invalidations: IntCounter,
    markets_resolved_ws: IntCounter,
    shard_status_changes: IntCounter,
    ws_shard_connected: IntGaugeVec,
    book_store_token_count: IntGauge,
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
    run_total: IntCounterVec,
    run_duration: HistogramVec,
    run_queue_latency: HistogramVec,
    schedule_gap: IntCounterVec,
    schedule_lateness: HistogramVec,
    run_active: IntGauge,
    run_queued: IntGauge,
    prepared_backlog: IntGauge,
    current_age: IntGaugeVec,
    expire_swept: IntCounter,
    fact_settlement_claim_lost: IntCounterVec,
    fact_worker_errors: IntCounterVec,
}

struct FeatureParityMetrics {
    runs: IntCounterVec,
    comparisons: IntCounterVec,
    latch_open: IntGauge,
    containment: IntCounterVec,
}

/// Execution / risk / governance counters (Phase 05.1–05.5).
struct ExecutionMetrics {
    auto_execution_halted: IntGauge,
    admission_denied: IntCounterVec,
    order_intents_created: IntCounterVec,
    order_intents_approved: IntCounterVec,
    order_intents_rejected: IntCounterVec,
    execution_orders_submitted: IntCounter,
    execution_fills: IntCounter,
    execution_breaker_trips: IntCounterVec,
    reconciliation_unresolvable: IntCounter,
    exit_triggers: IntCounterVec,
    exit_signal_reinference: IntCounterVec,
    opportunistic_sell_eval: IntCounterVec,
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
        ws_session_backpressure_invalidations: register_counter!(
            registry,
            "quant_pivot_ws_session_backpressure_invalidations_total",
            "WebSocket sessions invalidated after bounded output enqueue timeout"
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
        run_total: register_counter_vec!(
            registry,
            "quant_pivot_report_run_total",
            "Durable report-run transitions by trigger, status, and terminal reason",
            &["trigger_kind", "status", "reason"]
        ),
        run_duration: register_histogram_vec!(
            registry,
            "quant_pivot_report_run_duration_seconds",
            "Durable report build duration by trigger and terminal status",
            &["trigger_kind", "status"],
            REPORT_RUN_BUCKETS_SECS
        ),
        run_queue_latency: register_histogram_vec!(
            registry,
            "quant_pivot_report_run_queue_latency_seconds",
            "Durable report run queue latency by trigger kind",
            &["trigger_kind"],
            REPORT_RUN_BUCKETS_SECS
        ),
        schedule_gap: register_counter_vec!(
            registry,
            "quant_pivot_report_schedule_gap_total",
            "Durable missed report-schedule occurrences by schedule and reason",
            &["schedule_id", "reason"]
        ),
        schedule_lateness: register_histogram_vec!(
            registry,
            "quant_pivot_report_schedule_lateness_seconds",
            "Actual database decision time minus scheduled occurrence",
            &["schedule_id"],
            REPORT_RUN_BUCKETS_SECS
        ),
        run_active: register_gauge_int!(
            registry,
            "quant_pivot_report_run_active",
            "Whether a durable report build is currently running"
        ),
        run_queued: register_gauge_int!(
            registry,
            "quant_pivot_report_run_queued",
            "Number of durable report runs currently queued"
        ),
        prepared_backlog: register_gauge_int!(
            registry,
            "quant_pivot_report_prepared_backlog",
            "Number of immutable Prepared reports awaiting verified publication"
        ),
        current_age: register_gauge_vec!(
            registry,
            "quant_pivot_report_current_age_seconds",
            "Age of each scope's current Published report",
            &["profile_id", "report_kind"]
        ),
        expire_swept: register_counter!(
            registry,
            "quant_pivot_report_expire_swept_total",
            "Reports transitioned to expired by the TTL sweep"
        ),
        fact_settlement_claim_lost: register_counter_vec!(
            registry,
            "quant_pivot_report_fact_settlement_claim_lost_total",
            "Report fact settlement CAS losses by operation and durable status",
            &["operation", "status"]
        ),
        fact_worker_errors: register_counter_vec!(
            registry,
            "quant_pivot_report_fact_worker_errors_total",
            "Report fact worker process errors by bounded stage",
            &["stage"]
        ),
    }
}

fn register_feature_parity_metrics(registry: &Registry) -> FeatureParityMetrics {
    FeatureParityMetrics {
        runs: register_counter_vec!(
            registry,
            "quant_feature_parity_runs_total",
            "Deterministic parity runs by kind and terminal/pending status",
            &["kind", "status"]
        ),
        comparisons: register_counter_vec!(
            registry,
            "quant_feature_parity_comparisons_total",
            "Deterministic parity evidence comparisons by stage, status and controlled reason",
            &["stage", "status", "reason"]
        ),
        latch_open: register_gauge_int!(
            registry,
            "quant_feature_parity_latch_open",
            "1 when deterministic feature parity blocks new risk"
        ),
        containment: register_counter_vec!(
            registry,
            "quant_feature_parity_containment_total",
            "Feature parity report/intent containment attempts by outcome",
            &["outcome"]
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
        order_intents_created: register_counter_vec!(
            registry,
            "quant_order_intents_created_total",
            "Order intents created by runtime mode and intent kind",
            &["mode", "kind"]
        ),
        order_intents_approved: register_counter_vec!(
            registry,
            "quant_order_intents_approved_total",
            "Order intents approved by runtime mode and intent kind",
            &["mode", "kind"]
        ),
        order_intents_rejected: register_counter_vec!(
            registry,
            "quant_order_intents_rejected_total",
            "Order intents rejected by runtime mode and intent kind",
            &["mode", "kind"]
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
        exit_triggers: register_counter_vec!(
            registry,
            "quant_exit_triggers_total",
            "Exit-monitor triggers by exit reason",
            &["reason"]
        ),
        exit_signal_reinference: register_counter_vec!(
            registry,
            "quant_exit_signal_reinference_total",
            "Exit signal re-inference outcomes",
            &["outcome"]
        ),
        opportunistic_sell_eval: register_counter_vec!(
            registry,
            "quant_opportunistic_sell_eval_total",
            "Opportunistic-Sell scorer evaluation outcomes",
            &["outcome"]
        ),
    }
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = Registry::new();
        let pipeline = register_pipeline_metrics(&registry);
        let gamma = register_gamma_metrics(&registry);
        let subscription = register_subscription_metrics(&registry);
        let infra = register_infra_metrics(&registry);
        let report = register_report_metrics(&registry);
        let feature_parity = register_feature_parity_metrics(&registry);
        let data_quality_tokens = register_gauge_vec!(
            &registry,
            "quant_pivot_data_quality_tokens",
            "Live book tokens by data-quality status",
            &["status"]
        );
        let ingest_pipeline_lag_worst_ms = register_gauge_int!(
            &registry,
            "quant_pivot_ingest_pipeline_lag_worst_ms",
            "Peak ingest pipeline lag (enqueue→flush-ack) in the last window (milliseconds)"
        );
        let ingest_pipeline_lag_seconds = register_histogram_vec!(
            &registry,
            "quant_pivot_ingest_pipeline_lag_seconds",
            "Ingest pipeline lag (enqueue→flush-ack) by writer",
            &["writer"],
            FACT_LAG_BUCKETS_SECS
        );
        let execution = register_execution_metrics(&registry);

        Self {
            registry,
            ws_events_received: pipeline.ws_events_received,
            book_snapshots_applied: pipeline.book_snapshots_applied,
            price_changes_applied: pipeline.price_changes_applied,
            ws_session_backpressure_invalidations: pipeline.ws_session_backpressure_invalidations,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            ws_shard_connected: pipeline.ws_shard_connected,
            book_store_token_count: pipeline.book_store_token_count,
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
            ingest_pipeline_lag_worst_ms,
            ingest_pipeline_lag_seconds,
            async_writer_dropped: infra.async_writer_dropped,
            async_writer_queue_depth: infra.async_writer_queue_depth,
            async_writer_flush_failed: infra.async_writer_flush_failed,
            shutdown_stage_progress_remaining: infra.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: infra.shutdown_stage_timeouts,
            report_generated_total: report.generated,
            report_recommendations_total: report.recommendations,
            report_publish_failures_total: report.publish_failures,
            report_run_total: report.run_total,
            report_run_duration_seconds: report.run_duration,
            report_run_queue_latency_seconds: report.run_queue_latency,
            report_schedule_gap_total: report.schedule_gap,
            report_schedule_lateness_seconds: report.schedule_lateness,
            report_run_active: report.run_active,
            report_run_queued: report.run_queued,
            report_prepared_backlog: report.prepared_backlog,
            report_current_age_seconds: report.current_age,
            report_expire_swept_total: report.expire_swept,
            report_fact_settlement_claim_lost_total: report.fact_settlement_claim_lost,
            report_fact_worker_errors_total: report.fact_worker_errors,
            feature_parity_runs_total: feature_parity.runs,
            feature_parity_comparisons_total: feature_parity.comparisons,
            feature_parity_latch_open: feature_parity.latch_open,
            feature_parity_containment_total: feature_parity.containment,
            auto_execution_halted: execution.auto_execution_halted,
            admission_denied: execution.admission_denied,
            order_intents_created: execution.order_intents_created,
            order_intents_approved: execution.order_intents_approved,
            order_intents_rejected: execution.order_intents_rejected,
            execution_orders_submitted: execution.execution_orders_submitted,
            execution_fills: execution.execution_fills,
            execution_breaker_trips: execution.execution_breaker_trips,
            reconciliation_unresolvable: execution.reconciliation_unresolvable,
            exit_triggers: execution.exit_triggers,
            exit_signal_reinference: execution.exit_signal_reinference,
            opportunistic_sell_eval: execution.opportunistic_sell_eval,
        }
    }

    /// Count one entry order submitted to the venue.
    pub fn inc_execution_order_submitted(&self) {
        self.execution_orders_submitted.inc();
    }

    pub fn inc_order_intent_created(&self, mode: &str, kind: &str) {
        self.order_intents_created
            .with_label_values(&[mode, kind])
            .inc();
    }

    pub fn inc_order_intent_approved(&self, mode: &str, kind: &str) {
        self.order_intents_approved
            .with_label_values(&[mode, kind])
            .inc();
    }

    pub fn inc_order_intent_rejected(&self, mode: &str, kind: &str) {
        self.order_intents_rejected
            .with_label_values(&[mode, kind])
            .inc();
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

    /// Count one exit-monitor trigger for an exit reason (05.6).
    pub fn inc_exit_trigger(&self, reason: &str) {
        self.exit_triggers.with_label_values(&[reason]).inc();
    }

    /// Count one exit signal re-inference outcome (`fresh`, `unavailable`, `error`,
    /// `disabled`, `shadow_would_invalidate`, `shadow_hold`).
    pub fn inc_exit_signal_reinference(&self, outcome: &str) {
        self.exit_signal_reinference
            .with_label_values(&[outcome])
            .inc();
    }

    /// Count one opportunistic-Sell scorer evaluation outcome (`disabled`,
    /// `skipped_non_auto`, `unavailable`, `error`, `hold`, `shadow_would_sell`,
    /// `opportunistic_sell`).
    pub fn inc_opportunistic_sell_eval(&self, outcome: &str) {
        self.opportunistic_sell_eval
            .with_label_values(&[outcome])
            .inc();
    }

    /// Publish whether the kill-switch currently blocks new auto entries.
    pub fn set_auto_execution_halted(&self, halted: bool) {
        self.auto_execution_halted.set(i64::from(halted));
    }

    /// Observe one ingest-pipeline-lag sample (enqueue→flush-ack) for a writer.
    pub fn observe_ingest_pipeline_lag_ms(&self, writer: &str, lag_ms: u64) {
        self.ingest_pipeline_lag_seconds
            .with_label_values(&[writer])
            .observe(lag_secs_from_ms(lag_ms));
    }

    /// Publish the peak ingest pipeline lag for the elapsed observation window.
    pub fn set_ingest_pipeline_lag_worst_ms(&self, lag_ms: u64) {
        self.ingest_pipeline_lag_worst_ms
            .set(i64::try_from(lag_ms).unwrap_or(i64::MAX));
    }

    /// Build observability handles for one named async writer.
    ///
    /// Every writer reports its enqueue→flush-ack latency into the per-writer
    /// `ingest_pipeline_lag_seconds` histogram (complete backpressure telemetry).
    /// Feeding the plane-level [`IngestPipelineLagTracker`] that gates book-plane
    /// data quality is done separately, and only for the book-fact streams, so a
    /// slow output sink never poisons the live book-quality gate.
    #[must_use]
    pub fn async_writer_observability(&self, writer: &'static str) -> AsyncWriterObservability {
        let lag_histogram = self
            .ingest_pipeline_lag_seconds
            .with_label_values(&[writer]);
        AsyncWriterObservability {
            queue_depth: Some(self.async_writer_queue_depth.with_label_values(&[writer])),
            flush_failed: Some(self.async_writer_flush_failed.with_label_values(&[writer])),
            flush_lag_ms: Some(Arc::new(move |lag_ms| {
                lag_histogram.observe(lag_secs_from_ms(lag_ms));
            })),
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

    /// Count reports expired by the TTL sweep in one pass.
    pub fn inc_report_expire_swept(&self, swept: u64) {
        self.report_expire_swept_total.inc_by(swept);
    }

    pub fn inc_report_fact_settlement_claim_lost(&self, operation: &str, status: &str) {
        self.report_fact_settlement_claim_lost_total
            .with_label_values(&[operation, status])
            .inc();
    }

    pub fn inc_report_fact_worker_error(&self, stage: &str) {
        self.report_fact_worker_errors_total
            .with_label_values(&[stage])
            .inc();
    }

    pub fn record_feature_parity_run(&self, kind: &str, status: &str) {
        self.feature_parity_runs_total
            .with_label_values(&[kind, status])
            .inc();
    }

    pub fn record_feature_parity_comparison(&self, stage: &str, status: &str, reason: &str) {
        self.feature_parity_comparisons_total
            .with_label_values(&[stage, status, reason])
            .inc();
    }

    pub fn set_feature_parity_latch_open(&self, open: bool) {
        self.feature_parity_latch_open.set(i64::from(open));
    }

    pub fn record_feature_parity_containment(&self, outcome: &str) {
        self.feature_parity_containment_total
            .with_label_values(&[outcome])
            .inc();
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

#[cfg(test)]
mod tests {
    use super::MetricsHub;

    #[test]
    fn order_intent_created_and_approved_counters_increase() {
        let hub = MetricsHub::new();
        hub.inc_order_intent_created("auto_execution", "buy");
        hub.inc_order_intent_approved("auto_execution", "buy");
        let (_, text) = hub.gather_prometheus_text().expect("gather");
        let body = String::from_utf8(text).expect("utf8");
        assert!(body.contains("quant_order_intents_created_total"));
        assert!(body.contains("quant_order_intents_approved_total"));
        assert!(body.contains(r#"mode="auto_execution""#));
    }

    #[test]
    fn durable_report_metrics_expose_frozen_observability_contract() {
        let hub = MetricsHub::new();
        hub.report_run_total
            .with_label_values(&["ad_hoc", "failed", "build_failed"])
            .inc();
        hub.report_run_duration_seconds
            .with_label_values(&["ad_hoc", "failed"])
            .observe(2.0);
        hub.report_run_queue_latency_seconds
            .with_label_values(&["ad_hoc"])
            .observe(0.5);
        hub.report_schedule_gap_total
            .with_label_values(&["hourly", "coordinator_lag"])
            .inc();
        hub.report_prepared_backlog.set(2);
        hub.report_current_age_seconds
            .with_label_values(&["weather_forecast_24h", "top_n"])
            .set(60);
        hub.inc_report_fact_settlement_claim_lost("verify", "cancelled");
        hub.inc_report_fact_worker_error("process_one");

        let (_, text) = hub.gather_prometheus_text().expect("gather");
        let body = String::from_utf8(text).expect("utf8");
        for name in [
            "quant_pivot_report_run_total",
            "quant_pivot_report_run_duration_seconds",
            "quant_pivot_report_run_queue_latency_seconds",
            "quant_pivot_report_schedule_gap_total",
            "quant_pivot_report_prepared_backlog",
            "quant_pivot_report_current_age_seconds",
            "quant_pivot_report_fact_settlement_claim_lost_total",
            "quant_pivot_report_fact_worker_errors_total",
        ] {
            assert!(body.contains(name), "missing metric {name}");
        }
        assert!(body.contains(r#"reason="build_failed""#));
        assert!(body.contains(r#"profile_id="weather_forecast_24h""#));
    }
}
