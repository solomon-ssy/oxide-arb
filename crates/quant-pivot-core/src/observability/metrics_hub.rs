//! Operational metrics for the quant-pivot runtime.
//!
//! Covers catalog sync, subscription ingest, fact writers, research, reporting,
//! execution, governance, and orderly shutdown.

use std::sync::Arc;

use prometheus::{
    Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
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

macro_rules! register_gauge_float_vec {
    ($registry:expr, $name:expr, $help:expr, $labels:expr) => {{
        let gauge_vec = GaugeVec::new(Opts::new($name, $help), $labels).unwrap();
        $registry.register(Box::new(gauge_vec.clone())).unwrap();
        gauge_vec
    }};
}

macro_rules! register_histogram {
    ($registry:expr, $name:expr, $help:expr, $buckets:expr) => {{
        let histogram =
            Histogram::with_opts(HistogramOpts::new($name, $help).buckets($buckets.to_vec()))
                .unwrap();
        $registry.register(Box::new(histogram.clone())).unwrap();
        histogram
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

/// Settlement lifecycle buckets in seconds (1 s … 24 h).
const SETTLEMENT_LAG_BUCKETS_SECS: &[f64] = &[
    1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3_600.0, 21_600.0, 86_400.0,
];

/// Feedback lifecycle buckets in seconds (10 ms … 24 h).
const FEEDBACK_RUN_BUCKETS_SECS: &[f64] = &[
    0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 60.0, 300.0, 900.0, 3_600.0, 21_600.0, 86_400.0,
];

/// Central Prometheus registry for live paths only.
pub struct MetricsHub {
    pub registry: Registry,

    // ── Data pipeline (CLOB WS → BookStore) ─────────────────────────────
    pub ws_events_received: IntCounter,
    pub book_snapshots_applied: IntCounter,
    pub price_changes_applied: IntCounter,
    pub ws_session_invalidated_tokens: IntCounter,
    pub ws_fanout_best_effort_dropped: IntCounter,
    pub ws_fanout_best_effort_coalesced: IntCounter,
    pub ws_fanout_reliable_disconnects: IntCounter,
    pub ws_hub_control_timeouts: IntCounter,
    pub ws_hub_control_latency_seconds: Histogram,
    pub ws_hub_queue_depth: IntGaugeVec,
    pub ws_hub_queue_oldest_age_seconds: GaugeVec,
    pub ws_hub_frame_bytes: IntGauge,
    pub book_apply_backpressure_invalidations: IntCounter,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub ws_shard_connected: IntGaugeVec,
    pub book_store_token_count: IntGauge,
    pub mutable_book_count: IntGauge,

    // ── Gamma catalog sync ──────────────────────────────────────────────
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,
    pub gamma_markets_filtered: IntCounterVec,
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

    // ── Durable report coordinator ─────────────────────────────────
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

    // ── Training/serving feature parity ────────────────────────
    /// Runs by controlled kind/status labels.
    pub feature_parity_runs_total: IntCounterVec,
    /// Stage comparisons by controlled stage/status/reason labels.
    pub feature_parity_comparisons_total: IntCounterVec,
    /// `1` while deterministic parity blocks risk-increasing actions.
    pub feature_parity_latch_open: IntGauge,
    /// Containment attempts by terminal outcome (`completed`/`failed`).
    pub feature_parity_containment_total: IntCounterVec,

    // ── Research feedback lifecycle ───────────────────────────────────
    /// Terminal feedback cycles by controlled status/decision labels.
    pub feedback_cycle_total: IntCounterVec,
    /// End-to-end feedback-cycle duration by controlled status.
    pub feedback_cycle_duration_seconds: HistogramVec,
    /// Research-job duration observed at each feedback stage.
    pub feedback_stage_duration_seconds: HistogramVec,
    /// Feedback cycles currently owned by this process.
    pub feedback_cycle_active: IntGauge,
    /// Queued feedback cycles in authoritative `PostgreSQL` state.
    pub feedback_cycle_queued: IntGauge,
    /// Unpublished feedback control-plane revisions.
    pub feedback_outbox_pending: IntGauge,
    /// Running cycles that crossed the configured DB-clock age threshold.
    pub feedback_stuck_total: IntCounter,
    /// Age of the oldest unresolved inbox observation.
    pub feedback_resolution_inbox_age_seconds: IntGauge,
    /// Resolution observations currently quarantined.
    pub feedback_resolution_quarantined: IntGauge,
    /// DB-clock lag of the canonical resolution projection frontier.
    pub feedback_resolution_projector_lag_seconds: IntGauge,
    /// DB-clock lag of the immutable execution-attempt frontier.
    pub feedback_attempt_projector_lag_seconds: IntGauge,
    /// DB-clock lag of the sealed recommendation-rollup frontier.
    pub feedback_rollup_lag_seconds: IntGauge,
    /// Scheduler profiles whose effective durable due time has elapsed.
    pub feedback_scheduler_overdue_profiles: IntGauge,
    /// Maximum DB-clock lateness among overdue scheduler profiles.
    pub feedback_scheduler_max_overdue_seconds: IntGauge,
    /// Explanation efficiency failures by bounded method.
    pub feedback_attribution_efficiency_failures_total: IntCounterVec,
    /// Validation quality-gate outcomes by bounded gate and status.
    pub feedback_quality_gate_total: IntCounterVec,
    /// Governed activation attempts rejected because the permit expired.
    pub feedback_permit_expiry_total: IntCounter,
    /// Model-route governance conflicts by bounded action and authority layer.
    pub feedback_route_governance_conflict_total: IntCounterVec,
    /// Durable feedback WebSocket retries and recoveries.
    pub feedback_ws_recovery_total: IntCounterVec,
    /// Advisory progress values superseded in the single latest-value slot.
    pub research_progress_coalesced_total: IntCounter,
    /// Research-job lease heartbeat failures by controlled operation/result.
    pub research_heartbeat_total: IntCounterVec,

    // ── Execution governance ───────────────────────────────────────
    /// `1` when the operational kill-switch blocks new auto entries (any
    /// non-`closed` state), `0` when `closed`.
    pub auto_execution_halted: IntGauge,

    // ── Execution admission ────────────────────────────────────────
    /// Admission denials by the check id that determined the `Deny` outcome.
    pub admission_denied: IntCounterVec,
    /// Order intents created by runtime mode and intent kind.
    pub order_intents_created: IntCounterVec,
    /// Order intents approved by runtime mode and intent kind.
    pub order_intents_approved: IntCounterVec,
    /// Order intents rejected by runtime mode and intent kind.
    pub order_intents_rejected: IntCounterVec,

    // ── Entry execution ────────────────────────────────────────────
    /// Entry orders successfully submitted to the venue (write-ahead committed).
    pub execution_orders_submitted: IntCounter,
    /// Venue fills observed on submission (full or partial).
    pub execution_fills: IntCounter,
    /// Execution-breaker kill-switch trips by triggering dimension.
    pub execution_breaker_trips: IntCounterVec,

    // ── Reconciliation ─────────────────────────────────────────────
    /// Reconciliations that resolved to a terminal `Unresolvable` verdict
    /// (capital impaired, kill-switch latched until an operator resolves).
    pub reconciliation_unresolvable: IntCounter,
    /// Exit-monitor triggers by exit reason.
    pub exit_triggers: IntCounterVec,
    /// Exit signal re-inference outcomes.
    pub exit_signal_reinference: IntCounterVec,
    /// Opportunistic-Sell scorer evaluation outcomes.
    pub opportunistic_sell_eval: IntCounterVec,
    /// Settlement worker passes by bounded worker and typed outcome.
    pub settlement_worker_pass_total: IntCounterVec,
    /// Settlement worker failures by bounded worker.
    pub settlement_worker_error_total: IntCounterVec,
    /// Settlement leases lost while freezing a signed envelope.
    pub settlement_lease_lost_total: IntCounterVec,
    /// Durable reconciliation-required transitions by workflow and failure code.
    pub settlement_reconciliation_required_total: IntCounterVec,
    /// Resolution-to-case discovery lag.
    pub settlement_discovery_lag_seconds: Histogram,
    /// Age of durable submissions observed by recovery.
    pub settlement_submission_age_seconds: HistogramVec,
    /// Time spent awaiting canonical finality.
    pub settlement_finality_lag_seconds: HistogramVec,
}

struct PipelineMetrics {
    ws_events_received: IntCounter,
    book_snapshots_applied: IntCounter,
    price_changes_applied: IntCounter,
    ws_session_invalidated_tokens: IntCounter,
    ws_fanout_best_effort_dropped: IntCounter,
    ws_fanout_best_effort_coalesced: IntCounter,
    ws_fanout_reliable_disconnects: IntCounter,
    ws_hub_control_timeouts: IntCounter,
    ws_hub_control_latency_seconds: Histogram,
    ws_hub_queue_depth: IntGaugeVec,
    ws_hub_queue_oldest_age_seconds: GaugeVec,
    ws_hub_frame_bytes: IntGauge,
    book_apply_backpressure_invalidations: IntCounter,
    markets_resolved_ws: IntCounter,
    shard_status_changes: IntCounter,
    ws_shard_connected: IntGaugeVec,
    book_store_token_count: IntGauge,
    mutable_book_count: IntGauge,
}

struct GammaMetrics {
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    gamma_markets_filtered: IntCounterVec,
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

struct ResearchFeedbackMetrics {
    cycle_total: IntCounterVec,
    cycle_duration: HistogramVec,
    stage_duration: HistogramVec,
    cycle_active: IntGauge,
    cycle_queued: IntGauge,
    outbox_pending: IntGauge,
    stuck_total: IntCounter,
    resolution_inbox_age: IntGauge,
    resolution_quarantined: IntGauge,
    resolution_projector_lag: IntGauge,
    attempt_projector_lag: IntGauge,
    rollup_lag: IntGauge,
    scheduler_overdue_profiles: IntGauge,
    scheduler_max_overdue: IntGauge,
    attribution_efficiency_failures: IntCounterVec,
    quality_gate_total: IntCounterVec,
    permit_expiry_total: IntCounter,
    route_governance_conflict_total: IntCounterVec,
    ws_recovery_total: IntCounterVec,
    progress_coalesced: IntCounter,
    heartbeat_total: IntCounterVec,
}

/// Execution, risk, and governance counters.
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
    settlement_worker_pass_total: IntCounterVec,
    settlement_worker_error_total: IntCounterVec,
    settlement_lease_lost_total: IntCounterVec,
    settlement_reconciliation_required_total: IntCounterVec,
    settlement_discovery_lag_seconds: Histogram,
    settlement_submission_age_seconds: HistogramVec,
    settlement_finality_lag_seconds: HistogramVec,
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
        ws_session_invalidated_tokens: register_counter!(
            registry,
            "quant_pivot_ws_session_invalidated_tokens_total",
            "Tokens invalidated immediately when a WebSocket session loses continuity"
        ),
        ws_fanout_best_effort_dropped: register_counter!(
            registry,
            "quant_pivot_ws_fanout_best_effort_dropped_total",
            "Best-effort WebSocket frames dropped for full client queues"
        ),
        ws_fanout_best_effort_coalesced: register_counter!(
            registry,
            "quant_pivot_ws_fanout_best_effort_coalesced_total",
            "Best-effort WebSocket frames superseded in the latest-value topic coalescer"
        ),
        ws_fanout_reliable_disconnects: register_counter!(
            registry,
            "quant_pivot_ws_fanout_reliable_disconnects_total",
            "Slow WebSocket clients disconnected before a reliable frame could be queued"
        ),
        ws_hub_control_timeouts: register_counter!(
            registry,
            "quant_pivot_ws_hub_control_timeouts_total",
            "SessionHub control commands that exceeded the fail-closed deadline"
        ),
        ws_hub_control_latency_seconds: register_histogram!(
            registry,
            "quant_pivot_ws_hub_control_latency_seconds",
            "SessionHub control enqueue-to-ack latency",
            &[
                0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1
            ]
        ),
        ws_hub_queue_depth: register_gauge_vec!(
            registry,
            "quant_pivot_ws_hub_queue_depth",
            "Pending SessionHub work by lane",
            &["lane"]
        ),
        ws_hub_queue_oldest_age_seconds: register_gauge_float_vec!(
            registry,
            "quant_pivot_ws_hub_queue_oldest_age_seconds",
            "Oldest observed SessionHub pending age by lane",
            &["lane"]
        ),
        ws_hub_frame_bytes: register_gauge_int!(
            registry,
            "quant_pivot_ws_hub_frame_bytes",
            "Bytes retained by unique shared WebSocket frames"
        ),
        book_apply_backpressure_invalidations: register_counter!(
            registry,
            "quant_pivot_book_apply_backpressure_invalidations_total",
            "Tokens invalidated after bounded book-apply queue timeout"
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
        mutable_book_count: register_gauge_int!(
            registry,
            "quant_pivot_pipeline_mutable_book_count",
            "Actor-owned mutable order books currently resident"
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
        gamma_markets_filtered: register_counter_vec!(
            registry,
            "quant_pivot_gamma_markets_filtered_total",
            "Legitimate Gamma pre-listing objects excluded from the canonical market projection",
            &["reason"]
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
            "Age of the global current Published report",
            &["scope", "report_kind"]
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

fn register_research_feedback_metrics(registry: &Registry) -> ResearchFeedbackMetrics {
    ResearchFeedbackMetrics {
        cycle_total: register_counter_vec!(
            registry,
            "quant_feedback_cycle_total",
            "Terminal feedback cycles by status and decision",
            &["status", "decision"]
        ),
        cycle_duration: register_histogram_vec!(
            registry,
            "quant_feedback_cycle_duration_seconds",
            "End-to-end feedback-cycle duration by terminal status",
            &["status"],
            FEEDBACK_RUN_BUCKETS_SECS
        ),
        stage_duration: register_histogram_vec!(
            registry,
            "quant_feedback_stage_duration_seconds",
            "Research-job duration by feedback stage and terminal status",
            &["stage", "status"],
            FEEDBACK_RUN_BUCKETS_SECS
        ),
        cycle_active: register_gauge_int!(
            registry,
            "quant_feedback_cycle_active",
            "Feedback cycles currently owned by this process"
        ),
        cycle_queued: register_gauge_int!(
            registry,
            "quant_feedback_cycle_queued",
            "Queued feedback cycles in authoritative PostgreSQL state"
        ),
        outbox_pending: register_gauge_int!(
            registry,
            "quant_feedback_outbox_pending",
            "Unpublished feedback control-plane revisions"
        ),
        stuck_total: register_counter!(
            registry,
            "quant_feedback_stuck_total",
            "Running feedback cycles that crossed the configured DB-clock age threshold"
        ),
        resolution_inbox_age: register_gauge_int!(
            registry,
            "quant_feedback_resolution_inbox_oldest_age_seconds",
            "Age of the oldest unresolved resolution inbox observation on the PostgreSQL clock"
        ),
        resolution_quarantined: register_gauge_int!(
            registry,
            "quant_feedback_resolution_quarantined",
            "Resolution inbox observations currently quarantined"
        ),
        resolution_projector_lag: register_gauge_int!(
            registry,
            "quant_feedback_resolution_projector_lag_seconds",
            "PostgreSQL-clock lag of the canonical resolution projection frontier"
        ),
        attempt_projector_lag: register_gauge_int!(
            registry,
            "quant_feedback_execution_attempt_projector_lag_seconds",
            "PostgreSQL-clock lag of the immutable execution-attempt outcome frontier"
        ),
        rollup_lag: register_gauge_int!(
            registry,
            "quant_feedback_recommendation_rollup_lag_seconds",
            "PostgreSQL-clock lag of the sealed recommendation execution-rollup frontier"
        ),
        scheduler_overdue_profiles: register_gauge_int!(
            registry,
            "quant_feedback_scheduler_overdue_profiles",
            "Unpaused feedback scheduler profiles whose effective durable due time elapsed"
        ),
        scheduler_max_overdue: register_gauge_int!(
            registry,
            "quant_feedback_scheduler_max_overdue_seconds",
            "Maximum PostgreSQL-clock lateness among overdue feedback scheduler profiles"
        ),
        attribution_efficiency_failures: register_counter_vec!(
            registry,
            "quant_feedback_attribution_efficiency_failures_total",
            "Prediction-explanation efficiency failures by bounded explanation method",
            &["method"]
        ),
        quality_gate_total: register_counter_vec!(
            registry,
            "quant_feedback_quality_gate_total",
            "Feedback Validation quality-gate outcomes by bounded gate and status",
            &["gate", "status"]
        ),
        permit_expiry_total: register_counter!(
            registry,
            "quant_feedback_promotion_permit_expiry_total",
            "Model-route activation attempts rejected because their permit expired"
        ),
        route_governance_conflict_total: register_counter_vec!(
            registry,
            "quant_feedback_route_governance_conflict_total",
            "Governed model-route conflicts by bounded action and authority layer",
            &["action", "layer"]
        ),
        ws_recovery_total: register_counter_vec!(
            registry,
            "quant_feedback_ws_recovery_total",
            "Durable research.feedback WebSocket outbox retry and recovery outcomes",
            &["outcome"]
        ),
        progress_coalesced: register_counter!(
            registry,
            "quant_research_progress_coalesced_total",
            "Research progress values superseded in the single latest-value slot"
        ),
        heartbeat_total: register_counter_vec!(
            registry,
            "quant_research_heartbeat_total",
            "Research-job lease heartbeat results by operation and result",
            &["operation", "result"]
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
        settlement_worker_pass_total: register_counter_vec!(
            registry,
            "quant_settlement_worker_pass_total",
            "Settlement worker passes by worker and typed outcome",
            &["worker", "outcome"]
        ),
        settlement_worker_error_total: register_counter_vec!(
            registry,
            "quant_settlement_worker_error_total",
            "Settlement worker pass failures by worker",
            &["worker"]
        ),
        settlement_lease_lost_total: register_counter_vec!(
            registry,
            "quant_settlement_lease_lost_total",
            "Settlement leases lost while freezing a durable signed envelope",
            &["workflow"]
        ),
        settlement_reconciliation_required_total: register_counter_vec!(
            registry,
            "quant_settlement_reconciliation_required_total",
            "Durable settlement reconciliation-required transitions",
            &["workflow", "failure_code"]
        ),
        settlement_discovery_lag_seconds: register_histogram!(
            registry,
            "quant_settlement_discovery_lag_seconds",
            "Resolution-to-settlement-case discovery lag",
            SETTLEMENT_LAG_BUCKETS_SECS
        ),
        settlement_submission_age_seconds: register_histogram_vec!(
            registry,
            "quant_settlement_submission_age_seconds",
            "Age of durable settlement submissions observed by recovery",
            &["workflow"],
            SETTLEMENT_LAG_BUCKETS_SECS
        ),
        settlement_finality_lag_seconds: register_histogram_vec!(
            registry,
            "quant_settlement_finality_lag_seconds",
            "Time settlement submissions spend awaiting canonical finality",
            &["workflow"],
            SETTLEMENT_LAG_BUCKETS_SECS
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
        let research_feedback = register_research_feedback_metrics(&registry);
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
            ws_session_invalidated_tokens: pipeline.ws_session_invalidated_tokens,
            ws_fanout_best_effort_dropped: pipeline.ws_fanout_best_effort_dropped,
            ws_fanout_best_effort_coalesced: pipeline.ws_fanout_best_effort_coalesced,
            ws_fanout_reliable_disconnects: pipeline.ws_fanout_reliable_disconnects,
            ws_hub_control_timeouts: pipeline.ws_hub_control_timeouts,
            ws_hub_control_latency_seconds: pipeline.ws_hub_control_latency_seconds,
            ws_hub_queue_depth: pipeline.ws_hub_queue_depth,
            ws_hub_queue_oldest_age_seconds: pipeline.ws_hub_queue_oldest_age_seconds,
            ws_hub_frame_bytes: pipeline.ws_hub_frame_bytes,
            book_apply_backpressure_invalidations: pipeline.book_apply_backpressure_invalidations,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            ws_shard_connected: pipeline.ws_shard_connected,
            book_store_token_count: pipeline.book_store_token_count,
            mutable_book_count: pipeline.mutable_book_count,
            gamma_sync_duration_ms: gamma.gamma_sync_duration_ms,
            gamma_markets_total: gamma.gamma_markets_total,
            gamma_last_sync_success: gamma.gamma_last_sync_success,
            gamma_markets_filtered: gamma.gamma_markets_filtered,
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
            feedback_cycle_total: research_feedback.cycle_total,
            feedback_cycle_duration_seconds: research_feedback.cycle_duration,
            feedback_stage_duration_seconds: research_feedback.stage_duration,
            feedback_cycle_active: research_feedback.cycle_active,
            feedback_cycle_queued: research_feedback.cycle_queued,
            feedback_outbox_pending: research_feedback.outbox_pending,
            feedback_stuck_total: research_feedback.stuck_total,
            feedback_resolution_inbox_age_seconds: research_feedback.resolution_inbox_age,
            feedback_resolution_quarantined: research_feedback.resolution_quarantined,
            feedback_resolution_projector_lag_seconds: research_feedback.resolution_projector_lag,
            feedback_attempt_projector_lag_seconds: research_feedback.attempt_projector_lag,
            feedback_rollup_lag_seconds: research_feedback.rollup_lag,
            feedback_scheduler_overdue_profiles: research_feedback.scheduler_overdue_profiles,
            feedback_scheduler_max_overdue_seconds: research_feedback.scheduler_max_overdue,
            feedback_attribution_efficiency_failures_total: research_feedback
                .attribution_efficiency_failures,
            feedback_quality_gate_total: research_feedback.quality_gate_total,
            feedback_permit_expiry_total: research_feedback.permit_expiry_total,
            feedback_route_governance_conflict_total: research_feedback
                .route_governance_conflict_total,
            feedback_ws_recovery_total: research_feedback.ws_recovery_total,
            research_progress_coalesced_total: research_feedback.progress_coalesced,
            research_heartbeat_total: research_feedback.heartbeat_total,
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
            settlement_worker_pass_total: execution.settlement_worker_pass_total,
            settlement_worker_error_total: execution.settlement_worker_error_total,
            settlement_lease_lost_total: execution.settlement_lease_lost_total,
            settlement_reconciliation_required_total: execution
                .settlement_reconciliation_required_total,
            settlement_discovery_lag_seconds: execution.settlement_discovery_lag_seconds,
            settlement_submission_age_seconds: execution.settlement_submission_age_seconds,
            settlement_finality_lag_seconds: execution.settlement_finality_lag_seconds,
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

    /// Count one exit-monitor trigger for an exit reason.
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

    pub fn record_settlement_worker_pass(&self, worker: &str, outcome: &str) {
        self.settlement_worker_pass_total
            .with_label_values(&[worker, outcome])
            .inc();
    }

    pub fn record_settlement_worker_error(&self, worker: &str) {
        self.settlement_worker_error_total
            .with_label_values(&[worker])
            .inc();
    }

    pub fn record_settlement_lease_lost(&self, workflow: &str) {
        self.settlement_lease_lost_total
            .with_label_values(&[workflow])
            .inc();
    }

    pub fn record_settlement_reconciliation_required(&self, workflow: &str, failure_code: &str) {
        self.settlement_reconciliation_required_total
            .with_label_values(&[workflow, failure_code])
            .inc();
    }

    pub fn observe_discovery_lag_ms(&self, lag_ms: u64) {
        self.settlement_discovery_lag_seconds
            .observe(lag_secs_from_ms(lag_ms));
    }

    pub fn observe_submission_age_ms(&self, workflow: &str, age_ms: u64) {
        self.settlement_submission_age_seconds
            .with_label_values(&[workflow])
            .observe(lag_secs_from_ms(age_ms));
    }

    pub fn observe_finality_lag_ms(&self, workflow: &str, lag_ms: u64) {
        self.settlement_finality_lag_seconds
            .with_label_values(&[workflow])
            .observe(lag_secs_from_ms(lag_ms));
    }

    /// Publish whether the kill-switch currently blocks new auto entries.
    pub fn set_auto_execution_halted(&self, halted: bool) {
        self.auto_execution_halted.set(i64::from(halted));
    }

    /// Observe one ingest-pipeline-lag sample (enqueue→flush-ack) for a writer.
    pub fn observe_ingest_lag_ms(&self, writer: &str, lag_ms: u64) {
        self.ingest_pipeline_lag_seconds
            .with_label_values(&[writer])
            .observe(lag_secs_from_ms(lag_ms));
    }

    /// Publish the peak ingest pipeline lag for the elapsed observation window.
    pub fn set_worst_ingest_lag(&self, lag_ms: u64) {
        self.ingest_pipeline_lag_worst_ms
            .set(i64::try_from(lag_ms).unwrap_or(i64::MAX));
    }

    /// Build observability handles for one named async writer.
    ///
    /// Every writer reports its enqueue→flush-ack latency into the per-writer
    /// `ingest_pipeline_lag_seconds` histogram (complete backpressure telemetry).
    /// Feeding the plane-level ingest-lag tracker that gates book-plane
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

    pub fn inc_fact_claim_lost(&self, operation: &str, status: &str) {
        self.report_fact_settlement_claim_lost_total
            .with_label_values(&[operation, status])
            .inc();
    }

    pub fn inc_fact_worker_error(&self, stage: &str) {
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

    pub fn set_parity_latch_open(&self, open: bool) {
        self.feature_parity_latch_open.set(i64::from(open));
    }

    pub fn record_feature_parity_containment(&self, outcome: &str) {
        self.feature_parity_containment_total
            .with_label_values(&[outcome])
            .inc();
    }

    pub fn record_feedback_cycle(&self, status: &str, decision: &str, duration_secs: f64) {
        self.feedback_cycle_total
            .with_label_values(&[status, decision])
            .inc();
        self.feedback_cycle_duration_seconds
            .with_label_values(&[status])
            .observe(duration_secs);
    }

    pub fn observe_feedback_stage(&self, stage: &str, status: &str, duration_secs: f64) {
        self.feedback_stage_duration_seconds
            .with_label_values(&[stage, status])
            .observe(duration_secs);
    }

    pub fn set_feedback_queue(&self, queued: u64, pending_outbox: u64) {
        self.feedback_cycle_queued
            .set(i64::try_from(queued).unwrap_or(i64::MAX));
        self.feedback_outbox_pending
            .set(i64::try_from(pending_outbox).unwrap_or(i64::MAX));
    }

    pub fn set_feedback_truth(
        &self,
        inbox_age_secs: u64,
        quarantined: u64,
        resolution_lag_secs: u64,
        attempt_lag_secs: u64,
        rollup_lag_secs: u64,
    ) {
        self.feedback_resolution_inbox_age_seconds
            .set(i64::try_from(inbox_age_secs).unwrap_or(i64::MAX));
        self.feedback_resolution_quarantined
            .set(i64::try_from(quarantined).unwrap_or(i64::MAX));
        self.feedback_resolution_projector_lag_seconds
            .set(i64::try_from(resolution_lag_secs).unwrap_or(i64::MAX));
        self.feedback_attempt_projector_lag_seconds
            .set(i64::try_from(attempt_lag_secs).unwrap_or(i64::MAX));
        self.feedback_rollup_lag_seconds
            .set(i64::try_from(rollup_lag_secs).unwrap_or(i64::MAX));
    }

    pub fn set_scheduler_overdue(&self, profiles: u64, max_overdue_secs: u64) {
        self.feedback_scheduler_overdue_profiles
            .set(i64::try_from(profiles).unwrap_or(i64::MAX));
        self.feedback_scheduler_max_overdue_seconds
            .set(i64::try_from(max_overdue_secs).unwrap_or(i64::MAX));
    }

    pub fn record_attribution_efficiency_failure(&self, method: &str) {
        self.feedback_attribution_efficiency_failures_total
            .with_label_values(&[method])
            .inc();
    }

    pub fn record_feedback_quality_gate(&self, gate: &str, status: &str) {
        self.feedback_quality_gate_total
            .with_label_values(&[gate, status])
            .inc();
    }

    pub fn record_feedback_permit_expiry(&self) {
        self.feedback_permit_expiry_total.inc();
    }

    pub fn record_route_governance_conflict(&self, action: &str, layer: &str) {
        self.feedback_route_governance_conflict_total
            .with_label_values(&[action, layer])
            .inc();
    }

    pub fn record_feedback_ws_recovery(&self, outcome: &str) {
        self.feedback_ws_recovery_total
            .with_label_values(&[outcome])
            .inc();
    }

    pub fn record_research_heartbeat(&self, operation: &str, result: &str) {
        self.research_heartbeat_total
            .with_label_values(&[operation, result])
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
    fn order_intent_created_increase() {
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
    fn durable_report_metrics_contract() {
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
            .with_label_values(&["global", "top_n"])
            .set(60);
        hub.inc_fact_claim_lost("verify", "cancelled");
        hub.inc_fact_worker_error("process_one");

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
        assert!(body.contains(r#"scope="global""#));
    }

    #[test]
    fn feedback_operations_metrics_contract() {
        let hub = MetricsHub::new();
        hub.set_feedback_truth(60, 2, 30, 20, 10);
        hub.set_scheduler_overdue(1, 120);
        hub.record_attribution_efficiency_failure("exact_tree_shap");
        hub.record_feedback_quality_gate("pbo", "fail");
        hub.record_feedback_permit_expiry();
        hub.record_route_governance_conflict("promotion", "route");
        hub.record_feedback_ws_recovery("recovered");

        let (_, text) = hub.gather_prometheus_text().expect("gather");
        let body = String::from_utf8(text).expect("utf8");
        for name in [
            "quant_feedback_resolution_inbox_oldest_age_seconds",
            "quant_feedback_resolution_quarantined",
            "quant_feedback_resolution_projector_lag_seconds",
            "quant_feedback_execution_attempt_projector_lag_seconds",
            "quant_feedback_recommendation_rollup_lag_seconds",
            "quant_feedback_scheduler_overdue_profiles",
            "quant_feedback_scheduler_max_overdue_seconds",
            "quant_feedback_attribution_efficiency_failures_total",
            "quant_feedback_quality_gate_total",
            "quant_feedback_promotion_permit_expiry_total",
            "quant_feedback_route_governance_conflict_total",
            "quant_feedback_ws_recovery_total",
        ] {
            assert!(body.contains(name), "missing metric {name}");
        }
        assert!(body.contains(r#"method="exact_tree_shap""#));
        assert!(body.contains(r#"gate="pbo""#));
        assert!(body.contains(r#"layer="route""#));
        assert!(body.contains(r#"outcome="recovered""#));
    }

    #[test]
    fn settlement_metrics_expose_contract() {
        let hub = MetricsHub::new();
        hub.record_settlement_worker_pass("recovery", "confirmed");
        hub.record_settlement_worker_error("discovery");
        hub.record_settlement_lease_lost("redeem");
        hub.record_settlement_reconciliation_required("governed_action", "receipt_mismatch");
        hub.observe_discovery_lag_ms(2_000);
        hub.observe_submission_age_ms("redeem", 3_000);
        hub.observe_finality_lag_ms("governed_action", 4_000);

        let (_, text) = hub.gather_prometheus_text().expect("gather");
        let body = String::from_utf8(text).expect("utf8");
        for name in [
            "quant_settlement_worker_pass_total",
            "quant_settlement_worker_error_total",
            "quant_settlement_lease_lost_total",
            "quant_settlement_reconciliation_required_total",
            "quant_settlement_discovery_lag_seconds",
            "quant_settlement_submission_age_seconds",
            "quant_settlement_finality_lag_seconds",
        ] {
            assert!(body.contains(name), "missing metric {name}");
        }
        assert!(body.contains(r#"worker="recovery""#));
        assert!(body.contains(r#"outcome="confirmed""#));
        assert!(body.contains(r#"failure_code="receipt_mismatch""#));
    }
}
