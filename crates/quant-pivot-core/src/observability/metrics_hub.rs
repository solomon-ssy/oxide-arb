use oxide_arb_algorithm::DetectionRejectReason;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
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

macro_rules! register_histogram {
    ($registry:expr, $name:expr, $help:expr) => {{
        let histogram = Histogram::with_opts(HistogramOpts::new($name, $help)).unwrap();
        $registry.register(Box::new(histogram.clone())).unwrap();
        histogram
    }};
    ($registry:expr, $name:expr, $help:expr, $buckets:expr) => {{
        let histogram =
            Histogram::with_opts(HistogramOpts::new($name, $help).buckets($buckets)).unwrap();
        $registry.register(Box::new(histogram.clone())).unwrap();
        histogram
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

pub struct MetricsHub {
    pub registry: Registry,

    // Data Pipeline
    pub ws_events_received: IntCounter,
    pub book_snapshots_applied: IntCounter,
    pub price_changes_applied: IntCounter,
    pub ws_events_ignored: IntCounter,
    pub ws_events_dropped: IntCounter,
    pub book_level_rejected: IntCounterVec,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub ws_shard_connected: IntGaugeVec,
    pub book_store_token_count: IntGauge,
    pub book_apply_dropped: IntCounter,
    pub book_apply_coalesced_total: IntCounter,
    pub backpressure_events: IntCounterVec,

    // Detection
    pub scans_gate_rejected: IntCounter,
    pub scan_rejects: IntCounterVec,
    pub coalesced_scans: IntCounter,
    pub coalescer_dropped: IntCounter,
    pub scan_results_total: Histogram,
    pub scan_duration_seconds: Histogram,
    pub opportunities_detected: IntCounter,

    // Funnel
    pub funnel_enqueued: IntCounter,
    pub funnel_dispatched: IntCounter,
    pub funnel_dropped: IntCounter,
    pub funnel_dispatch_age_ms: Histogram,
    pub funnel_queue_depth: IntGauge,
    pub funnel_fast_lane_dispatched: IntCounter,
    pub execution_shard_evicted_total: IntCounter,

    // Execution
    pub execution_latency: Histogram,
    pub execute_intent_to_http_us: Histogram,
    pub book_freshness_rejected: IntCounter,
    pub trades_filled: IntCounter,
    pub trades_missed: IntCounter,
    pub trades_failed: IntCounter,
    pub risk_denials: IntCounter,
    pub validation_failures: IntCounter,
    pub sizing_zero: IntCounter,
    pub reservation_failures: IntCounter,
    pub execution_market_busy: IntCounter,
    pub post_trade_relay_processed: IntCounter,
    pub post_trade_relay_failed: IntCounter,

    // FOK execution
    pub fok_fills: IntCounter,
    pub fok_misses: IntCounter,

    // Latency segments (WS → scan → dispatch → HTTP)
    pub latency_ws_to_scan_us: Histogram,
    pub latency_scan_to_dispatch_us: Histogram,
    pub latency_tick_to_http_us: Histogram,

    // Kill switch
    pub fsm_emergency_entries: IntCounter,
    pub venue_cancel_all_total: IntCounter,

    // Risk
    pub risk_checks_total: IntCounter,
    pub risk_exposure_usd: Gauge,
    pub risk_daily_pnl_usd: Gauge,
    pub risk_daily_loss_usd: Gauge,
    pub risk_weekly_loss_usd: Gauge,
    pub risk_reservations_active: IntGauge,
    pub risk_reservations_total_usd: Gauge,
    pub exposure_gc_cleaned: IntCounter,

    // Calibration
    pub calibration_update_total: IntCounter,
    pub calibration_resolved: IntCounter,
    pub calibration_bucket_count: IntGauge,

    // System
    pub uptime_seconds: IntGauge,
    pub active_tasks: IntGauge,
    pub health_check_failures: IntCounter,
    pub post_trade_relay_pending: IntGauge,
    pub async_writer_dropped: IntCounterVec,

    // Gamma sync
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,
    pub gamma_markets_rejected: IntCounterVec,
    pub gamma_markets_paused: IntCounterVec,
    pub catalog_ready: IntGauge,

    // WS hotset
    pub hotset_candidates_total: IntCounterVec,
    pub hotset_selected_total: IntCounterVec,
    pub hotset_detection_window_coverage_ratio: Gauge,

    // Metrics refresh
    pub metrics_refresh_failures: IntCounter,

    // Settlement
    pub settlement_requests_total: IntCounterVec,
    pub settlement_positions_settled_total: IntCounter,
    pub settlement_redeem_success_total: IntCounterVec,
    pub settlement_redeem_failure_total: IntCounterVec,
    pub settlement_oracle_mismatch_total: IntCounter,
    pub settlement_channel_dropped_total: IntCounter,
    pub settlement_no_open_positions_total: IntCounter,
    pub settlement_duration_ms: Histogram,

    // Control factors (Phase 5.6 live consumption)
    pub control_factor_refresh_total: IntCounter,
    pub control_factor_refresh_failures: IntCounter,
    pub control_factor_version_changes: IntCounter,
    pub control_factor_snapshot_load_age_seconds: IntGauge,
    pub control_factor_active_count: IntGauge,
    pub control_factor_hard_rejects: IntCounter,
    pub control_factor_validation_rejections: IntCounter,
    pub control_factor_shadow_decisions: IntCounter,
    pub control_factor_shadow_dropped: IntCounter,
    pub control_factor_fail_closed_events: IntCounter,
    pub control_factor_publication_active: IntGauge,

    // Shutdown
    pub shutdown_stage_progress_remaining: IntGaugeVec,
    pub shutdown_stage_timeouts: IntCounterVec,
}

struct PipelineMetrics {
    ws_events_received: IntCounter,
    book_snapshots_applied: IntCounter,
    price_changes_applied: IntCounter,
    ws_events_ignored: IntCounter,
    ws_events_dropped: IntCounter,
    book_level_rejected: IntCounterVec,
    markets_resolved_ws: IntCounter,
    shard_status_changes: IntCounter,
    ws_shard_connected: IntGaugeVec,
    book_store_token_count: IntGauge,
    book_apply_dropped: IntCounter,
    book_apply_coalesced_total: IntCounter,
    backpressure_events: IntCounterVec,
}

struct DetectionMetrics {
    scans_gate_rejected: IntCounter,
    scan_rejects: IntCounterVec,
    coalesced_scans: IntCounter,
    coalescer_dropped: IntCounter,
    scan_results_total: Histogram,
    scan_duration_seconds: Histogram,
    opportunities_detected: IntCounter,
}

struct FunnelMetrics {
    enqueued: IntCounter,
    dispatched: IntCounter,
    dropped: IntCounter,
    dispatch_age_ms: Histogram,
    queue_depth: IntGauge,
    fast_lane_dispatched: IntCounter,
    execution_shard_evicted_total: IntCounter,
}

struct ExecutionMetrics {
    execution_latency: Histogram,
    execute_intent_to_http_us: Histogram,
    book_freshness_rejected: IntCounter,
    trades_filled: IntCounter,
    trades_missed: IntCounter,
    trades_failed: IntCounter,
    risk_denials: IntCounter,
    validation_failures: IntCounter,
    sizing_zero: IntCounter,
    reservation_failures: IntCounter,
    execution_market_busy: IntCounter,
    post_trade_relay_processed: IntCounter,
    post_trade_relay_failed: IntCounter,
    fok_fills: IntCounter,
    fok_misses: IntCounter,
    latency_ws_to_scan_us: Histogram,
    latency_scan_to_dispatch_us: Histogram,
    latency_tick_to_http_us: Histogram,
    fsm_emergency_entries: IntCounter,
    venue_cancel_all_total: IntCounter,
}

struct RiskMetrics {
    checks_total: IntCounter,
    exposure_usd: Gauge,
    daily_pnl_usd: Gauge,
    daily_loss_usd: Gauge,
    weekly_loss_usd: Gauge,
    reservations_active: IntGauge,
    reservations_total_usd: Gauge,
    exposure_gc_cleaned: IntCounter,
}

struct CalibrationMetrics {
    update_total: IntCounter,
    resolved: IntCounter,
    bucket_count: IntGauge,
}

struct HotsetMetrics {
    candidates_total: IntCounterVec,
    selected_total: IntCounterVec,
    detection_window_coverage_ratio: Gauge,
}

struct SystemMetrics {
    uptime_seconds: IntGauge,
    active_tasks: IntGauge,
    health_check_failures: IntCounter,
    post_trade_relay_pending: IntGauge,
    async_writer_dropped: IntCounterVec,
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    gamma_markets_rejected: IntCounterVec,
    gamma_markets_paused: IntCounterVec,
    catalog_ready: IntGauge,
    metrics_refresh_failures: IntCounter,
    shutdown_stage_progress_remaining: IntGaugeVec,
    shutdown_stage_timeouts: IntCounterVec,
}

struct ControlFactorMetrics {
    refresh_total: IntCounter,
    refresh_failures: IntCounter,
    version_changes: IntCounter,
    snapshot_load_age_seconds: IntGauge,
    active_count: IntGauge,
    hard_rejects: IntCounter,
    validation_rejections: IntCounter,
    shadow_decisions: IntCounter,
    shadow_dropped: IntCounter,
    fail_closed_events: IntCounter,
    publication_active: IntGauge,
}

fn register_control_factor_metrics(registry: &Registry) -> ControlFactorMetrics {
    ControlFactorMetrics {
        refresh_total: register_counter!(
            registry,
            "oxide_arb_control_factor_refresh_total",
            "Control-factor snapshot refresh attempts"
        ),
        refresh_failures: register_counter!(
            registry,
            "oxide_arb_control_factor_refresh_failures_total",
            "Control-factor snapshot refresh failures (prior snapshot retained)"
        ),
        version_changes: register_counter!(
            registry,
            "oxide_arb_control_factor_version_changes_total",
            "Control-factor publication version changes applied"
        ),
        snapshot_load_age_seconds: register_gauge_int!(
            registry,
            "oxide_arb_control_factor_snapshot_load_age_seconds",
            "Seconds since the active control-factor snapshot was loaded"
        ),
        active_count: register_gauge_int!(
            registry,
            "oxide_arb_control_factor_active_count",
            "Number of factors in the active published snapshot"
        ),
        hard_rejects: register_counter!(
            registry,
            "oxide_arb_control_factor_hard_rejects_total",
            "Trades hard-rejected by a named control-factor risk gate"
        ),
        validation_rejections: register_counter!(
            registry,
            "oxide_arb_control_factor_validation_rejections_total",
            "Opportunities rejected by execution-quality factor validation"
        ),
        shadow_decisions: register_counter!(
            registry,
            "oxide_arb_control_factor_shadow_decisions_total",
            "Shadow decisions recorded"
        ),
        shadow_dropped: register_counter!(
            registry,
            "oxide_arb_control_factor_shadow_dropped_total",
            "Shadow decisions dropped under backpressure or write failure"
        ),
        fail_closed_events: register_counter!(
            registry,
            "oxide_arb_control_factor_fail_closed_events_total",
            "Fail-closed events from expired/unloadable safety factors"
        ),
        publication_active: register_gauge_int!(
            registry,
            "oxide_arb_control_factor_publication_active",
            "Whether a published control-factor snapshot is active in Live (1=yes, 0=no)"
        ),
    }
}

struct SettlementMetrics {
    requests_total: IntCounterVec,
    positions_settled_total: IntCounter,
    redeem_success_total: IntCounterVec,
    redeem_failure_total: IntCounterVec,
    oracle_mismatch_total: IntCounter,
    channel_dropped_total: IntCounter,
    no_open_positions_total: IntCounter,
    duration_ms: Histogram,
}

fn register_pipeline_metrics(registry: &Registry) -> PipelineMetrics {
    PipelineMetrics {
        ws_events_received: register_counter!(
            registry,
            "oxide_arb_pipeline_ws_events_received_total",
            "WebSocket events received"
        ),
        book_snapshots_applied: register_counter!(
            registry,
            "oxide_arb_pipeline_book_snapshots_applied_total",
            "Book snapshots applied"
        ),
        price_changes_applied: register_counter!(
            registry,
            "oxide_arb_pipeline_price_changes_applied_total",
            "Price-change events applied"
        ),
        ws_events_ignored: register_counter!(
            registry,
            "oxide_arb_pipeline_ws_events_ignored_total",
            "WebSocket events ignored"
        ),
        ws_events_dropped: register_counter!(
            registry,
            "oxide_arb_pipeline_ws_events_dropped_total",
            "WebSocket events dropped when output channel full"
        ),
        book_level_rejected: register_counter_vec!(
            registry,
            "oxide_arb_pipeline_book_level_rejected_total",
            "Invalid book levels rejected at ingest",
            &["source"]
        ),
        markets_resolved_ws: register_counter!(
            registry,
            "oxide_arb_pipeline_markets_resolved_ws_total",
            "Markets resolved from WS"
        ),
        shard_status_changes: register_counter!(
            registry,
            "oxide_arb_pipeline_shard_status_changes_total",
            "Shard status transitions"
        ),
        ws_shard_connected: register_gauge_vec!(
            registry,
            "oxide_arb_pipeline_ws_shard_connected",
            "Per-shard CLOB WebSocket connection state (1 = connected)",
            &["shard"]
        ),
        book_store_token_count: register_gauge_int!(
            registry,
            "oxide_arb_pipeline_book_store_token_count",
            "Tokens tracked in book store"
        ),
        book_apply_dropped: register_counter!(
            registry,
            "oxide_arb_pipeline_book_apply_dropped_total",
            "WS events dropped when book apply queue full (non-coalescable)"
        ),
        book_apply_coalesced_total: register_counter!(
            registry,
            "oxide_arb_book_apply_coalesced_total",
            "Book apply events coalesced under backpressure"
        ),
        backpressure_events: register_counter_vec!(
            registry,
            "oxide_arb_backpressure_events_total",
            "Backpressure actions by site",
            &["site", "action"]
        ),
    }
}

fn register_detection_metrics(registry: &Registry) -> DetectionMetrics {
    DetectionMetrics {
        scans_gate_rejected: register_counter!(
            registry,
            "oxide_arb_detection_scans_gate_rejected_total",
            "Scans rejected by gate"
        ),
        scan_rejects: register_counter_vec!(
            registry,
            "oxide_arb_detection_scan_rejects_total",
            "Scans rejected by detection funnel stage",
            &["reason"]
        ),
        coalesced_scans: register_counter!(
            registry,
            "oxide_arb_detection_coalesced_scans_total",
            "Scans coalesced"
        ),
        coalescer_dropped: register_counter!(
            registry,
            "oxide_arb_detection_coalescer_dropped_total",
            "Token updates dropped when coalescer channel full"
        ),
        scan_results_total: register_histogram!(
            registry,
            "oxide_arb_detection_scan_results",
            "Opportunities per scan",
            vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0]
        ),
        scan_duration_seconds: register_histogram!(
            registry,
            "oxide_arb_detection_scan_duration_seconds",
            "Scan duration in seconds",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
        ),
        opportunities_detected: register_counter!(
            registry,
            "oxide_arb_detection_opportunities_detected_total",
            "Opportunities detected"
        ),
    }
}

fn register_funnel_metrics(registry: &Registry) -> FunnelMetrics {
    FunnelMetrics {
        enqueued: register_counter!(
            registry,
            "oxide_arb_funnel_enqueued_total",
            "Items enqueued to funnel"
        ),
        dispatched: register_counter!(
            registry,
            "oxide_arb_funnel_dispatched_total",
            "Items dispatched from funnel"
        ),
        dropped: register_counter!(
            registry,
            "oxide_arb_funnel_dropped_total",
            "Items dropped by funnel"
        ),
        dispatch_age_ms: register_histogram!(
            registry,
            "oxide_arb_funnel_dispatch_age_milliseconds",
            "Age of items at dispatch time (ms)",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]
        ),
        queue_depth: register_gauge_int!(
            registry,
            "oxide_arb_funnel_queue_depth",
            "Current funnel queue depth"
        ),
        fast_lane_dispatched: register_counter!(
            registry,
            "oxide_arb_funnel_fast_lane_dispatched_total",
            "Opportunities dispatched via fast lane (bypass funnel sweep)"
        ),
        execution_shard_evicted_total: register_counter!(
            registry,
            "oxide_arb_execution_shard_evicted_total",
            "Lowest-score funnel entries evicted under execution shard backpressure"
        ),
    }
}

struct LatencyMetrics {
    ws_to_scan: Histogram,
    scan_to_dispatch: Histogram,
    tick_to_http: Histogram,
}

fn register_latency_metrics(registry: &Registry) -> LatencyMetrics {
    LatencyMetrics {
        ws_to_scan: register_histogram!(
            registry,
            "oxide_arb_latency_ws_to_scan_microseconds",
            "WS ingress to scan emit",
            vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ),
        scan_to_dispatch: register_histogram!(
            registry,
            "oxide_arb_latency_scan_to_dispatch_microseconds",
            "Scan emit to execution dispatch",
            vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ),
        tick_to_http: register_histogram!(
            registry,
            "oxide_arb_latency_tick_to_http_microseconds",
            "Dispatch start to HTTP send",
            vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ),
    }
}

fn register_execution_metrics(registry: &Registry) -> ExecutionMetrics {
    let latency = register_latency_metrics(registry);
    ExecutionMetrics {
        execution_latency: register_histogram!(
            registry,
            "oxide_arb_execution_latency_seconds",
            "End-to-end execution latency",
            vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        ),
        execute_intent_to_http_us: register_histogram!(
            registry,
            "oxide_arb_execution_intent_to_http_microseconds",
            "Execute intent to HTTP request emit (SLO-1)",
            vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ),
        book_freshness_rejected: register_counter!(
            registry,
            "oxide_arb_execution_book_freshness_rejected_total",
            "Validation rejected due to book version/age SLO-2"
        ),
        trades_filled: register_counter!(
            registry,
            "oxide_arb_execution_trades_filled_total",
            "Trades filled"
        ),
        trades_missed: register_counter!(
            registry,
            "oxide_arb_execution_trades_missed_total",
            "Trades missed"
        ),
        trades_failed: register_counter!(
            registry,
            "oxide_arb_execution_trades_failed_total",
            "Trades failed"
        ),
        risk_denials: register_counter!(
            registry,
            "oxide_arb_execution_risk_denials_total",
            "Denied by risk check"
        ),
        validation_failures: register_counter!(
            registry,
            "oxide_arb_execution_validation_failures_total",
            "Validation failures"
        ),
        sizing_zero: register_counter!(
            registry,
            "oxide_arb_execution_sizing_zero_total",
            "Zero-size results"
        ),
        reservation_failures: register_counter!(
            registry,
            "oxide_arb_execution_reservation_failures_total",
            "Capital reservation failures"
        ),
        execution_market_busy: register_counter!(
            registry,
            "oxide_arb_execution_market_busy_total",
            "Rejected because market already in-flight"
        ),
        post_trade_relay_processed: register_counter!(
            registry,
            "oxide_arb_post_trade_relay_processed_total",
            "Trades advanced to terminal state by the post-trade relay"
        ),
        post_trade_relay_failed: register_counter!(
            registry,
            "oxide_arb_post_trade_relay_failed_total",
            "Post-trade relay processing failures"
        ),
        fok_fills: register_counter!(
            registry,
            "oxide_arb_execution_fok_fills_total",
            "Fills by FOK execution"
        ),
        fok_misses: register_counter!(
            registry,
            "oxide_arb_execution_fok_misses_total",
            "Misses or failures by FOK execution"
        ),
        latency_ws_to_scan_us: latency.ws_to_scan,
        latency_scan_to_dispatch_us: latency.scan_to_dispatch,
        latency_tick_to_http_us: latency.tick_to_http,
        fsm_emergency_entries: register_counter!(
            registry,
            "oxide_arb_fsm_emergency_entries_total",
            "Emergency state entries"
        ),
        venue_cancel_all_total: register_counter!(
            registry,
            "oxide_arb_venue_cancel_all_total",
            "Emergency cancel_all invocations"
        ),
    }
}

fn register_risk_metrics(registry: &Registry) -> RiskMetrics {
    RiskMetrics {
        checks_total: register_counter!(
            registry,
            "oxide_arb_risk_checks_total",
            "Total risk checks performed"
        ),
        exposure_usd: register_gauge_float!(
            registry,
            "oxide_arb_risk_exposure_usd",
            "Current exposure in USD"
        ),
        daily_pnl_usd: register_gauge_float!(
            registry,
            "oxide_arb_risk_daily_pnl_usd",
            "Daily PnL in USD"
        ),
        daily_loss_usd: register_gauge_float!(
            registry,
            "oxide_arb_risk_daily_loss_usd",
            "Daily loss in USD"
        ),
        weekly_loss_usd: register_gauge_float!(
            registry,
            "oxide_arb_risk_weekly_loss_usd",
            "Weekly loss in USD"
        ),
        reservations_active: register_gauge_int!(
            registry,
            "oxide_arb_risk_reservations_active",
            "Active capital reservations"
        ),
        reservations_total_usd: register_gauge_float!(
            registry,
            "oxide_arb_risk_reservations_total_usd",
            "Total reserved capital in USD"
        ),
        exposure_gc_cleaned: register_counter!(
            registry,
            "oxide_arb_risk_exposure_gc_cleaned_total",
            "Expired exposure reservations cleaned by GC"
        ),
    }
}

fn register_calibration_metrics(registry: &Registry) -> CalibrationMetrics {
    CalibrationMetrics {
        update_total: register_counter!(
            registry,
            "oxide_arb_calibration_update_total",
            "Calibration updates performed"
        ),
        resolved: register_counter!(
            registry,
            "oxide_arb_calibration_resolved_total",
            "Calibration entries resolved"
        ),
        bucket_count: register_gauge_int!(
            registry,
            "oxide_arb_calibration_bucket_count",
            "Active calibration buckets"
        ),
    }
}

fn register_system_metrics(registry: &Registry) -> SystemMetrics {
    SystemMetrics {
        uptime_seconds: register_gauge_int!(
            registry,
            "oxide_arb_system_uptime_seconds",
            "Process uptime"
        ),
        active_tasks: register_gauge_int!(
            registry,
            "oxide_arb_system_active_tasks",
            "Active background tasks"
        ),
        health_check_failures: register_counter!(
            registry,
            "oxide_arb_system_health_check_failures_total",
            "Health check failures"
        ),
        post_trade_relay_pending: register_gauge_int!(
            registry,
            "oxide_arb_post_trade_relay_pending",
            "Unprocessed post-trade rows claimed in the last relay drain"
        ),
        async_writer_dropped: register_counter_vec!(
            registry,
            "oxide_arb_system_async_writer_dropped_total",
            "AsyncWriter items dropped due to channel pressure",
            &["writer"]
        ),
        gamma_sync_duration_ms: register_gauge_int!(
            registry,
            "oxide_arb_gamma_sync_duration_ms",
            "Last gamma sync duration in milliseconds"
        ),
        gamma_markets_total: register_gauge_int!(
            registry,
            "oxide_arb_gamma_markets_total",
            "Total markets from last gamma full sync"
        ),
        gamma_last_sync_success: register_gauge_int!(
            registry,
            "oxide_arb_gamma_last_sync_success",
            "1 if the last gamma sync succeeded, 0 otherwise"
        ),
        gamma_markets_rejected: register_counter_vec!(
            registry,
            "oxide_arb_gamma_markets_rejected_total",
            "Markets dropped during Gamma catalog normalization, by reason",
            &["reason"]
        ),
        gamma_markets_paused: register_counter_vec!(
            registry,
            "oxide_arb_gamma_markets_paused_total",
            "Markets transitioned to paused during catalog sync, by reason",
            &["reason"]
        ),
        catalog_ready: register_gauge_int!(
            registry,
            "oxide_arb_catalog_ready",
            "1 once the first Gamma catalog sync completed (detection unlocked)"
        ),
        metrics_refresh_failures: register_counter!(
            registry,
            "oxide_arb_system_metrics_refresh_failures_total",
            "Metrics refresh failures"
        ),
        shutdown_stage_progress_remaining: register_gauge_vec!(
            registry,
            "oxide_arb_shutdown_stage_progress_remaining",
            "Tasks remaining during staged shutdown",
            &["stage"]
        ),
        shutdown_stage_timeouts: register_counter_vec!(
            registry,
            "oxide_arb_shutdown_stage_timeouts_total",
            "Staged shutdown stage timeouts",
            &["stage"]
        ),
    }
}

fn register_hotset_metrics(registry: &Registry) -> HotsetMetrics {
    HotsetMetrics {
        candidates_total: register_counter_vec!(
            registry,
            "oxide_arb_hotset_candidates_total",
            "WS hotset candidate markets by lifecycle bucket",
            &["kind"]
        ),
        selected_total: register_counter_vec!(
            registry,
            "oxide_arb_hotset_selected_total",
            "WS hotset selected markets by tier",
            &["kind"]
        ),
        detection_window_coverage_ratio: register_gauge_float!(
            registry,
            "oxide_arb_hotset_detection_window_coverage_ratio",
            "Fraction of detection-window candidates selected into the hotset"
        ),
    }
}

fn register_settlement_metrics(registry: &Registry) -> SettlementMetrics {
    SettlementMetrics {
        requests_total: register_counter_vec!(
            registry,
            "oxide_arb_settlement_requests_total",
            "Settlement requests by source",
            &["source"]
        ),
        positions_settled_total: register_counter!(
            registry,
            "oxide_arb_settlement_positions_settled_total",
            "Positions settled"
        ),
        redeem_success_total: register_counter_vec!(
            registry,
            "oxide_arb_settlement_redeem_success_total",
            "CTF redeem successes",
            &["route", "resolution"]
        ),
        redeem_failure_total: register_counter_vec!(
            registry,
            "oxide_arb_settlement_redeem_failure_total",
            "CTF redeem failures",
            &["route", "error_class"]
        ),
        oracle_mismatch_total: register_counter!(
            registry,
            "oxide_arb_settlement_oracle_mismatch_total",
            "Oracle audit mismatches after settlement"
        ),
        channel_dropped_total: register_counter!(
            registry,
            "oxide_arb_settlement_channel_dropped_total",
            "Settlement requests dropped because channel send failed"
        ),
        no_open_positions_total: register_counter!(
            registry,
            "oxide_arb_settlement_no_open_positions_total",
            "Settlement requests with no open positions"
        ),
        duration_ms: register_histogram!(
            registry,
            "oxide_arb_settlement_duration_milliseconds",
            "Settlement processing duration in milliseconds",
            vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 30000.0]
        ),
    }
}

/// Registered metric groups, flattened into [`MetricsHub`] by
/// [`MetricsHub::from_groups`].
struct MetricGroups {
    pipeline: PipelineMetrics,
    detection: DetectionMetrics,
    funnel: FunnelMetrics,
    execution: ExecutionMetrics,
    risk: RiskMetrics,
    calibration: CalibrationMetrics,
    system: SystemMetrics,
    settlement: SettlementMetrics,
    control_factor: ControlFactorMetrics,
    hotset: HotsetMetrics,
}

/// Operational metric handles assembled from non-data-plane registration groups.
struct HubOperationalMetrics {
    risk_checks_total: IntCounter,
    risk_exposure_usd: Gauge,
    risk_daily_pnl_usd: Gauge,
    risk_daily_loss_usd: Gauge,
    risk_weekly_loss_usd: Gauge,
    risk_reservations_active: IntGauge,
    risk_reservations_total_usd: Gauge,
    exposure_gc_cleaned: IntCounter,
    calibration_update_total: IntCounter,
    calibration_resolved: IntCounter,
    calibration_bucket_count: IntGauge,
    uptime_seconds: IntGauge,
    active_tasks: IntGauge,
    health_check_failures: IntCounter,
    post_trade_relay_pending: IntGauge,
    async_writer_dropped: IntCounterVec,
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    gamma_markets_rejected: IntCounterVec,
    gamma_markets_paused: IntCounterVec,
    catalog_ready: IntGauge,
    hotset_candidates_total: IntCounterVec,
    hotset_selected_total: IntCounterVec,
    hotset_detection_window_coverage_ratio: Gauge,
    metrics_refresh_failures: IntCounter,
    settlement_requests_total: IntCounterVec,
    settlement_positions_settled_total: IntCounter,
    settlement_redeem_success_total: IntCounterVec,
    settlement_redeem_failure_total: IntCounterVec,
    settlement_oracle_mismatch_total: IntCounter,
    settlement_channel_dropped_total: IntCounter,
    settlement_no_open_positions_total: IntCounter,
    settlement_duration_ms: Histogram,
    control_factor_refresh_total: IntCounter,
    control_factor_refresh_failures: IntCounter,
    control_factor_version_changes: IntCounter,
    control_factor_snapshot_load_age_seconds: IntGauge,
    control_factor_active_count: IntGauge,
    control_factor_hard_rejects: IntCounter,
    control_factor_validation_rejections: IntCounter,
    control_factor_shadow_decisions: IntCounter,
    control_factor_shadow_dropped: IntCounter,
    control_factor_fail_closed_events: IntCounter,
    control_factor_publication_active: IntGauge,
    shutdown_stage_progress_remaining: IntGaugeVec,
    shutdown_stage_timeouts: IntCounterVec,
}

impl HubOperationalMetrics {
    fn from_groups(
        risk: RiskMetrics,
        calibration: CalibrationMetrics,
        system: SystemMetrics,
        settlement: SettlementMetrics,
        control_factor: ControlFactorMetrics,
        hotset: HotsetMetrics,
    ) -> Self {
        Self {
            risk_checks_total: risk.checks_total,
            risk_exposure_usd: risk.exposure_usd,
            risk_daily_pnl_usd: risk.daily_pnl_usd,
            risk_daily_loss_usd: risk.daily_loss_usd,
            risk_weekly_loss_usd: risk.weekly_loss_usd,
            risk_reservations_active: risk.reservations_active,
            risk_reservations_total_usd: risk.reservations_total_usd,
            exposure_gc_cleaned: risk.exposure_gc_cleaned,
            calibration_update_total: calibration.update_total,
            calibration_resolved: calibration.resolved,
            calibration_bucket_count: calibration.bucket_count,
            uptime_seconds: system.uptime_seconds,
            active_tasks: system.active_tasks,
            health_check_failures: system.health_check_failures,
            post_trade_relay_pending: system.post_trade_relay_pending,
            async_writer_dropped: system.async_writer_dropped,
            gamma_sync_duration_ms: system.gamma_sync_duration_ms,
            gamma_markets_total: system.gamma_markets_total,
            gamma_last_sync_success: system.gamma_last_sync_success,
            gamma_markets_rejected: system.gamma_markets_rejected,
            gamma_markets_paused: system.gamma_markets_paused,
            catalog_ready: system.catalog_ready,
            hotset_candidates_total: hotset.candidates_total,
            hotset_selected_total: hotset.selected_total,
            hotset_detection_window_coverage_ratio: hotset.detection_window_coverage_ratio,
            metrics_refresh_failures: system.metrics_refresh_failures,
            settlement_requests_total: settlement.requests_total,
            settlement_positions_settled_total: settlement.positions_settled_total,
            settlement_redeem_success_total: settlement.redeem_success_total,
            settlement_redeem_failure_total: settlement.redeem_failure_total,
            settlement_oracle_mismatch_total: settlement.oracle_mismatch_total,
            settlement_channel_dropped_total: settlement.channel_dropped_total,
            settlement_no_open_positions_total: settlement.no_open_positions_total,
            settlement_duration_ms: settlement.duration_ms,
            control_factor_refresh_total: control_factor.refresh_total,
            control_factor_refresh_failures: control_factor.refresh_failures,
            control_factor_version_changes: control_factor.version_changes,
            control_factor_snapshot_load_age_seconds: control_factor.snapshot_load_age_seconds,
            control_factor_active_count: control_factor.active_count,
            control_factor_hard_rejects: control_factor.hard_rejects,
            control_factor_validation_rejections: control_factor.validation_rejections,
            control_factor_shadow_decisions: control_factor.shadow_decisions,
            control_factor_shadow_dropped: control_factor.shadow_dropped,
            control_factor_fail_closed_events: control_factor.fail_closed_events,
            control_factor_publication_active: control_factor.publication_active,
            shutdown_stage_progress_remaining: system.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: system.shutdown_stage_timeouts,
        }
    }
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = Registry::new();
        let groups = MetricGroups {
            pipeline: register_pipeline_metrics(&registry),
            detection: register_detection_metrics(&registry),
            funnel: register_funnel_metrics(&registry),
            execution: register_execution_metrics(&registry),
            risk: register_risk_metrics(&registry),
            calibration: register_calibration_metrics(&registry),
            system: register_system_metrics(&registry),
            settlement: register_settlement_metrics(&registry),
            control_factor: register_control_factor_metrics(&registry),
            hotset: register_hotset_metrics(&registry),
        };
        Self::from_groups(registry, groups)
    }

    fn from_groups(registry: Registry, g: MetricGroups) -> Self {
        let MetricGroups {
            pipeline,
            detection,
            funnel,
            execution,
            risk,
            calibration,
            system,
            settlement,
            control_factor,
            hotset,
        } = g;
        let operational = HubOperationalMetrics::from_groups(
            risk,
            calibration,
            system,
            settlement,
            control_factor,
            hotset,
        );
        Self::assemble_hub(registry, pipeline, detection, funnel, execution, operational)
    }

    fn assemble_hub(
        registry: Registry,
        pipeline: PipelineMetrics,
        detection: DetectionMetrics,
        funnel: FunnelMetrics,
        execution: ExecutionMetrics,
        operational: HubOperationalMetrics,
    ) -> Self {
        Self {
            registry,
            ws_events_received: pipeline.ws_events_received,
            book_snapshots_applied: pipeline.book_snapshots_applied,
            price_changes_applied: pipeline.price_changes_applied,
            ws_events_ignored: pipeline.ws_events_ignored,
            ws_events_dropped: pipeline.ws_events_dropped,
            book_level_rejected: pipeline.book_level_rejected,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            ws_shard_connected: pipeline.ws_shard_connected,
            book_store_token_count: pipeline.book_store_token_count,
            book_apply_dropped: pipeline.book_apply_dropped,
            book_apply_coalesced_total: pipeline.book_apply_coalesced_total,
            backpressure_events: pipeline.backpressure_events,
            scans_gate_rejected: detection.scans_gate_rejected,
            scan_rejects: detection.scan_rejects,
            coalesced_scans: detection.coalesced_scans,
            coalescer_dropped: detection.coalescer_dropped,
            scan_results_total: detection.scan_results_total,
            scan_duration_seconds: detection.scan_duration_seconds,
            opportunities_detected: detection.opportunities_detected,
            funnel_enqueued: funnel.enqueued,
            funnel_dispatched: funnel.dispatched,
            funnel_dropped: funnel.dropped,
            funnel_dispatch_age_ms: funnel.dispatch_age_ms,
            funnel_queue_depth: funnel.queue_depth,
            funnel_fast_lane_dispatched: funnel.fast_lane_dispatched,
            execution_shard_evicted_total: funnel.execution_shard_evicted_total,
            execution_latency: execution.execution_latency,
            execute_intent_to_http_us: execution.execute_intent_to_http_us,
            book_freshness_rejected: execution.book_freshness_rejected,
            trades_filled: execution.trades_filled,
            trades_missed: execution.trades_missed,
            trades_failed: execution.trades_failed,
            risk_denials: execution.risk_denials,
            validation_failures: execution.validation_failures,
            sizing_zero: execution.sizing_zero,
            reservation_failures: execution.reservation_failures,
            execution_market_busy: execution.execution_market_busy,
            post_trade_relay_processed: execution.post_trade_relay_processed,
            post_trade_relay_failed: execution.post_trade_relay_failed,
            fok_fills: execution.fok_fills,
            fok_misses: execution.fok_misses,
            latency_ws_to_scan_us: execution.latency_ws_to_scan_us,
            latency_scan_to_dispatch_us: execution.latency_scan_to_dispatch_us,
            latency_tick_to_http_us: execution.latency_tick_to_http_us,
            fsm_emergency_entries: execution.fsm_emergency_entries,
            venue_cancel_all_total: execution.venue_cancel_all_total,
            risk_checks_total: operational.risk_checks_total,
            risk_exposure_usd: operational.risk_exposure_usd,
            risk_daily_pnl_usd: operational.risk_daily_pnl_usd,
            risk_daily_loss_usd: operational.risk_daily_loss_usd,
            risk_weekly_loss_usd: operational.risk_weekly_loss_usd,
            risk_reservations_active: operational.risk_reservations_active,
            risk_reservations_total_usd: operational.risk_reservations_total_usd,
            exposure_gc_cleaned: operational.exposure_gc_cleaned,
            calibration_update_total: operational.calibration_update_total,
            calibration_resolved: operational.calibration_resolved,
            calibration_bucket_count: operational.calibration_bucket_count,
            uptime_seconds: operational.uptime_seconds,
            active_tasks: operational.active_tasks,
            health_check_failures: operational.health_check_failures,
            post_trade_relay_pending: operational.post_trade_relay_pending,
            async_writer_dropped: operational.async_writer_dropped,
            gamma_sync_duration_ms: operational.gamma_sync_duration_ms,
            gamma_markets_total: operational.gamma_markets_total,
            gamma_last_sync_success: operational.gamma_last_sync_success,
            gamma_markets_rejected: operational.gamma_markets_rejected,
            gamma_markets_paused: operational.gamma_markets_paused,
            catalog_ready: operational.catalog_ready,
            hotset_candidates_total: operational.hotset_candidates_total,
            hotset_selected_total: operational.hotset_selected_total,
            hotset_detection_window_coverage_ratio: operational
                .hotset_detection_window_coverage_ratio,
            metrics_refresh_failures: operational.metrics_refresh_failures,
            settlement_requests_total: operational.settlement_requests_total,
            settlement_positions_settled_total: operational.settlement_positions_settled_total,
            settlement_redeem_success_total: operational.settlement_redeem_success_total,
            settlement_redeem_failure_total: operational.settlement_redeem_failure_total,
            settlement_oracle_mismatch_total: operational.settlement_oracle_mismatch_total,
            settlement_channel_dropped_total: operational.settlement_channel_dropped_total,
            settlement_no_open_positions_total: operational.settlement_no_open_positions_total,
            settlement_duration_ms: operational.settlement_duration_ms,
            control_factor_refresh_total: operational.control_factor_refresh_total,
            control_factor_refresh_failures: operational.control_factor_refresh_failures,
            control_factor_version_changes: operational.control_factor_version_changes,
            control_factor_snapshot_load_age_seconds: operational
                .control_factor_snapshot_load_age_seconds,
            control_factor_active_count: operational.control_factor_active_count,
            control_factor_hard_rejects: operational.control_factor_hard_rejects,
            control_factor_validation_rejections: operational.control_factor_validation_rejections,
            control_factor_shadow_decisions: operational.control_factor_shadow_decisions,
            control_factor_shadow_dropped: operational.control_factor_shadow_dropped,
            control_factor_fail_closed_events: operational.control_factor_fail_closed_events,
            control_factor_publication_active: operational.control_factor_publication_active,
            shutdown_stage_progress_remaining: operational.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: operational.shutdown_stage_timeouts,
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
    ///
    /// Intended for the public `GET /metrics` scrape handler (see ng-gateway
    /// `gather_prometheus_text`); encodes every family registered on this hub's
    /// [`Registry`].
    pub fn gather_prometheus_text(&self) -> Result<(String, Vec<u8>), String> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(8 * 1024);
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|error| format!("prometheus encode failed: {error}"))?;
        Ok((encoder.format_type().to_string(), buffer))
    }

    /// Increment the labeled detection-funnel reject counter.
    pub fn record_scan_reject(&self, reason: DetectionRejectReason) {
        self.scan_rejects
            .with_label_values(&[reason.metric_label()])
            .inc();
    }

    /// Register and return the per-kind real-time event-bus drop counter.
    ///
    /// Call exactly once at wiring time. The returned vec shares this hub's
    /// registry, so `/metrics` exposes `oxide_arb_ws_event_dropped_total{kind}`;
    /// it is wired into the [`CoreEventPublisher`](oxide_arb_models::domain::CoreEventPublisher)
    /// drop hook so a full/disconnected bus is observable without a hub field.
    #[must_use]
    pub fn register_ws_event_dropped(&self) -> IntCounterVec {
        register_counter_vec!(
            &self.registry,
            "oxide_arb_ws_event_dropped_total",
            "Real-time CoreEvents dropped on a full/disconnected bus, by kind",
            &["kind"]
        )
    }
}

impl Default for MetricsHub {
    fn default() -> Self {
        Self::new()
    }
}
