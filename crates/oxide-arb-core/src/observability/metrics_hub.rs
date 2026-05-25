use prometheus::{
    Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
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

pub struct MetricsHub {
    pub registry: Registry,

    // Data Pipeline
    pub ws_events_received: IntCounter,
    pub book_snapshots_applied: IntCounter,
    pub price_changes_applied: IntCounter,
    pub ws_events_ignored: IntCounter,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub book_store_token_count: IntGauge,

    // Detection
    pub scans_gate_rejected: IntCounter,
    pub coalesced_scans: IntCounter,
    pub scan_results_total: Histogram,
    pub scan_duration_seconds: Histogram,
    pub opportunities_detected: IntCounter,

    // Funnel
    pub funnel_enqueued: IntCounter,
    pub funnel_dispatched: IntCounter,
    pub funnel_dropped: IntCounter,
    pub funnel_dispatch_age_ms: Histogram,
    pub funnel_queue_depth: IntGauge,

    // Execution
    pub execution_latency: Histogram,
    pub trades_filled: IntCounter,
    pub trades_missed: IntCounter,
    pub trades_failed: IntCounter,
    pub risk_denials: IntCounter,
    pub validation_failures: IntCounter,
    pub sizing_zero: IntCounter,
    pub reservation_failures: IntCounter,

    // Tiered execution
    pub tier_fills: IntCounterVec,
    pub tier_misses: IntCounterVec,

    // FSM
    pub fsm_transitions: IntCounterVec,
    pub fsm_invalid_transitions: IntCounter,
    pub fsm_emergency_entries: IntCounter,

    // Risk
    pub risk_checks_total: IntCounter,
    pub risk_exposure_usd: Gauge,
    pub risk_daily_pnl_usd: Gauge,
    pub risk_daily_loss_usd: Gauge,
    pub risk_weekly_loss_usd: Gauge,
    pub risk_reservations_active: IntGauge,
    pub risk_reservations_total_usd: Gauge,

    // Calibration
    pub calibration_update_total: IntCounter,
    pub calibration_resolved: IntCounter,
    pub calibration_bucket_count: IntGauge,

    // Cache
    pub cache_hits: IntCounterVec,
    pub cache_misses: IntCounterVec,

    // System
    pub uptime_seconds: IntGauge,
    pub active_tasks: IntGauge,
    pub health_check_failures: IntCounter,
    pub outbox_pending: IntGauge,
    pub outbox_flushed: IntCounter,
    pub outbox_dead_letters: IntCounter,

    // Gamma sync
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,

    // Metrics refresh
    pub metrics_refresh_failures: IntCounter,
}

struct PipelineMetrics {
    ws_events_received: IntCounter,
    book_snapshots_applied: IntCounter,
    price_changes_applied: IntCounter,
    ws_events_ignored: IntCounter,
    markets_resolved_ws: IntCounter,
    shard_status_changes: IntCounter,
    book_store_token_count: IntGauge,
}

struct DetectionMetrics {
    scans_gate_rejected: IntCounter,
    coalesced_scans: IntCounter,
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
}

struct ExecutionMetrics {
    execution_latency: Histogram,
    trades_filled: IntCounter,
    trades_missed: IntCounter,
    trades_failed: IntCounter,
    risk_denials: IntCounter,
    validation_failures: IntCounter,
    sizing_zero: IntCounter,
    reservation_failures: IntCounter,
    tier_fills: IntCounterVec,
    tier_misses: IntCounterVec,
    fsm_transitions: IntCounterVec,
    fsm_invalid_transitions: IntCounter,
    fsm_emergency_entries: IntCounter,
}

struct RiskMetrics {
    checks_total: IntCounter,
    exposure_usd: Gauge,
    daily_pnl_usd: Gauge,
    daily_loss_usd: Gauge,
    weekly_loss_usd: Gauge,
    reservations_active: IntGauge,
    reservations_total_usd: Gauge,
}

struct CalibrationMetrics {
    update_total: IntCounter,
    resolved: IntCounter,
    bucket_count: IntGauge,
}

struct CacheMetrics {
    cache_hits: IntCounterVec,
    cache_misses: IntCounterVec,
}

struct SystemMetrics {
    uptime_seconds: IntGauge,
    active_tasks: IntGauge,
    health_check_failures: IntCounter,
    outbox_pending: IntGauge,
    outbox_flushed: IntCounter,
    outbox_dead_letters: IntCounter,
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    metrics_refresh_failures: IntCounter,
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
        book_store_token_count: register_gauge_int!(
            registry,
            "oxide_arb_pipeline_book_store_token_count",
            "Tokens tracked in book store"
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
        coalesced_scans: register_counter!(
            registry,
            "oxide_arb_detection_coalesced_scans_total",
            "Scans coalesced"
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
    }
}

fn register_execution_metrics(registry: &Registry) -> ExecutionMetrics {
    ExecutionMetrics {
        execution_latency: register_histogram!(
            registry,
            "oxide_arb_execution_latency_seconds",
            "End-to-end execution latency",
            vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
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
        tier_fills: register_counter_vec!(
            registry,
            "oxide_arb_execution_tier_fills_total",
            "Fills by execution tier",
            &["tier"]
        ),
        tier_misses: register_counter_vec!(
            registry,
            "oxide_arb_execution_tier_misses_total",
            "Misses by execution tier",
            &["tier"]
        ),
        fsm_transitions: register_counter_vec!(
            registry,
            "oxide_arb_fsm_transitions_total",
            "FSM state transitions",
            &["from", "to"]
        ),
        fsm_invalid_transitions: register_counter!(
            registry,
            "oxide_arb_fsm_invalid_transitions_total",
            "Invalid FSM transitions attempted"
        ),
        fsm_emergency_entries: register_counter!(
            registry,
            "oxide_arb_fsm_emergency_entries_total",
            "Emergency state entries"
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

fn register_cache_metrics(registry: &Registry) -> CacheMetrics {
    CacheMetrics {
        cache_hits: register_counter_vec!(
            registry,
            "oxide_arb_cache_hits_total",
            "Cache hits by domain",
            &["domain"]
        ),
        cache_misses: register_counter_vec!(
            registry,
            "oxide_arb_cache_misses_total",
            "Cache misses by domain",
            &["domain"]
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
        outbox_pending: register_gauge_int!(
            registry,
            "oxide_arb_system_outbox_pending",
            "Pending outbox events"
        ),
        outbox_flushed: register_counter!(
            registry,
            "oxide_arb_system_outbox_flushed_total",
            "Outbox events flushed"
        ),
        outbox_dead_letters: register_counter!(
            registry,
            "oxide_arb_system_outbox_dead_letters_total",
            "Outbox dead-lettered events"
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
        metrics_refresh_failures: register_counter!(
            registry,
            "oxide_arb_system_metrics_refresh_failures_total",
            "Metrics refresh failures"
        ),
    }
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = Registry::new();
        let pipeline = register_pipeline_metrics(&registry);
        let detection = register_detection_metrics(&registry);
        let funnel = register_funnel_metrics(&registry);
        let execution = register_execution_metrics(&registry);
        let risk = register_risk_metrics(&registry);
        let calibration = register_calibration_metrics(&registry);
        let cache = register_cache_metrics(&registry);
        let system = register_system_metrics(&registry);

        Self {
            registry,
            ws_events_received: pipeline.ws_events_received,
            book_snapshots_applied: pipeline.book_snapshots_applied,
            price_changes_applied: pipeline.price_changes_applied,
            ws_events_ignored: pipeline.ws_events_ignored,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            book_store_token_count: pipeline.book_store_token_count,
            scans_gate_rejected: detection.scans_gate_rejected,
            coalesced_scans: detection.coalesced_scans,
            scan_results_total: detection.scan_results_total,
            scan_duration_seconds: detection.scan_duration_seconds,
            opportunities_detected: detection.opportunities_detected,
            funnel_enqueued: funnel.enqueued,
            funnel_dispatched: funnel.dispatched,
            funnel_dropped: funnel.dropped,
            funnel_dispatch_age_ms: funnel.dispatch_age_ms,
            funnel_queue_depth: funnel.queue_depth,
            execution_latency: execution.execution_latency,
            trades_filled: execution.trades_filled,
            trades_missed: execution.trades_missed,
            trades_failed: execution.trades_failed,
            risk_denials: execution.risk_denials,
            validation_failures: execution.validation_failures,
            sizing_zero: execution.sizing_zero,
            reservation_failures: execution.reservation_failures,
            tier_fills: execution.tier_fills,
            tier_misses: execution.tier_misses,
            fsm_transitions: execution.fsm_transitions,
            fsm_invalid_transitions: execution.fsm_invalid_transitions,
            fsm_emergency_entries: execution.fsm_emergency_entries,
            risk_checks_total: risk.checks_total,
            risk_exposure_usd: risk.exposure_usd,
            risk_daily_pnl_usd: risk.daily_pnl_usd,
            risk_daily_loss_usd: risk.daily_loss_usd,
            risk_weekly_loss_usd: risk.weekly_loss_usd,
            risk_reservations_active: risk.reservations_active,
            risk_reservations_total_usd: risk.reservations_total_usd,
            calibration_update_total: calibration.update_total,
            calibration_resolved: calibration.resolved,
            calibration_bucket_count: calibration.bucket_count,
            cache_hits: cache.cache_hits,
            cache_misses: cache.cache_misses,
            uptime_seconds: system.uptime_seconds,
            active_tasks: system.active_tasks,
            health_check_failures: system.health_check_failures,
            outbox_pending: system.outbox_pending,
            outbox_flushed: system.outbox_flushed,
            outbox_dead_letters: system.outbox_dead_letters,
            gamma_sync_duration_ms: system.gamma_sync_duration_ms,
            gamma_markets_total: system.gamma_markets_total,
            gamma_last_sync_success: system.gamma_last_sync_success,
            metrics_refresh_failures: system.metrics_refresh_failures,
        }
    }
}

impl Default for MetricsHub {
    fn default() -> Self {
        Self::new()
    }
}
