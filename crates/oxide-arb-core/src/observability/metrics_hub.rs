use prometheus::{
    Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
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
    pub book_store_token_count: IntGauge,
    pub book_apply_dropped: IntCounter,
    pub book_apply_coalesced_total: IntCounter,
    pub backpressure_events: IntCounterVec,

    // Detection
    pub scans_gate_rejected: IntCounter,
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
    pub post_trade_dropped: IntCounter,
    pub post_trade_spilled_total: IntCounter,

    // Tiered execution
    pub tier_fills: IntCounterVec,
    pub tier_misses: IntCounterVec,

    // Latency segments (WS → scan → dispatch → HTTP)
    pub latency_ws_to_scan_us: Histogram,
    pub latency_scan_to_dispatch_us: Histogram,
    pub latency_tick_to_http_us: Histogram,

    // Kill switch
    pub fsm_emergency_entries: IntCounter,

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
    pub async_writer_dropped: IntCounterVec,

    // Gamma sync
    pub gamma_sync_duration_ms: IntGauge,
    pub gamma_markets_total: IntGauge,
    pub gamma_last_sync_success: IntGauge,

    // Metrics refresh
    pub metrics_refresh_failures: IntCounter,

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
    book_store_token_count: IntGauge,
    book_apply_dropped: IntCounter,
    book_apply_coalesced_total: IntCounter,
    backpressure_events: IntCounterVec,
}

struct DetectionMetrics {
    scans_gate_rejected: IntCounter,
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
    post_trade_dropped: IntCounter,
    post_trade_spilled_total: IntCounter,
    tier_fills: IntCounterVec,
    tier_misses: IntCounterVec,
    latency_ws_to_scan_us: Histogram,
    latency_scan_to_dispatch_us: Histogram,
    latency_tick_to_http_us: Histogram,
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
    exposure_gc_cleaned: IntCounter,
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
    async_writer_dropped: IntCounterVec,
    gamma_sync_duration_ms: IntGauge,
    gamma_markets_total: IntGauge,
    gamma_last_sync_success: IntGauge,
    metrics_refresh_failures: IntCounter,
    shutdown_stage_progress_remaining: IntGaugeVec,
    shutdown_stage_timeouts: IntCounterVec,
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
        post_trade_dropped: register_counter!(
            registry,
            "oxide_arb_execution_post_trade_dropped_total",
            "Post-trade jobs dropped when spill buffer fails"
        ),
        post_trade_spilled_total: register_counter!(
            registry,
            "oxide_arb_post_trade_spilled_total",
            "Post-trade jobs spilled to in-memory outbox when queue full"
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
        latency_ws_to_scan_us: latency.ws_to_scan,
        latency_scan_to_dispatch_us: latency.scan_to_dispatch,
        latency_tick_to_http_us: latency.tick_to_http,
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
            ws_events_dropped: pipeline.ws_events_dropped,
            book_level_rejected: pipeline.book_level_rejected,
            markets_resolved_ws: pipeline.markets_resolved_ws,
            shard_status_changes: pipeline.shard_status_changes,
            book_store_token_count: pipeline.book_store_token_count,
            book_apply_dropped: pipeline.book_apply_dropped,
            book_apply_coalesced_total: pipeline.book_apply_coalesced_total,
            backpressure_events: pipeline.backpressure_events,
            scans_gate_rejected: detection.scans_gate_rejected,
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
            post_trade_dropped: execution.post_trade_dropped,
            post_trade_spilled_total: execution.post_trade_spilled_total,
            tier_fills: execution.tier_fills,
            tier_misses: execution.tier_misses,
            latency_ws_to_scan_us: execution.latency_ws_to_scan_us,
            latency_scan_to_dispatch_us: execution.latency_scan_to_dispatch_us,
            latency_tick_to_http_us: execution.latency_tick_to_http_us,
            fsm_emergency_entries: execution.fsm_emergency_entries,
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
            cache_hits: cache.cache_hits,
            cache_misses: cache.cache_misses,
            uptime_seconds: system.uptime_seconds,
            active_tasks: system.active_tasks,
            health_check_failures: system.health_check_failures,
            outbox_pending: system.outbox_pending,
            outbox_flushed: system.outbox_flushed,
            outbox_dead_letters: system.outbox_dead_letters,
            async_writer_dropped: system.async_writer_dropped,
            gamma_sync_duration_ms: system.gamma_sync_duration_ms,
            gamma_markets_total: system.gamma_markets_total,
            gamma_last_sync_success: system.gamma_last_sync_success,
            metrics_refresh_failures: system.metrics_refresh_failures,
            shutdown_stage_progress_remaining: system.shutdown_stage_progress_remaining,
            shutdown_stage_timeouts: system.shutdown_stage_timeouts,
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
}

impl Default for MetricsHub {
    fn default() -> Self {
        Self::new()
    }
}
