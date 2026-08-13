//! Deploy-config validation: detects infeasible values and credential-policy
//! violations that would cause silent failure at runtime.

use std::collections::BTreeSet;

use chrono_tz::Tz;
use quant_pivot_error::config_validation::{ConfigValidationError, ConfigValidationReport};
use rust_decimal_macros::dec;
use url::Url;
use zeroize::Zeroizing;

use super::{
    DeployConfig, DomainSourcesConfig, MAX_TRADE_TAPE_RECONCILIATION_ROWS, PolygonRpcEndpoint,
    ResearchJobsConfig, TornadoRegionScopeConfig, WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
    WeatherHistoricalBindingKind,
};
use crate::{
    constants::POLYGON_CHAIN_ID,
    enums::quant::{ExecutionWalletKind, QuantRuntimeMode},
    types::{HkoStation, IcaoStation},
};

impl DeployConfig {
    /// Mode-agnostic deploy validation: structural and platform invariants that
    /// must hold regardless of quant runtime mode.
    #[must_use]
    pub fn validate_deploy_common(&self) -> ConfigValidationReport {
        let mut report = ConfigValidationReport::default();

        if self.polymarket.chain_id != POLYGON_CHAIN_ID {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.chain_id",
                detail: format!("must be Polygon chain id {POLYGON_CHAIN_ID}"),
            });
        }
        if self.polymarket.order_post_timeout_ms < 35_000 {
            report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.order_post_timeout_ms",
            detail: "must be at least 35000 ms to contain the SDK's 30-second async-commit identity enrichment budget".into(),
        });
        }
        if self.polymarket.clob_market_info_refresh_secs < 60 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.clob_market_info_refresh_secs",
                detail: "must be at least 60 seconds".to_owned(),
            });
        }
        let settlement = &self.polymarket.settlement;
        if !(5..=300).contains(&settlement.claim_lease_secs) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.claim_lease_secs",
                detail: "must be between 5 and 300 seconds inclusive".to_owned(),
            });
        }
        if !(30..=3_600).contains(&settlement.semi_auto_authorization_ttl_secs) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.semi_auto_authorization_ttl_secs",
                detail: "must be between 30 and 3600 seconds inclusive".to_owned(),
            });
        }
        if settlement.discovery_poll_secs == 0 || settlement.submission_poll_secs == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.worker_poll_secs",
                detail: "discovery and submission polls must both be > 0".to_owned(),
            });
        }
        if !(1..=1_024).contains(&settlement.max_claims_per_tick) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.max_claims_per_tick",
                detail: "must be between 1 and 1024 inclusive".to_owned(),
            });
        }
        if !(1..=32).contains(&settlement.rpc_concurrency) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.rpc_concurrency",
                detail: "must be between 1 and 32 inclusive".to_owned(),
            });
        }
        if !(1..=60).contains(&settlement.readiness_ui_cache_secs) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.readiness_ui_cache_secs",
                detail: "must be between 1 and 60 seconds inclusive".to_owned(),
            });
        }
        if !(1..=10_000).contains(&settlement.external_scan_block_span) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.external_scan_block_span",
                detail: "must be between 1 and 10000 inclusive".to_owned(),
            });
        }
        if settlement.retry_initial_secs == 0
            || settlement.retry_initial_secs > settlement.retry_max_secs
            || settlement.retry_max_secs > 3_600
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.settlement.retry",
                detail: "initial must be > 0, not exceed max, and max must be <= 3600 seconds"
                    .to_owned(),
            });
        }
        validate_polygon_rpc(self, &mut report);

        if self.quant.workers.report_expire_sweep_secs == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.workers.report_expire_sweep_secs",
                detail: "must be > 0".into(),
            });
        }
        let workers = &self.quant.workers;
        if workers.report_schedule_poll_secs == 0
            || workers.report_run_lease_secs == 0
            || workers.report_run_heartbeat_secs == 0
            || workers.report_ad_hoc_queue_ttl_secs == 0
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.workers.report_lifecycle",
                detail: "poll, lease, heartbeat, and ad-hoc TTL must all be > 0".into(),
            });
        }
        if workers.report_run_heartbeat_secs > workers.report_run_lease_secs / 3 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.workers.report_run_heartbeat_secs",
                detail: "must be <= report_run_lease_secs / 3".into(),
            });
        }
        if workers.report_ad_hoc_queue_ttl_secs <= workers.report_schedule_poll_secs {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.workers.report_ad_hoc_queue_ttl_secs",
                detail: "must be greater than report_schedule_poll_secs".into(),
            });
        }
        if !(1..=1_024).contains(&workers.report_ad_hoc_queue_capacity) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.workers.report_ad_hoc_queue_capacity",
                detail: "must be between 1 and 1024 inclusive".into(),
            });
        }

        validate_portfolio_solver(self, &mut report);
        validate_research_jobs(self, &mut report);
        validate_databases(self, &mut report);
        validate_cache_redis(self, &mut report);
        validate_model_serving_registry(self, &mut report);
        validate_web(self, &mut report);

        if self.market_data.websocket.max_subscriptions_per_connection == 0
            || self.market_data.websocket.engine_max_subscription_tokens == 0
            || self.market_data.websocket.engine_subscription_window_hours == 0
        {
            report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.websocket",
            detail: "max_subscriptions_per_connection, engine_max_subscription_tokens, engine_subscription_window_hours must be > 0".into(),
        });
        }
        validate_market_data(self, &mut report);
        validate_domain_sources(self, &mut report);

        report
    }
}

fn validate_portfolio_solver(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let solver = &deploy.quant.portfolio_solver;
    if !(1..=600).contains(&solver.deadline_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.portfolio_solver.deadline_secs",
            detail: "global-planner deadline must be between 1 and 600 seconds inclusive"
                .to_owned(),
        });
    }
    if solver.threads != 1 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.portfolio_solver.threads",
            detail: "must be exactly 1 for deterministic serial HiGHS execution".to_owned(),
        });
    }
    if !(1..=50_000).contains(&solver.max_tiers) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.portfolio_solver.max_tiers",
            detail: "must be between 1 and 50000 inclusive".to_owned(),
        });
    }
    if !(3..=10_000).contains(&solver.max_scenarios) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.portfolio_solver.max_scenarios",
            detail: "must be between 3 and 10000 inclusive".to_owned(),
        });
    }
    if !(1..=1_000).contains(&solver.max_top_n) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.portfolio_solver.max_top_n",
            detail: "must be between 1 and 1000 inclusive".to_owned(),
        });
    }
}

fn validate_research_jobs(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let jobs = &deploy.quant.research_jobs;
    if !(1..=32).contains(&jobs.global_concurrency) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.global_concurrency",
            detail: "must be between 1 and 32 inclusive".to_owned(),
        });
    }
    let kind_caps = [
        ("dataset_build_concurrency", jobs.dataset_build_concurrency),
        ("model_train_concurrency", jobs.model_train_concurrency),
        ("backtest_concurrency", jobs.backtest_concurrency),
        (
            "bias_table_fit_concurrency",
            jobs.bias_table_fit_concurrency,
        ),
        (
            "model_calibration_fit_concurrency",
            jobs.model_calibration_fit_concurrency,
        ),
        ("cpcv_backtest_concurrency", jobs.cpcv_backtest_concurrency),
        (
            "feature_parity_concurrency",
            jobs.feature_parity_concurrency,
        ),
        (
            "feedback_coverage_concurrency",
            jobs.feedback_coverage_concurrency,
        ),
        (
            "feedback_drift_concurrency",
            jobs.feedback_drift_concurrency,
        ),
        (
            "feedback_learning_concurrency",
            jobs.feedback_learning_concurrency,
        ),
        (
            "trade_policy_fit_concurrency",
            jobs.trade_policy_fit_concurrency,
        ),
        (
            "trade_policy_validation_concurrency",
            jobs.trade_policy_validation_concurrency,
        ),
    ];
    for (field, cap) in kind_caps {
        if cap == 0 || cap > jobs.global_concurrency {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "quant.research_jobs.kind_concurrency",
                detail: format!("{field} must be positive and no greater than global_concurrency"),
            });
        }
    }
    validate_compute_budget(jobs, report);
    if jobs.feedback_cycle_concurrency == 0
        || jobs.feedback_cycle_concurrency > jobs.global_concurrency
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_cycle_concurrency",
            detail: "must be positive and no greater than global_concurrency".to_owned(),
        });
    }
    if !(3..=3_600).contains(&jobs.lease_ttl_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.lease_ttl_secs",
            detail: "must be between 3 and 3600 seconds inclusive".to_owned(),
        });
    }
    let heartbeat_valid = jobs.lease_ttl_secs.try_into().is_ok_and(|lease_ttl: u64| {
        jobs.heartbeat_secs > 0 && jobs.heartbeat_secs <= lease_ttl / 3
    });
    if !heartbeat_valid {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.heartbeat_secs",
            detail: "must be positive and no greater than lease_ttl_secs / 3".to_owned(),
        });
    }
    if !(1..=300).contains(&jobs.poll_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.poll_secs",
            detail: "must be between 1 and 300 seconds inclusive".to_owned(),
        });
    }
    if !(0..=32).contains(&jobs.max_recovery_attempts) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.max_recovery_attempts",
            detail: "must be between 0 and 32 inclusive".to_owned(),
        });
    }
    if jobs.execution_retry_initial_secs == 0
        || jobs.execution_retry_initial_secs > jobs.execution_retry_max_secs
        || jobs.execution_retry_max_secs > 3_600
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.execution_retry",
            detail: "initial delay must be positive, no greater than max delay, and max delay must be <= 3600 seconds"
                .to_owned(),
        });
    }
    if jobs.max_spine_samples == 0 || jobs.max_spine_samples > 100_000_000 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.max_spine_samples",
            detail: "must be between 1 and 100000000 inclusive".to_owned(),
        });
    }
    if jobs.plan_sample_slices > 64 || !(1..=10_000).contains(&jobs.plan_sample_markets) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.plan_sample_budget",
            detail: "plan_sample_slices must be <= 64 and plan_sample_markets within 1..=10000"
                .to_owned(),
        });
    }
    if !(50..=60_000).contains(&jobs.progress_min_interval_ms) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.progress_min_interval_ms",
            detail: "must be between 50 and 60000 ms inclusive".to_owned(),
        });
    }
    let stuck_valid = jobs.lease_ttl_secs.try_into().is_ok_and(|lease_ttl: u64| {
        jobs.feedback_stuck_secs > lease_ttl && jobs.feedback_stuck_secs <= 2_592_000
    });
    if !stuck_valid {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_stuck_secs",
            detail: "must be greater than lease_ttl_secs and no greater than 30 days".to_owned(),
        });
    }
    if !(1..=jobs.shutdown_drain_secs).contains(&jobs.feedback_alert_timeout_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_alert_timeout_secs",
            detail: "must be positive and no greater than shutdown_drain_secs".to_owned(),
        });
    }
    if jobs.feedback_alert_dedupe_secs == 0
        || jobs.feedback_alert_dedupe_secs < jobs.feedback_alert_timeout_secs
        || jobs.feedback_alert_dedupe_secs > 86_400
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_alert_dedupe_secs",
            detail: "must be at least the alert timeout and no greater than one day".to_owned(),
        });
    }
    if !(1..=3).contains(&jobs.shutdown_drain_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.shutdown_drain_secs",
            detail: "must be between 1 and 3 seconds inclusive".to_owned(),
        });
    }
}

fn validate_compute_budget(jobs: &ResearchJobsConfig, report: &mut ConfigValidationReport) {
    let parity = jobs.feature_parity_compute;
    if !(1..=1_000).contains(&parity.page_size) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feature_parity_compute.page_size",
            detail: "must be between 1 and 1000 subjects inclusive".to_owned(),
        });
    }
    if parity.max_concurrency != 1 || parity.max_concurrency > jobs.feature_parity_concurrency {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feature_parity_compute.max_concurrency",
            detail: "must be exactly one and no greater than feature_parity_concurrency".to_owned(),
        });
    }
    if !(1_048_576..=10_737_418_240).contains(&parity.max_working_set_bytes) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feature_parity_compute.max_working_set_bytes",
            detail: "must be between 1 MiB and the 10 GiB process offline-memory budget".to_owned(),
        });
    }
    if !(1..=86_400).contains(&parity.deadline_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feature_parity_compute.deadline_secs",
            detail: "must be between 1 second and 24 hours inclusive".to_owned(),
        });
    }
    let attribution = jobs.feedback_attribution_compute;
    if !(1..=1_000).contains(&attribution.page_size) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_attribution_compute.page_size",
            detail: "must be between 1 and 1000 rows inclusive".to_owned(),
        });
    }
    if !(1..=32).contains(&attribution.max_concurrency) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_attribution_compute.max_concurrency",
            detail: "must be between 1 and 32 in-flight attribution groups inclusive".to_owned(),
        });
    }
    if !(1_048_576..=10_737_418_240).contains(&attribution.max_working_set_bytes) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_attribution_compute.max_working_set_bytes",
            detail: "must be between 1 MiB and the 10 GiB process offline-memory budget".to_owned(),
        });
    }
    if !(1..=86_400).contains(&attribution.deadline_secs) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.research_jobs.feedback_attribution_compute.deadline_secs",
            detail: "must be between 1 second and 24 hours inclusive".to_owned(),
        });
    }
}

fn validate_polygon_rpc(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let endpoint = &deploy.polymarket.onchain.rpc_endpoint;
    let parsed = Url::parse(endpoint.resolved_url()).ok();
    let structurally_valid = parsed
        .as_ref()
        .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some());
    let public_endpoint_is_non_secret = match (endpoint, parsed.as_ref()) {
        (PolygonRpcEndpoint::Public { .. }, Some(url)) => {
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.path(), "" | "/")
        }
        (PolygonRpcEndpoint::Public { .. }, None) => false,
        (PolygonRpcEndpoint::Protected { .. }, _) => true,
    };
    if !structurally_valid || !public_endpoint_is_non_secret {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.onchain.rpc_endpoint",
            detail: "must be an HTTP(S) URL; public endpoints cannot contain user-info, path credentials, query parameters, or fragments, and authenticated URLs must use a protected deploy-secret source".to_owned(),
        });
    }
    if deploy.polymarket.onchain.rpc_timeout_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.onchain.rpc_timeout_ms",
            detail: "must be > 0 so Polygon reads and transactions cannot hang indefinitely"
                .to_owned(),
        });
    }
}

fn validate_model_serving_registry(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let registry = deploy.research.model_serving_registry;
    if !(1..=1_024).contains(&registry.max_cached_contracts) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.model_serving_registry.max_cached_contracts",
            detail: "must be between 1 and 1024 inclusive".to_owned(),
        });
    }
    if !(1..=64).contains(&registry.max_concurrent_loads) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.model_serving_registry.max_concurrent_loads",
            detail: "must be between 1 and 64 inclusive".to_owned(),
        });
    }
    if registry.max_pending_loads < registry.max_concurrent_loads
        || registry.max_pending_loads > 4_096
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.model_serving_registry.max_pending_loads",
            detail: "must be >= max_concurrent_loads and <= 4096".to_owned(),
        });
    }
    if !(1_000..=300_000).contains(&registry.load_timeout_ms) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.model_serving_registry.load_timeout_ms",
            detail: "must be between 1000 and 300000 ms inclusive".to_owned(),
        });
    }
    if !(67_108_864..=1_099_511_627_776).contains(&registry.max_total_shadow_model_bytes) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "research.model_serving_registry.max_total_shadow_model_bytes",
            detail: "must be between 64 MiB and 1 TiB inclusive".to_owned(),
        });
    }
}

fn validate_web(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let password_crypto = &deploy.web.password_crypto;
    if !(1..=64).contains(&password_crypto.max_in_flight) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "web.password_crypto.max_in_flight",
            detail: "must be between 1 and 64 inclusive".to_owned(),
        });
    }
    if !(1_000..=60_000).contains(&password_crypto.deadline_ms) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "web.password_crypto.deadline_ms",
            detail: "must be between 1000 and 60000 ms inclusive".to_owned(),
        });
    }
    let jwt = &deploy.web.jwt;
    if jwt.issuer.trim().is_empty() || jwt.audience.trim().is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "web.jwt",
            detail: "issuer and audience must be non-empty".to_owned(),
        });
    }
    if jwt.access_ttl_secs <= 0
        || jwt.refresh_ttl_secs <= 0
        || jwt.absolute_session_ttl_secs <= 0
        || jwt.access_ttl_secs > jwt.absolute_session_ttl_secs
        || jwt.refresh_ttl_secs > jwt.absolute_session_ttl_secs
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "web.jwt",
            detail: "token TTLs must be positive and no greater than absolute_session_ttl_secs"
                .to_owned(),
        });
    }
    let evidence_key = &deploy.research.evidence_attestation.signing_key;
    if !evidence_key.is_empty() {
        let mut evidence_bytes = Zeroizing::new([0_u8; 32]);
        if let Ok(jwt_bytes) = jwt.signing_key_bytes()
            && hex::decode_to_slice(evidence_key.expose_secret(), evidence_bytes.as_mut()).is_ok()
            && jwt_bytes.as_ref() == evidence_bytes.as_ref()
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "research.evidence_attestation.signing_key",
                detail: "must be cryptographically independent from web.jwt.signing_key".to_owned(),
            });
        }
    }
}

fn validate_databases(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let postgres = &deploy.db.postgres;
    if postgres.max_connections == 0 || postgres.min_connections > postgres.max_connections {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.postgres",
            detail: "max_connections must be > 0 and >= min_connections".into(),
        });
    }

    let clickhouse = &deploy.db.clickhouse;
    if clickhouse.batch_size == 0
        || clickhouse.flush_interval_secs == 0
        || clickhouse.max_concurrent_inserts == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse",
            detail: "batch_size, flush_interval_secs, max_concurrent_inserts must be > 0".into(),
        });
    }
    if postgres.user.trim().is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.postgres.user",
            detail: "database user must not be empty".into(),
        });
    }
    if clickhouse.user.trim().is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse.user",
            detail: "database user must not be empty".into(),
        });
    }
}

fn validate_domain_sources(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let sources = &deploy.domain_sources;
    if sources.binance.enabled
        && (sources.binance.request_timeout_ms == 0
            || sources.binance.agg_trade_recovery_poll_secs == 0
            || sources.binance.websocket_rotation_secs == 0
            || sources.binance.batch_size == 0
            || sources.binance.max_clock_skew_ms == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.binance",
            detail: "request_timeout_ms, agg_trade_recovery_poll_secs, websocket_rotation_secs, batch_size, and max_clock_skew_ms must be > 0".into(),
        });
    }
    if sources.binance_usdm_futures.enabled
        && (sources.binance_usdm_futures.request_timeout_ms == 0
            || sources.binance_usdm_futures.agg_trade_recovery_poll_secs == 0
            || sources.binance_usdm_futures.websocket_rotation_secs == 0
            || sources.binance_usdm_futures.batch_size == 0
            || sources.binance_usdm_futures.max_clock_skew_ms == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.binance_usdm_futures",
            detail: "request_timeout_ms, agg_trade_recovery_poll_secs, websocket_rotation_secs, batch_size, and max_clock_skew_ms must be > 0".into(),
        });
    }
    if sources.polymarket_rtds.enabled {
        let valid_url = Url::parse(&sources.polymarket_rtds.websocket_url)
            .ok()
            .is_some_and(|url| matches!(url.scheme(), "ws" | "wss") && url.host_str().is_some());
        if !valid_url
            || sources.polymarket_rtds.connect_timeout_ms == 0
            || sources.polymarket_rtds.keepalive_secs != 5
            || sources.polymarket_rtds.max_clock_skew_ms == 0
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.polymarket_rtds",
                detail: "websocket_url must be ws/wss, connect/max skew must be > 0, and official keepalive cadence must equal 5 seconds".into(),
            });
        }
    }
    if sources.chainlink_data_streams.enabled {
        let credentials_present = sources.chainlink_data_streams.api_key.is_some()
            && sources.chainlink_data_streams.api_secret.is_some();
        if !credentials_present || sources.chainlink_data_streams.feeds.is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.chainlink_data_streams",
                detail: "enabled source requires API credentials and at least one frozen V3 feed"
                    .into(),
            });
        }
        if sources.chainlink_data_streams.max_clock_skew_ms == 0
            || sources.chainlink_data_streams.rest_page_limit == 0
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.chainlink_data_streams",
                detail: "max_clock_skew_ms and rest_page_limit must be > 0".into(),
            });
        }
    }
    if sources.aviation_weather.enabled
        && (sources.aviation_weather.poll_secs == 0
            || sources.aviation_weather.request_timeout_ms == 0
            || sources.aviation_weather.day_close_grace_secs
                != WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.aviation_weather",
            detail: format!(
                "poll_secs and request_timeout_ms must be > 0; day_close_grace_secs must equal governed methodology value {WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS}"
            ),
        });
    }
    if sources.ghcnh.enabled
        && (!valid_http_url(&sources.ghcnh.base_url)
            || sources.ghcnh.request_timeout_ms == 0
            || sources.ghcnh.refresh_secs == 0
            || sources.ghcnh.calibration_years == 0
            || !(1..=8).contains(&sources.ghcnh.max_concurrency))
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.ghcnh",
            detail: "request_timeout_ms, refresh_secs, and calibration_years must be > 0; max_concurrency must be between 1 and 8"
                .into(),
        });
    }
    if sources.ghcnd.enabled
        && (!valid_http_url(&sources.ghcnd.base_url)
            || sources.ghcnd.request_timeout_ms == 0
            || sources.ghcnd.refresh_secs == 0
            || sources.ghcnd.lookback_years == 0
            || !(1..=8).contains(&sources.ghcnd.max_concurrency))
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.ghcnd",
            detail: "base_url must be HTTP(S); request_timeout_ms, refresh_secs, and lookback_years must be > 0; max_concurrency must be between 1 and 8"
                .into(),
        });
    }
    if sources.gefs.enabled
        && (sources.gefs.request_timeout_ms == 0
            || sources.gefs.poll_secs == 0
            || sources.gefs.publication_lag_secs == 0
            || !(3..=240).contains(&sources.gefs.max_lead_hours)
            || !sources.gefs.max_lead_hours.is_multiple_of(3)
            || sources.gefs.max_concurrency == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.gefs",
            detail: "timeouts/cadences/concurrency must be > 0 and max_lead_hours must be a 3-hour step in 3..=240".into(),
        });
    }
    validate_public_weather_sources(sources, report);
    validate_weather_vertical_bindings(deploy, report);
    validate_weather_stations(sources, report);
}

fn validate_weather_stations(sources: &DomainSourcesConfig, report: &mut ConfigValidationReport) {
    for (station, profile) in &sources.weather_stations {
        let historical_binding_valid = match profile.historical_binding_kind {
            WeatherHistoricalBindingKind::ExactStation => {
                profile
                    .ghcnh_station_id
                    .as_deref()
                    .is_some_and(valid_ncei_station_id)
                    && profile
                        .ghcnd_station_id
                        .as_deref()
                        .is_some_and(valid_ncei_station_id)
            }
            WeatherHistoricalBindingKind::OfficialNearbyProxy => {
                profile
                    .ghcnh_station_id
                    .as_deref()
                    .is_some_and(valid_ncei_station_id)
                    && profile.ghcnd_station_id.is_none()
            }
            WeatherHistoricalBindingKind::Unavailable => {
                profile.ghcnh_station_id.is_none() && profile.ghcnd_station_id.is_none()
            }
        };
        if IcaoStation::parse(station).is_err()
            || profile.timezone.parse::<Tz>().is_err()
            || profile.latitude < dec!(-90)
            || profile.latitude > dec!(90)
            || profile.longitude < dec!(-180)
            || profile.longitude > dec!(180)
            || !historical_binding_valid
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.weather_stations",
                detail: format!(
                    "station `{station}` must have a valid ICAO id, IANA timezone, coordinates, and binding-kind-consistent official GHCNh/GHCNd identities"
                ),
            });
        }
    }
}

fn valid_ncei_station_id(value: &str) -> bool {
    value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

fn validate_public_weather_sources(
    sources: &DomainSourcesConfig,
    report: &mut ConfigValidationReport,
) {
    if sources.hko_open_data.enabled
        && (!valid_http_url(&sources.hko_open_data.base_url)
            || sources.hko_open_data.request_timeout_ms == 0
            || sources.hko_open_data.daily_rainfall_poll_secs == 0
            || !(1..=366).contains(&sources.hko_open_data.daily_rainfall_lookback_days)
            || sources.hko_open_data.daily_temperature_poll_secs == 0
            || !(1..=120).contains(&sources.hko_open_data.daily_temperature_lookback_months))
    {
        invalid_domain_source(
            report,
            "domain_sources.hko_open_data",
            "base_url must be HTTP(S), timeout/rainfall/daily-temperature cadences must be > 0, daily_rainfall_lookback_days must be in 1..=366, and daily_temperature_lookback_months must be in 1..=120",
        );
    }
    if sources.airnow.enabled
        && (!valid_http_url(&sources.airnow.reporting_area_url)
            || !valid_http_url(&sources.airnow.hourly_aq_base_url)
            || sources.airnow.request_timeout_ms == 0
            || sources.airnow.poll_secs == 0
            || !(1..=168).contains(&sources.airnow.correction_lookback_hours))
    {
        invalid_domain_source(
            report,
            "domain_sources.airnow",
            "URLs must be HTTP(S), timeout/poll cadence must be > 0, and correction_lookback_hours must be in 1..=168",
        );
    }
    if sources.tornado.enabled
        && (!valid_http_url(&sources.tornado.spc_base_url)
            || !valid_http_url(&sources.tornado.ncei_csv_base_url)
            || !valid_http_url(&sources.tornado.ncei_time_series_base_url)
            || sources.tornado.request_timeout_ms == 0
            || sources.tornado.spc_poll_secs == 0
            || sources.tornado.ncei_refresh_secs == 0
            || sources.tornado.ncei_time_series_poll_secs == 0
            || !(1..=10).contains(&sources.tornado.ncei_backfill_years))
    {
        invalid_domain_source(
            report,
            "domain_sources.tornado",
            "URLs must be HTTP(S), timeout/refresh/poll cadences must be > 0, and ncei_backfill_years must be in 1..=10",
        );
    }
    if sources.nhc.enabled
        && (!valid_http_url(&sources.nhc.current_storms_url)
            || !valid_http_url(&sources.nhc.data_archive_url)
            || sources.nhc.request_timeout_ms == 0
            || sources.nhc.advisory_poll_secs == 0
            || sources.nhc.best_track_refresh_secs == 0)
    {
        invalid_domain_source(
            report,
            "domain_sources.nhc",
            "URLs must be HTTP(S), and timeout/refresh cadences must be > 0",
        );
    }
    if sources.nasa_gistemp.enabled
        && (!valid_http_url(&sources.nasa_gistemp.csv_url)
            || !valid_http_url(&sources.nasa_gistemp.annual_url)
            || sources.nasa_gistemp.request_timeout_ms == 0
            || sources.nasa_gistemp.refresh_secs == 0)
    {
        invalid_domain_source(
            report,
            "domain_sources.nasa_gistemp",
            "csv_url must be HTTP(S), and timeout/refresh cadence must be > 0",
        );
    }
    if sources.nsidc_sea_ice.enabled
        && (!valid_http_url(&sources.nsidc_sea_ice.north_daily_csv_url)
            || !valid_http_url(&sources.nsidc_sea_ice.south_daily_csv_url)
            || !valid_http_url(&sources.nsidc_sea_ice.north_monthly_base_url)
            || !valid_http_url(&sources.nsidc_sea_ice.south_monthly_base_url)
            || sources.nsidc_sea_ice.request_timeout_ms == 0
            || sources.nsidc_sea_ice.refresh_secs == 0)
    {
        invalid_domain_source(
            report,
            "domain_sources.nsidc_sea_ice",
            "daily and monthly hemisphere URLs must be HTTP(S), and timeout/refresh cadence must be > 0",
        );
    }
    if sources.nws_observation.enabled
        && (!valid_http_url(&sources.nws_observation.base_url)
            || sources.nws_observation.request_timeout_ms == 0
            || sources.nws_observation.poll_secs == 0
            || !(1..=500).contains(&sources.nws_observation.lookback_observations))
    {
        invalid_domain_source(
            report,
            "domain_sources.nws_observation",
            "base_url must be HTTP(S), timeout/poll cadence must be > 0, and lookback_observations must be in 1..=500",
        );
    }
}

fn valid_http_url(value: &str) -> bool {
    Url::parse(value)
        .ok()
        .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
}

fn invalid_domain_source(
    report: &mut ConfigValidationReport,
    field: &'static str,
    detail: &'static str,
) {
    report.errors.push(ConfigValidationError::InvalidValue {
        field,
        detail: detail.to_owned(),
    });
}

fn validate_weather_vertical_bindings(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let bindings = &deploy.domain_sources.weather_vertical_bindings;
    let mut identities = BTreeSet::new();
    for binding in &bindings.hko_rainfall {
        let station_segment = format!("/{}/", binding.station_key);
        let coordinate_valid = binding.latitude >= dec!(-90)
            && binding.latitude <= dec!(90)
            && binding.longitude >= dec!(-180)
            && binding.longitude <= dec!(180);
        let valid = printable_binding(&binding.site_key, 128)
            && HkoStation::parse(&binding.station_key).is_ok()
            && valid_http_url(&binding.daily_csv_url)
            && binding.daily_csv_url.contains(&station_segment)
            && coordinate_valid
            && binding.timezone == "Asia/Hong_Kong"
            && identities.insert(format!("HKO:{}:RAIN", binding.station_key));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.hko_rainfall",
                "binding must carry a unique HKO station, matching official HTTP(S) daily CSV, printable site, valid coordinates, and Asia/Hong_Kong timezone",
            );
        }
    }
    for binding in &bindings.hko_daily_temperature {
        let valid = HkoStation::parse(&binding.station).is_ok()
            && binding.timezone == "Asia/Hong_Kong"
            && identities.insert(format!("HKO:{}:TEMP", binding.station));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.hko_daily_temperature",
                "station must be a unique source-native HKO code and timezone must be Asia/Hong_Kong",
            );
        }
    }
    for binding in &bindings.airnow_pm25_reporting_areas {
        let state_valid =
            binding.state.len() == 2 && binding.state.bytes().all(|byte| byte.is_ascii_uppercase());
        let valid = printable_binding(&binding.area, 128)
            && state_valid
            && binding.timezone.parse::<Tz>().is_ok()
            && identities.insert(format!("AIRNOW:{}:{}", binding.state, binding.area));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.airnow_pm25_reporting_areas",
                "area/state must form a unique printable binding and timezone must be an IANA identifier",
            );
        }
    }
    for binding in &bindings.airnow_pm25_sites {
        let state_valid =
            binding.state.len() == 2 && binding.state.bytes().all(|byte| byte.is_ascii_uppercase());
        let aqsid_valid =
            binding.aqsid.len() == 12 && binding.aqsid.bytes().all(|byte| byte.is_ascii_digit());
        let coordinate_valid = binding.latitude >= dec!(-90)
            && binding.latitude <= dec!(90)
            && binding.longitude >= dec!(-180)
            && binding.longitude <= dec!(180);
        let valid = printable_binding(&binding.contract_location, 128)
            && printable_binding(&binding.site_name, 128)
            && valid_http_url(&binding.primary_resolution_url)
            && aqsid_valid
            && state_valid
            && coordinate_valid
            && binding.timezone.parse::<Tz>().is_ok()
            && identities.insert(format!("AIRNOW_SITE:{}:PM25_AQI", binding.aqsid));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.airnow_pm25_sites",
                "binding must carry a unique 12-digit full AQSID, printable location/site, HTTP(S) resolution URL, valid state/coordinates, and IANA timezone",
            );
        }
    }
    for binding in &bindings.tornado_regions {
        let scope_valid = match &binding.scope {
            TornadoRegionScopeConfig::UnitedStates => binding.region_id == "united_states",
            TornadoRegionScopeConfig::State {
                spc_state_code,
                ncei_state_name,
            } => {
                spc_state_code.len() == 2
                    && spc_state_code.bytes().all(|byte| byte.is_ascii_uppercase())
                    && printable_binding(ncei_state_name, 128)
            }
        };
        let valid = printable_binding(&binding.region_id, 64)
            && scope_valid
            && binding.timezone.parse::<Tz>().is_ok()
            && identities.insert(format!("TORNADO:{}", binding.region_id));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.tornado_regions",
                "scope must be a unique national or valid state binding and timezone must be an IANA identifier",
            );
        }
    }
    for binding in &bindings.nhc_historical_storms {
        let prefix = match binding.basin.as_str() {
            "atlantic" => Some("AL"),
            "eastern_pacific" => Some("EP"),
            "central_pacific" => Some("CP"),
            _ => None,
        };
        let storm_valid = binding.storm_id.len() == 8
            && binding
                .storm_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
            && prefix.is_some_and(|prefix| binding.storm_id.starts_with(prefix));
        if !storm_valid
            || !identities.insert(format!("HURDAT2:{}:{}", binding.basin, binding.storm_id))
        {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.nhc_historical_storms",
                "basin/storm_id must be a unique supported HURDAT2 binding",
            );
        }
    }
    for binding in &bindings.nws_wind_stations {
        let valid = IcaoStation::parse(&binding.station).is_ok()
            && binding.timezone.parse::<Tz>().is_ok()
            && identities.insert(format!("NWS:{}", binding.station));
        if !valid {
            invalid_domain_source(
                report,
                "domain_sources.weather_vertical_bindings.nws_wind_stations",
                "station must be a unique ICAO id and timezone must be an IANA identifier",
            );
        }
    }
}

fn printable_binding(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn validate_market_data(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    if deploy.market_data.gamma.page_size == 0
        || deploy.market_data.gamma.reconcile_interval_secs == 0
        || deploy.market_data.gamma.max_keyset_pages == 0
        || deploy.market_data.gamma.max_keyset_requests == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma",
            detail: "page_size, reconcile_interval_secs, max_keyset_pages, and max_keyset_requests must be > 0".into(),
        });
    }
    if deploy.market_data.gamma.max_keyset_requests < deploy.market_data.gamma.max_keyset_pages {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma.max_keyset_requests",
            detail: "must be >= max_keyset_pages".into(),
        });
    }
    if deploy.market_data.gamma.page_size > 500 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma.page_size",
            detail: "must be <= 500 (Gamma /events/keyset limit)".into(),
        });
    }
    let trade_tape = &deploy.market_data.trade_tape_on_chain;
    if trade_tape.max_blocks_per_tick == 0
        || trade_tape.max_blocks_per_request == 0
        || trade_tape.batch_size == 0
        || trade_tape.reconciliation_match_window_ms == 0
        || trade_tape.reconciliation_terminal_age_secs == 0
        || trade_tape.reconciliation_max_rows == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.trade_tape_on_chain",
            detail: "block, batch, match-window, terminal-age, and row limits must be > 0".into(),
        });
    }
    if trade_tape.reconciliation_lookback_secs <= trade_tape.reconciliation_terminal_age_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.trade_tape_on_chain.reconciliation_lookback_secs",
            detail: "must exceed reconciliation_terminal_age_secs".into(),
        });
    }
    if trade_tape.reconciliation_max_rows > MAX_TRADE_TAPE_RECONCILIATION_ROWS {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.trade_tape_on_chain.reconciliation_max_rows",
            detail: format!(
                "must be <= {MAX_TRADE_TAPE_RECONCILIATION_ROWS} (native SQL result budget)"
            ),
        });
    }
}

fn validate_cache_redis(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let redis = &deploy.cache.redis;
    if redis.host.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.host",
            detail: "must not be empty".into(),
        });
    }
    if redis.port == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.port",
            detail: "must be > 0".into(),
        });
    }
    if redis.pool_size == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.pool_size",
            detail: "must be > 0".into(),
        });
    }
    if redis.try_connection_url().is_err() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis",
            detail: "host/port/user/password produce an invalid connection URL".into(),
        });
    }
}

/// Quant-mode-aware deploy validation: enforces credential and JWT policies.
#[must_use]
pub fn validate_deploy_mode(
    deploy: &DeployConfig,
    mode: QuantRuntimeMode,
) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_credentials_quant_mode(deploy, mode, &mut report);
    validate_web_quant_mode(deploy, mode, &mut report);
    report
}

/// Credential policy (all modes): `report_only` is **not** dry-run — report
/// sizing is built on the real venue account, so the private key (CLOB
/// collateral / L2 read credential) and the funder (Data API position reads) are
/// required in every mode. Missing either fails closed. The private key is used
/// only for reads here; signing/submission gating stays mode-aware elsewhere.
/// EOA auto-redeem additionally checks signer/funder equality during
/// CTF worker assembly, where the signer address is available.
fn validate_credentials_quant_mode(
    deploy: &DeployConfig,
    mode: QuantRuntimeMode,
    report: &mut ConfigValidationReport,
) {
    let mut missing = Vec::new();
    if !deploy.keys.private_key_present() {
        missing.push("private_key");
    }
    let funder_present = deploy
        .quant
        .account
        .funder
        .as_deref()
        .is_some_and(|funder| !funder.trim().is_empty());
    if !funder_present {
        missing.push("quant.account.funder");
    }
    if !missing.is_empty() {
        report
            .errors
            .push(ConfigValidationError::MissingCredentials {
                mode: mode.to_string(),
                missing,
            });
    }

    // Every contract-wallet topology moves money via the gasless relayer, so
    // relayer API credentials are mandatory once order submission is allowed
    // (SemiAuto / AutoExecution). ReportOnly never redeems, so it is exempt.
    if mode.allows_order_submission()
        && matches!(
            deploy.quant.account.wallet_kind,
            ExecutionWalletKind::Proxy
                | ExecutionWalletKind::GnosisSafe
                | ExecutionWalletKind::DepositWallet
        )
    {
        let mut relayer_missing = Vec::new();
        if deploy.polymarket.relayer.api_key().is_none() {
            relayer_missing.push("polymarket.relayer.api_key");
        }
        if deploy.polymarket.relayer.api_key_address().is_none() {
            relayer_missing.push("polymarket.relayer.api_key_address");
        }
        if !relayer_missing.is_empty() {
            report
                .errors
                .push(ConfigValidationError::MissingCredentials {
                    mode: mode.to_string(),
                    missing: relayer_missing,
                });
        }
    }
}

fn validate_web_quant_mode(
    deploy: &DeployConfig,
    _mode: QuantRuntimeMode,
    report: &mut ConfigValidationReport,
) {
    if deploy.web.has_jwt_signing_key() {
        return;
    }
    report.errors.push(ConfigValidationError::InvalidValue {
        field: "web.jwt",
        detail: "authentication requires a Base64URL-no-pad encoded 32-byte HS256 signing key"
            .to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ModelServingRegistryConfig;

    #[test]
    fn common_validation_passes_defaults() {
        let deploy = DeployConfig::default();
        let report = deploy.validate_deploy_common();
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn research_job_budgets_bounded() {
        let mut deploy = DeployConfig::default();
        let jobs = &mut deploy.quant.research_jobs;
        jobs.global_concurrency = 0;
        jobs.feedback_cycle_concurrency = 0;
        jobs.heartbeat_secs = jobs.lease_ttl_secs.unsigned_abs();
        jobs.feedback_stuck_secs = 1;
        jobs.feedback_alert_timeout_secs = 0;
        jobs.feedback_alert_dedupe_secs = 0;
        jobs.shutdown_drain_secs = 5;
        jobs.feature_parity_compute.page_size = 0;
        jobs.feature_parity_compute.max_concurrency = 0;
        jobs.feature_parity_compute.max_working_set_bytes = 0;
        jobs.feature_parity_compute.deadline_secs = 0;
        jobs.feedback_attribution_compute.page_size = 0;
        jobs.feedback_attribution_compute.max_concurrency = 0;
        jobs.feedback_attribution_compute.max_working_set_bytes = 0;
        jobs.feedback_attribution_compute.deadline_secs = 0;

        let report = deploy.validate_deploy_common();
        for field in [
            "global_concurrency",
            "feedback_cycle_concurrency",
            "heartbeat_secs",
            "feedback_stuck_secs",
            "feedback_alert_timeout_secs",
            "feedback_alert_dedupe_secs",
            "shutdown_drain_secs",
            "feature_parity_compute.page_size",
            "feature_parity_compute.max_concurrency",
            "feature_parity_compute.max_working_set_bytes",
            "feature_parity_compute.deadline_secs",
            "feedback_attribution_compute.page_size",
            "feedback_attribution_compute.max_concurrency",
            "feedback_attribution_compute.max_working_set_bytes",
            "feedback_attribution_compute.deadline_secs",
        ] {
            assert!(
                report
                    .errors
                    .iter()
                    .any(|error| error.to_string().contains(field)),
                "invalid research-job field {field} was accepted: {:?}",
                report.errors
            );
        }
    }

    #[test]
    fn model_registry_budgets_bounded() {
        let defaults = ModelServingRegistryConfig::default();
        let invalid = [
            (
                ModelServingRegistryConfig {
                    max_cached_contracts: 0,
                    ..defaults
                },
                "max_cached_contracts",
            ),
            (
                ModelServingRegistryConfig {
                    max_concurrent_loads: 65,
                    max_pending_loads: 65,
                    ..defaults
                },
                "max_concurrent_loads",
            ),
            (
                ModelServingRegistryConfig {
                    max_pending_loads: defaults.max_concurrent_loads - 1,
                    ..defaults
                },
                "max_pending_loads",
            ),
            (
                ModelServingRegistryConfig {
                    load_timeout_ms: 999,
                    ..defaults
                },
                "load_timeout_ms",
            ),
        ];
        for (registry, field) in invalid {
            let mut deploy = DeployConfig::default();
            deploy.research.model_serving_registry = registry;
            let report = deploy.validate_deploy_common();
            assert!(
                report
                    .errors
                    .iter()
                    .any(|error| error.to_string().contains(field)),
                "invalid registry field {field} was accepted: {:?}",
                report.errors
            );
        }
    }

    #[test]
    fn weather_bindings_reject_identity() {
        let mut deploy = DeployConfig::default();
        let duplicate = deploy
            .domain_sources
            .weather_vertical_bindings
            .airnow_pm25_reporting_areas[0]
            .clone();
        deploy
            .domain_sources
            .weather_vertical_bindings
            .airnow_pm25_reporting_areas
            .push(duplicate);
        deploy.domain_sources.nasa_gistemp.csv_url = "file:///tmp/gistemp.csv".to_owned();
        let report = deploy.validate_deploy_common();
        assert!(report.has_errors());
        assert!(report.errors.iter().any(|error| {
            error
                .to_string()
                .contains("weather_vertical_bindings.airnow_pm25_reporting_areas")
        }));
        assert!(
            report
                .errors
                .iter()
                .any(|error| { error.to_string().contains("domain_sources.nasa_gistemp") })
        );
    }

    #[test]
    fn airnow_pm25_requires_identity() {
        let mut deploy = DeployConfig::default();
        let duplicate = deploy
            .domain_sources
            .weather_vertical_bindings
            .airnow_pm25_sites[0]
            .clone();
        deploy
            .domain_sources
            .weather_vertical_bindings
            .airnow_pm25_sites
            .push(duplicate);
        let report = deploy.validate_deploy_common();
        assert!(report.has_errors());
        assert!(report.errors.iter().any(|error| {
            error
                .to_string()
                .contains("weather_vertical_bindings.airnow_pm25_sites")
        }));
    }

    #[test]
    fn ghcnh_concurrency_bounded_contract() {
        let mut deploy = DeployConfig::default();
        deploy.domain_sources.ghcnh.max_concurrency = 9;
        let report = deploy.validate_deploy_common();
        assert!(report.has_errors());
        assert!(report.errors.iter().any(|error| {
            error.to_string().contains("domain_sources.ghcnh")
                && error.to_string().contains("max_concurrency")
        }));
    }

    #[test]
    fn wrong_chain_id_fatal() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.chain_id = 1;
        assert!(deploy.validate_deploy_common().has_errors());
    }

    #[test]
    fn password_crypto_budget_fatal() {
        for (max_in_flight, deadline_ms, field) in [
            (0, 15_000, "max_in_flight"),
            (8, 999, "deadline_ms"),
            (65, 15_000, "max_in_flight"),
            (8, 60_001, "deadline_ms"),
        ] {
            let mut deploy = DeployConfig::default();
            deploy.web.password_crypto.max_in_flight = max_in_flight;
            deploy.web.password_crypto.deadline_ms = deadline_ms;
            let report = deploy.validate_deploy_common();
            assert!(report.has_errors());
            assert!(
                report
                    .errors
                    .iter()
                    .any(|error| error.to_string().contains(field))
            );
        }
    }

    #[test]
    fn order_post_timeout_fatal() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.order_post_timeout_ms = 34_999;
        assert!(deploy.validate_deploy_common().has_errors());
    }

    #[test]
    fn order_post_accepts_floor() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.order_post_timeout_ms = 35_000;
        let report = deploy.validate_deploy_common();
        assert!(!report.errors.iter().any(|error| {
            error
                .to_string()
                .contains("polymarket.order_post_timeout_ms")
        }));
    }

    #[test]
    fn settlement_lease_authorization_bounded() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.settlement.claim_lease_secs = 4;
        deploy
            .polymarket
            .settlement
            .semi_auto_authorization_ttl_secs = 3_601;
        let report = deploy.validate_deploy_common();
        assert!(report.has_errors());
        assert!(report.errors.iter().any(|error| {
            error
                .to_string()
                .contains("polymarket.settlement.claim_lease_secs")
        }));
        assert!(report.errors.iter().any(|error| {
            error
                .to_string()
                .contains("polymarket.settlement.semi_auto_authorization_ttl_secs")
        }));
    }

    #[test]
    fn settlement_ui_readiness_bounded() {
        for invalid_ttl in [0, 61] {
            let mut deploy = DeployConfig::default();
            deploy.polymarket.settlement.readiness_ui_cache_secs = invalid_ttl;
            let report = deploy.validate_deploy_common();
            assert!(report.has_errors());
            assert!(report.errors.iter().any(|error| {
                error
                    .to_string()
                    .contains("polymarket.settlement.readiness_ui_cache_secs")
            }));
        }
    }

    #[test]
    fn zero_trade_tape_fatal() {
        let mut deploy = DeployConfig::default();
        deploy
            .market_data
            .trade_tape_on_chain
            .max_blocks_per_request = 0;
        assert!(deploy.validate_deploy_common().has_errors());
    }

    #[test]
    fn trade_tape_reconciliation_budget() {
        let mut deploy = DeployConfig::default();
        deploy
            .market_data
            .trade_tape_on_chain
            .reconciliation_max_rows = MAX_TRADE_TAPE_RECONCILIATION_ROWS + 1;
        let report = deploy.validate_deploy_common();
        assert!(
            report
                .errors
                .iter()
                .any(|error| { error.to_string().contains("reconciliation_max_rows") })
        );
    }

    #[test]
    fn auto_execution_requires_credentials() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(report.has_errors());
    }

    #[test]
    fn report_only_requires_credentials() {
        // report_only is not dry-run: it needs a private key + funder to read
        // the real venue account, so missing credentials fail closed.
        let deploy = DeployConfig::default();
        let report = validate_deploy_mode(&deploy, QuantRuntimeMode::ReportOnly);
        assert!(report.has_errors());
    }

    #[test]
    fn auto_rejects_invalid_key() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.web.jwt.signing_key = "human-password".into();
        let report = validate_deploy_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(report.has_errors(), "incomplete jwt keyring must be fatal");
    }

    #[test]
    fn keys_never_reuse_bytes() {
        let mut deploy = DeployConfig::default();
        deploy.web.jwt.signing_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into();
        deploy.research.evidence_attestation.signing_key = "00".repeat(32).into();
        let report = deploy.validate_deploy_common();
        assert!(report.has_errors());
        assert!(report.errors.iter().any(|error| {
            error.to_string().contains("cryptographically independent")
                && !error.to_string().contains(&"00".repeat(32))
        }));
    }

    #[test]
    fn auto_execution_accepts_credentials() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        deploy.web.jwt.signing_key = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".into();
        let report = validate_deploy_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn report_only_accepts_key() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        deploy.web.jwt.signing_key = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".into();
        let report = validate_deploy_mode(&deploy, QuantRuntimeMode::ReportOnly);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
        assert!(report.warnings.is_empty());
    }
}
