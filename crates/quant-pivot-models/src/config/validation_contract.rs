//! Canonical human-readable contracts for Deploy Config validation.
//!
//! Runtime predicates remain strongly typed in [`super::validation`]. This
//! inventory is the shared, exhaustive documentation boundary consumed by the
//! descriptor, generated TOML, safe projection, and contract audits. A rule is
//! attached only to the exact leaves that participate in it; `**` denotes a
//! descendant scope and `*` denotes one dynamic-map segment.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One stable validation rule exposed alongside every affected deploy leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployValidationRuleDescriptor {
    pub rule_id: String,
    pub condition: String,
    pub requirement: String,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DeployValidationRuleContract {
    pub rule_id: &'static str,
    pub scopes: &'static [&'static str],
    pub condition: &'static str,
    pub requirement: &'static str,
}

impl DeployValidationRuleContract {
    pub(super) const ALL: &'static [Self] = &[
        Self::new(
            "deploy.polymarket.chain",
            &["polymarket.chain_id"],
            "Always",
            "The value must equal the Polygon mainnet chain ID 137.",
        ),
        Self::new(
            "deploy.polymarket.order-post-timeout",
            &["polymarket.order_post_timeout_ms"],
            "Always",
            "The value must be at least 35000 ms so it contains the SDK's 30-second asynchronous commit identity-enrichment budget.",
        ),
        Self::new(
            "deploy.polymarket.market-info-refresh",
            &["polymarket.clob_market_info_refresh_secs"],
            "Always",
            "The value must be at least 60 seconds.",
        ),
        Self::new(
            "deploy.polymarket.rpc-endpoint",
            &["polymarket.onchain.rpc_endpoint"],
            "Always",
            "The endpoint must be HTTP(S). A public endpoint may not contain user-info, path credentials, query parameters, or a fragment; authenticated endpoints must use the protected variant.",
        ),
        Self::new(
            "deploy.polymarket.rpc-timeout",
            &["polymarket.onchain.rpc_timeout_ms"],
            "Always",
            "The timeout must be greater than zero.",
        ),
        Self::new(
            "deploy.settlement.claim-lease",
            &["polymarket.settlement.claim_lease_secs"],
            "Always",
            "The lease must be between 5 and 300 seconds inclusive.",
        ),
        Self::new(
            "deploy.settlement.authorization-ttl",
            &["polymarket.settlement.operator_authorization_ttl_secs"],
            "Always",
            "The authorization TTL must be between 30 and 3600 seconds inclusive.",
        ),
        Self::new(
            "deploy.settlement.polls",
            &[
                "polymarket.settlement.discovery_poll_secs",
                "polymarket.settlement.submission_poll_secs",
            ],
            "Always",
            "Discovery and submission polling intervals must each be greater than zero.",
        ),
        Self::new(
            "deploy.settlement.claim-budget",
            &["polymarket.settlement.max_claims_per_tick"],
            "Always",
            "The per-tick claim budget must be between 1 and 1024 inclusive.",
        ),
        Self::new(
            "deploy.settlement.rpc-concurrency",
            &["polymarket.settlement.rpc_concurrency"],
            "Always",
            "RPC concurrency must be between 1 and 32 inclusive.",
        ),
        Self::new(
            "deploy.settlement.readiness-cache",
            &["polymarket.settlement.readiness_ui_cache_secs"],
            "Always",
            "The readiness cache TTL must be between 1 and 60 seconds inclusive.",
        ),
        Self::new(
            "deploy.settlement.scan-span",
            &["polymarket.settlement.external_scan_block_span"],
            "Always",
            "The external settlement scan span must be between 1 and 10000 blocks inclusive.",
        ),
        Self::new(
            "deploy.settlement.retry-window",
            &[
                "polymarket.settlement.retry_initial_secs",
                "polymarket.settlement.retry_max_secs",
            ],
            "Always",
            "retry_initial_secs must be greater than zero and no greater than retry_max_secs; retry_max_secs must be no greater than 3600 seconds.",
        ),
        Self::new(
            "deploy.report.lifecycle-positive",
            &[
                "quant.workers.report_schedule_poll_secs",
                "quant.workers.report_run_lease_secs",
                "quant.workers.report_run_heartbeat_secs",
                "quant.workers.report_ad_hoc_queue_ttl_secs",
            ],
            "Always",
            "The report polling interval, run lease, heartbeat interval, and ad-hoc queue TTL must each be greater than zero.",
        ),
        Self::new(
            "deploy.report.heartbeat-lease",
            &[
                "quant.workers.report_run_heartbeat_secs",
                "quant.workers.report_run_lease_secs",
            ],
            "Always",
            "report_run_heartbeat_secs must be no greater than one third of report_run_lease_secs.",
        ),
        Self::new(
            "deploy.report.ad-hoc-ttl",
            &[
                "quant.workers.report_ad_hoc_queue_ttl_secs",
                "quant.workers.report_schedule_poll_secs",
            ],
            "Always",
            "report_ad_hoc_queue_ttl_secs must be greater than report_schedule_poll_secs.",
        ),
        Self::new(
            "deploy.report.ad-hoc-capacity",
            &["quant.workers.report_ad_hoc_queue_capacity"],
            "Always",
            "The ad-hoc report queue capacity must be between 1 and 1024 inclusive.",
        ),
        Self::new(
            "deploy.report.expiry-sweep",
            &["quant.workers.report_expire_sweep_secs"],
            "Always",
            "The report expiry sweep interval must be greater than zero.",
        ),
        Self::new(
            "deploy.market-websocket.subscription-budget",
            &[
                "market_data.websocket.max_subscriptions_per_connection",
                "market_data.websocket.engine_max_subscription_tokens",
                "market_data.websocket.engine_subscription_window_hours",
            ],
            "Always",
            "The per-connection subscription limit, engine token limit, and subscription window must each be greater than zero.",
        ),
        Self::new(
            "deploy.gamma.pagination",
            &[
                "market_data.gamma.page_size",
                "market_data.gamma.reconcile_interval_secs",
                "market_data.gamma.max_keyset_pages",
                "market_data.gamma.max_keyset_requests",
                "market_data.gamma.historical_identity_days",
            ],
            "Always",
            "All values must be greater than zero; page_size must be no greater than the Gamma /events/keyset limit of 500; max_keyset_requests must be no less than max_keyset_pages; historical identity must cover the exchange-history retention frontier.",
        ),
        Self::new(
            "deploy.exchange-history.work-budget",
            &[
                "market_data.finalized_exchange_history.connect_timeout_ms",
                "market_data.finalized_exchange_history.request_timeout_ms",
                "market_data.finalized_exchange_history.attestor.max_blocks_per_log_request",
                "market_data.finalized_exchange_history.attestor.max_concurrent_log_requests",
                "market_data.finalized_exchange_history.max_hypersync_response_body_bytes",
                "market_data.finalized_exchange_history.max_rpc_response_body_bytes",
                "market_data.finalized_exchange_history.max_canonical_chunk_bytes",
                "market_data.finalized_exchange_history.min_blocks_per_chunk",
                "market_data.finalized_exchange_history.max_blocks_per_chunk",
                "market_data.finalized_exchange_history.retry_initial_ms",
                "market_data.finalized_exchange_history.retry_max_ms",
                "market_data.finalized_exchange_history.retry_max_attempts",
                "market_data.finalized_exchange_history.hot_window_blocks_per_tick",
                "market_data.finalized_exchange_history.full_history_blocks_per_tick",
                "market_data.finalized_exchange_history.batch_size",
            ],
            "Always",
            "Timeout, HyperSync/RPC response-body, canonical-chunk, attestor request span/concurrency, adaptive logical-chunk, retry, worker, and batch budgets must form valid bounded ranges.",
        ),
        Self::new(
            "deploy.exchange-history.authority",
            &[
                "market_data.finalized_exchange_history.enabled",
                "market_data.finalized_exchange_history.hypersync.endpoint",
                "market_data.finalized_exchange_history.hypersync.api_token",
                "market_data.finalized_exchange_history.attestor.rpc_endpoint",
                "market_data.finalized_exchange_history.model_confirmation_blocks",
                "market_data.finalized_exchange_history.rollback_buffer_blocks",
                "market_data.finalized_exchange_history.activation_frontier_days",
                "market_data.finalized_exchange_history.retention_frontier_days",
            ],
            "Always",
            "Enabled history requires authenticated HyperSync extraction, an independent non-Envio archive RPC witness, and the fixed fresh-boot PIT/frontier policy.",
        ),
        Self::new(
            "deploy.postgres.pool",
            &["db.postgres.min_connections", "db.postgres.max_connections"],
            "Always",
            "max_connections must be greater than zero and no less than min_connections.",
        ),
        Self::new(
            "deploy.postgres.user",
            &["db.postgres.user"],
            "Always",
            "The PostgreSQL user must contain at least one non-whitespace character.",
        ),
        Self::new(
            "deploy.clickhouse.writer",
            &[
                "db.clickhouse.batch_size",
                "db.clickhouse.flush_interval_secs",
                "db.clickhouse.max_concurrent_inserts",
                "db.clickhouse.max_inflight_write_bytes",
                "db.clickhouse.max_concurrent_reads",
                "db.clickhouse.max_threads_per_query",
                "db.clickhouse.io.query_timeout_ms",
                "db.clickhouse.io.maintenance_timeout_ms",
                "db.clickhouse.io.critical_insert.send_timeout_ms",
                "db.clickhouse.io.critical_insert.end_timeout_ms",
                "db.clickhouse.io.critical_insert.attempt_timeout_ms",
                "db.clickhouse.io.bulk_insert.send_timeout_ms",
                "db.clickhouse.io.bulk_insert.end_timeout_ms",
                "db.clickhouse.io.bulk_insert.attempt_timeout_ms",
            ],
            "Always",
            "Batch count/age, writer byte budgets, read concurrency, per-query threads, and all ClickHouse I/O deadlines are bounded. Insert send plus end fits within one attempt; the complete three-attempt retry window remains below the durable ACK ceiling, one-second derived flush plus retry plus scheduling margin fits the shared receipt deadline, configured flush plus retry plus margin fits the shutdown capacity budget, and a critical attempt remains below publication quarantine.",
        ),
        Self::new(
            "deploy.clickhouse.user",
            &["db.clickhouse.user"],
            "Always",
            "The ClickHouse user must contain at least one non-whitespace character.",
        ),
        Self::new(
            "deploy.redis.connection",
            &[
                "cache.redis.host",
                "cache.redis.port",
                "cache.redis.user",
                "cache.redis.password",
                "cache.redis.database",
            ],
            "Always",
            "host must be non-empty, port must be greater than zero, and host/port/user/password/database must compose a valid Redis connection URL.",
        ),
        Self::new(
            "deploy.redis.pool",
            &["cache.redis.pool_size"],
            "Always",
            "The Redis pool size must be greater than zero.",
        ),
        Self::new(
            "deploy.web.password-crypto-concurrency",
            &["web.password_crypto.max_in_flight"],
            "Always",
            "Password-crypto concurrency must be between 1 and 64 inclusive.",
        ),
        Self::new(
            "deploy.web.password-crypto-deadline",
            &["web.password_crypto.deadline_ms"],
            "Always",
            "The password-crypto deadline must be between 1000 and 60000 ms inclusive.",
        ),
        Self::new(
            "deploy.web.jwt-identity",
            &["web.jwt.issuer", "web.jwt.audience"],
            "Always",
            "Issuer and audience must each contain at least one non-whitespace character.",
        ),
        Self::new(
            "deploy.web.jwt-ttl",
            &[
                "web.jwt.access_ttl_secs",
                "web.jwt.refresh_ttl_secs",
                "web.jwt.absolute_session_ttl_secs",
            ],
            "Always",
            "All token TTLs must be positive; access_ttl_secs and refresh_ttl_secs must each be no greater than absolute_session_ttl_secs.",
        ),
        Self::new(
            "deploy.web.jwt-signing-key",
            &["web.jwt.signing_key"],
            "Every deployment",
            "The value must be a Base64URL-no-pad encoded 32-byte HS256 signing key.",
        ),
        Self::new(
            "deploy.evidence.key-separation",
            &[
                "research.evidence_attestation.signing_key",
                "web.jwt.signing_key",
            ],
            "When the evidence-attestation signing key is configured",
            "The evidence-attestation signing key must be cryptographically independent from the JWT signing key.",
        ),
        Self::new(
            "deploy.account-read-credentials",
            &["keys.private_key", "quant.account.funder"],
            "Whenever the production account is read",
            "A private key and non-empty funder address are required because reports freeze real collateral and position state; account reads are not simulation.",
        ),
        Self::new(
            "deploy.relayer.credentials",
            &[
                "quant.account.wallet_kind",
                "polymarket.relayer.api_key",
                "polymarket.relayer.api_key_address",
            ],
            "When an authorized intent may submit and wallet_kind is proxy, gnosis_safe, or deposit_wallet",
            "Both relayer API key and relayer API-key address are required.",
        ),
        Self::new(
            "deploy.solver.deadline",
            &["quant.portfolio_solver.deadline_secs"],
            "Always",
            "The end-to-end global-planner deadline must be between 1 and 600 seconds inclusive; reaching it is a report failure, never an acceptable incumbent fallback. The default is a bootstrap liveness ceiling for the qualified 10000-tier/400-scenario/Top20 workload, not a universal latency SLO.",
        ),
        Self::new(
            "deploy.solver.threads",
            &["quant.portfolio_solver.threads"],
            "Always",
            "The value must equal 1 for deterministic serial HiGHS execution.",
        ),
        Self::new(
            "deploy.solver.tier-budget",
            &["quant.portfolio_solver.max_tiers"],
            "Always",
            "The executable-tier budget must be between 1 and 50000 inclusive.",
        ),
        Self::new(
            "deploy.solver.scenario-budget",
            &["quant.portfolio_solver.max_scenarios"],
            "Always",
            "The joint-scenario budget must be between 3 and 10000 inclusive.",
        ),
        Self::new(
            "deploy.solver.top-n-budget",
            &["quant.portfolio_solver.max_top_n"],
            "Always",
            "The publishable leave-one-out width must be between 1 and 1000 inclusive; the default 20 is part of the qualified 10000-tier/400-scenario/Top20 workload tuple.",
        ),
        Self::new(
            "deploy.research.global-concurrency",
            &["quant.research_jobs.global_concurrency"],
            "Always",
            "Global durable research-job concurrency must be between 1 and 32 inclusive.",
        ),
        Self::new(
            "deploy.research.kind-concurrency",
            &[
                "quant.research_jobs.dataset_build_concurrency",
                "quant.research_jobs.model_train_concurrency",
                "quant.research_jobs.backtest_concurrency",
                "quant.research_jobs.bias_table_fit_concurrency",
                "quant.research_jobs.model_calibration_fit_concurrency",
                "quant.research_jobs.cpcv_backtest_concurrency",
                "quant.research_jobs.feature_parity_concurrency",
                "quant.research_jobs.feedback_coverage_concurrency",
                "quant.research_jobs.feedback_drift_concurrency",
                "quant.research_jobs.feedback_learning_concurrency",
                "quant.research_jobs.feedback_cycle_concurrency",
                "quant.research_jobs.trade_policy_fit_concurrency",
                "quant.research_jobs.trade_policy_validation_concurrency",
            ],
            "Always",
            "Each stage-specific concurrency must be greater than zero and no greater than global_concurrency.",
        ),
        Self::new(
            "deploy.research.lease",
            &["quant.research_jobs.lease_ttl_secs"],
            "Always",
            "The durable job lease TTL must be between 3 and 3600 seconds inclusive.",
        ),
        Self::new(
            "deploy.research.heartbeat",
            &[
                "quant.research_jobs.heartbeat_secs",
                "quant.research_jobs.lease_ttl_secs",
            ],
            "Always",
            "heartbeat_secs must be greater than zero and no greater than one third of lease_ttl_secs.",
        ),
        Self::new(
            "deploy.research.poll",
            &["quant.research_jobs.poll_secs"],
            "Always",
            "The durable job polling interval must be between 1 and 300 seconds inclusive.",
        ),
        Self::new(
            "deploy.research.recovery-attempts",
            &["quant.research_jobs.max_recovery_attempts"],
            "Always",
            "The recovery-attempt limit must be between 0 and 32 inclusive.",
        ),
        Self::new(
            "deploy.research.execution-retry",
            &[
                "quant.research_jobs.execution_retry_initial_secs",
                "quant.research_jobs.execution_retry_max_secs",
            ],
            "Always",
            "execution_retry_initial_secs must be greater than zero and no greater than execution_retry_max_secs; execution_retry_max_secs must be no greater than 3600 seconds.",
        ),
        Self::new(
            "deploy.research.spine-budget",
            &["quant.research_jobs.max_spine_samples"],
            "Always",
            "The frozen training-spine sample budget must be between 1 and 100000000 inclusive.",
        ),
        Self::new(
            "deploy.research.plan-sample-budget",
            &[
                "quant.research_jobs.plan_sample_slices",
                "quant.research_jobs.plan_sample_markets",
            ],
            "Always",
            "plan_sample_slices must be no greater than 64 and plan_sample_markets must be between 1 and 10000 inclusive.",
        ),
        Self::new(
            "deploy.research.progress-cadence",
            &["quant.research_jobs.progress_min_interval_ms"],
            "Always",
            "The durable progress-update interval must be between 50 and 60000 ms inclusive.",
        ),
        Self::new(
            "deploy.research.stuck-deadline",
            &[
                "quant.research_jobs.feedback_stuck_secs",
                "quant.research_jobs.lease_ttl_secs",
            ],
            "Always",
            "feedback_stuck_secs must be greater than lease_ttl_secs and no greater than 2592000 seconds (30 days).",
        ),
        Self::new(
            "deploy.research.alert-timeout",
            &[
                "quant.research_jobs.feedback_alert_timeout_secs",
                "quant.research_jobs.shutdown_drain_secs",
            ],
            "Always",
            "feedback_alert_timeout_secs must be greater than zero and no greater than shutdown_drain_secs.",
        ),
        Self::new(
            "deploy.research.alert-dedupe",
            &[
                "quant.research_jobs.feedback_alert_dedupe_secs",
                "quant.research_jobs.feedback_alert_timeout_secs",
            ],
            "Always",
            "feedback_alert_dedupe_secs must be no less than feedback_alert_timeout_secs and no greater than 86400 seconds.",
        ),
        Self::new(
            "deploy.research.shutdown-drain",
            &["quant.research_jobs.shutdown_drain_secs"],
            "Always",
            "The shutdown drain budget must be between 1 and 3 seconds inclusive.",
        ),
        Self::new(
            "deploy.research.feature-parity-page",
            &["quant.research_jobs.feature_parity_compute.page_size"],
            "Always",
            "The feature-parity page size must be between 1 and 1000 subjects inclusive.",
        ),
        Self::new(
            "deploy.research.feature-parity-concurrency",
            &[
                "quant.research_jobs.feature_parity_compute.max_concurrency",
                "quant.research_jobs.feature_parity_concurrency",
            ],
            "Always",
            "max_concurrency must equal 1 and be no greater than feature_parity_concurrency.",
        ),
        Self::new(
            "deploy.research.feature-parity-memory",
            &["quant.research_jobs.feature_parity_compute.max_working_set_bytes"],
            "Always",
            "The memory budget must be between 1048576 bytes (1 MiB) and 10737418240 bytes (10 GiB) inclusive.",
        ),
        Self::new(
            "deploy.research.feature-parity-deadline",
            &["quant.research_jobs.feature_parity_compute.deadline_secs"],
            "Always",
            "The stage deadline must be between 1 and 86400 seconds inclusive.",
        ),
        Self::new(
            "deploy.research.attribution-page",
            &["quant.research_jobs.feedback_attribution_compute.page_size"],
            "Always",
            "The feedback-attribution page size must be between 1 and 1000 rows inclusive.",
        ),
        Self::new(
            "deploy.research.attribution-concurrency",
            &["quant.research_jobs.feedback_attribution_compute.max_concurrency"],
            "Always",
            "Attribution-group concurrency must be between 1 and 32 inclusive.",
        ),
        Self::new(
            "deploy.research.attribution-memory",
            &["quant.research_jobs.feedback_attribution_compute.max_working_set_bytes"],
            "Always",
            "The memory budget must be between 1048576 bytes (1 MiB) and 10737418240 bytes (10 GiB) inclusive.",
        ),
        Self::new(
            "deploy.research.attribution-deadline",
            &["quant.research_jobs.feedback_attribution_compute.deadline_secs"],
            "Always",
            "The stage deadline must be between 1 and 86400 seconds inclusive.",
        ),
        Self::new(
            "deploy.model-registry.cache",
            &["research.model_serving_registry.max_cached_contracts"],
            "Always",
            "The champion/shadow contract cache must hold between 1 and 1024 contracts inclusive.",
        ),
        Self::new(
            "deploy.model-registry.load-concurrency",
            &["research.model_serving_registry.max_concurrent_loads"],
            "Always",
            "Concurrent model loads must be between 1 and 64 inclusive.",
        ),
        Self::new(
            "deploy.model-registry.pending-loads",
            &[
                "research.model_serving_registry.max_pending_loads",
                "research.model_serving_registry.max_concurrent_loads",
            ],
            "Always",
            "max_pending_loads must be no less than max_concurrent_loads and no greater than 4096.",
        ),
        Self::new(
            "deploy.model-registry.load-timeout",
            &["research.model_serving_registry.load_timeout_ms"],
            "Always",
            "The model-load timeout must be between 1000 and 300000 ms inclusive.",
        ),
        Self::new(
            "deploy.model-registry.shadow-memory",
            &["research.model_serving_registry.max_total_shadow_model_bytes"],
            "Always",
            "The aggregate shadow-model memory budget must be between 67108864 bytes (64 MiB) and 1099511627776 bytes (1 TiB) inclusive.",
        ),
        Self::new(
            "deploy.source.binance",
            &["domain_sources.binance.**"],
            "domain_sources.binance.enabled is true",
            "request_timeout_ms, agg_trade_recovery_poll_secs, websocket_rotation_secs, batch_size, and max_clock_skew_ms must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.binance-usdm",
            &["domain_sources.binance_usdm_futures.**"],
            "domain_sources.binance_usdm_futures.enabled is true",
            "request_timeout_ms, agg_trade_recovery_poll_secs, websocket_rotation_secs, batch_size, and max_clock_skew_ms must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.polymarket-rtds",
            &["domain_sources.polymarket_rtds.**"],
            "domain_sources.polymarket_rtds.enabled is true",
            "websocket_url must use ws/wss with a host, connect_timeout_ms and max_clock_skew_ms must be greater than zero, and keepalive_secs must equal the official 5-second cadence.",
        ),
        Self::new(
            "deploy.source.chainlink-streams",
            &["domain_sources.chainlink_data_streams.**"],
            "domain_sources.chainlink_data_streams.enabled is true",
            "API key and API secret must both be present, at least one frozen V3 feed must exist, and max_clock_skew_ms and rest_page_limit must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.aviation-weather",
            &["domain_sources.aviation_weather.**"],
            "domain_sources.aviation_weather.enabled is true",
            "poll_secs and request_timeout_ms must each be greater than zero, and day_close_grace_secs must equal the governed 7200-second methodology value.",
        ),
        Self::new(
            "deploy.source.ghcnh",
            &["domain_sources.ghcnh.**"],
            "domain_sources.ghcnh.enabled is true",
            "base_url must be HTTP(S); request_timeout_ms, refresh_secs, and calibration_years must each be greater than zero; max_concurrency must be between 1 and 8 inclusive.",
        ),
        Self::new(
            "deploy.source.ghcnd",
            &["domain_sources.ghcnd.**"],
            "domain_sources.ghcnd.enabled is true",
            "base_url must be HTTP(S); request_timeout_ms, refresh_secs, and lookback_years must each be greater than zero; max_concurrency must be between 1 and 8 inclusive.",
        ),
        Self::new(
            "deploy.source.gefs",
            &["domain_sources.gefs.**"],
            "domain_sources.gefs.enabled is true",
            "request_timeout_ms, poll_secs, publication_lag_secs, and max_concurrency must each be greater than zero; max_lead_hours must be a multiple of 3 between 3 and 240 inclusive.",
        ),
        Self::new(
            "deploy.source.hko-open-data",
            &["domain_sources.hko_open_data.**"],
            "domain_sources.hko_open_data.enabled is true",
            "base_url must be HTTP(S); request and polling intervals must be greater than zero; daily_rainfall_lookback_days must be 1..=366; daily_temperature_lookback_months must be 1..=120.",
        ),
        Self::new(
            "deploy.source.airnow",
            &["domain_sources.airnow.**"],
            "domain_sources.airnow.enabled is true",
            "Both URLs must be HTTP(S), request_timeout_ms and poll_secs must be greater than zero, and correction_lookback_hours must be 1..=168.",
        ),
        Self::new(
            "deploy.source.tornado",
            &["domain_sources.tornado.**"],
            "domain_sources.tornado.enabled is true",
            "All URLs must be HTTP(S), request/refresh/poll intervals must be greater than zero, and ncei_backfill_years must be 1..=10.",
        ),
        Self::new(
            "deploy.source.nhc",
            &["domain_sources.nhc.**"],
            "domain_sources.nhc.enabled is true",
            "Both URLs must be HTTP(S), and request_timeout_ms, advisory_poll_secs, and best_track_refresh_secs must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.nasa-gistemp",
            &["domain_sources.nasa_gistemp.**"],
            "domain_sources.nasa_gistemp.enabled is true",
            "Both URLs must be HTTP(S), and request_timeout_ms and refresh_secs must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.nsidc-sea-ice",
            &["domain_sources.nsidc_sea_ice.**"],
            "domain_sources.nsidc_sea_ice.enabled is true",
            "All daily and monthly URLs must be HTTP(S), and request_timeout_ms and refresh_secs must each be greater than zero.",
        ),
        Self::new(
            "deploy.source.nws-observation",
            &["domain_sources.nws_observation.**"],
            "domain_sources.nws_observation.enabled is true",
            "base_url must be HTTP(S), request_timeout_ms and poll_secs must be greater than zero, and lookback_observations must be 1..=500.",
        ),
        Self::new(
            "deploy.weather-station.binding",
            &["domain_sources.weather_stations.*.**"],
            "For every configured weather-station map entry",
            "The map key must be a valid ICAO station, timezone must be an IANA identifier, latitude must be -90..=90 degrees, longitude must be -180..=180 degrees, and GHCNh/GHCNd identities must match historical_binding_kind.",
        ),
        Self::new(
            "deploy.weather-vertical.hko-rainfall",
            &["domain_sources.weather_vertical_bindings.hko_rainfall.**"],
            "For every configured binding",
            "The binding must have a unique source-native HKO station, matching official HTTP(S) daily CSV, printable site key no longer than 128 bytes, latitude in -90..=90 degrees, longitude in -180..=180 degrees, and Asia/Hong_Kong timezone.",
        ),
        Self::new(
            "deploy.weather-vertical.hko-temperature",
            &["domain_sources.weather_vertical_bindings.hko_daily_temperature.**"],
            "For every configured binding",
            "The HKO station must be source-native and unique, and timezone must equal Asia/Hong_Kong.",
        ),
        Self::new(
            "deploy.weather-vertical.airnow-area",
            &["domain_sources.weather_vertical_bindings.airnow_pm25_reporting_areas.**"],
            "For every configured binding",
            "Area/state identity must be unique, area must be printable and no longer than 128 bytes, state must be two uppercase ASCII letters, and timezone must be an IANA identifier.",
        ),
        Self::new(
            "deploy.weather-vertical.airnow-site",
            &["domain_sources.weather_vertical_bindings.airnow_pm25_sites.**"],
            "For every configured binding",
            "AQSID must be a unique 12-digit identity; location/site must be printable and no longer than 128 bytes; resolution URL must be HTTP(S); state must be two uppercase ASCII letters; latitude must be -90..=90 degrees, longitude must be -180..=180 degrees, and timezone must be an IANA identifier.",
        ),
        Self::new(
            "deploy.weather-vertical.tornado-region",
            &["domain_sources.weather_vertical_bindings.tornado_regions.**"],
            "For every configured binding",
            "Region identity must be unique and printable within 64 bytes; scope must be United States or a valid state code/name pair; timezone must be an IANA identifier.",
        ),
        Self::new(
            "deploy.weather-vertical.nhc-storm",
            &["domain_sources.weather_vertical_bindings.nhc_historical_storms.**"],
            "For every configured binding",
            "Basin and eight-character storm ID must form a unique supported HURDAT2 identity with the correct AL, EP, or CP prefix.",
        ),
        Self::new(
            "deploy.weather-vertical.nws-wind",
            &["domain_sources.weather_vertical_bindings.nws_wind_stations.**"],
            "For every configured binding",
            "Station must be a unique ICAO identifier and timezone must be an IANA identifier.",
        ),
    ];

    const fn new(
        rule_id: &'static str,
        scopes: &'static [&'static str],
        condition: &'static str,
        requirement: &'static str,
    ) -> Self {
        Self {
            rule_id,
            scopes,
            condition,
            requirement,
        }
    }

    pub(super) fn applies_to(self, path: &str) -> bool {
        self.scopes
            .iter()
            .any(|scope| Self::scope_matches(scope, path))
    }

    pub(super) fn scope_matches(scope: &str, path: &str) -> bool {
        let expected = scope.split('.').collect::<Vec<_>>();
        let actual = path.split('.').collect::<Vec<_>>();
        let descendant = expected.last() == Some(&"**");
        let compared = if descendant {
            &expected[..expected.len().saturating_sub(1)]
        } else {
            expected.as_slice()
        };
        (descendant && actual.len() >= compared.len() || actual.len() == compared.len())
            && compared
                .iter()
                .zip(actual.iter())
                .all(|(left, right)| *left == "*" || left == right)
    }

    pub(super) fn descriptor(self) -> DeployValidationRuleDescriptor {
        DeployValidationRuleDescriptor {
            rule_id: self.rule_id.to_owned(),
            condition: self.condition.to_owned(),
            requirement: self.requirement.to_owned(),
        }
    }
}
