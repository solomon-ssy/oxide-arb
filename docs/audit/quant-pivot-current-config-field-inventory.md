# quant-pivot Current Config Field Inventory

> The original W0 inventory is superseded by the clean-break S1 contract. This file retains the
> reviewed inventory shape, but field names below use the current finalized-exchange-history and
> execution-quality vocabulary. The generated Config API schema remains authoritative.

## Runtime Config

| Current resource | Raw schema leaves excluding schema_version | Current logical operator controls | Target resource |
|---|---:|---:|---|
| recommendation_policy | 24 | 24 | recommendation_policy |
| execution_risk_policy | 44 | 44 | execution_risk_policy |
| model_routing | 44 | 26 | model_routing |
| report_schedule | 8 | 5 | report_schedule |
| operational_control | 13 | 13 | operations_policy |
| execution_authorization | 5 | 5 | execution_automation_policy |

Current logical operator-control total: **117**. Raw schema leaves are higher because ModelRouting
binding evidence and schedule collection members expand into readonly/generated subfields.

### recommendation_policy raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /data_quality/feature_staleness_policy | enum | migrate to v2 descriptor |
| /data_quality/max_book_age_ms | integer | migrate to v2 descriptor |
| /data_quality/max_domain_observation_age_secs | integer | migrate to v2 descriptor |
| /data_quality/max_feature_bucket_age_secs | integer | migrate to v2 descriptor |
| /data_quality/max_ingest_lag_ms | integer | migrate to v2 descriptor |
| /data_quality/max_stale_book_ratio_bps | integer | migrate to v2 descriptor |
| /data_quality/max_execution_age_secs | integer | current execution-history staleness descriptor |
| /data_quality/reject_crossed_books | boolean | migrate to v2 descriptor |
| /data_quality/reject_empty_books | boolean | migrate to v2 descriptor |
| /reports/ad_hoc_default_knowledge_lag_secs | integer | migrate to v2 descriptor |
| /reports/ad_hoc_default_top_n | integer | migrate to v2 descriptor |
| /reports/ad_hoc_report_enabled | boolean | migrate to v2 descriptor |
| /reports/delivery_policy | enum | migrate to v2 descriptor |
| /reports/entry_window_ratio | string | migrate to v2 descriptor |
| /reports/fallback_horizon_secs | integer | delete; horizon comes from exact Route/Trade Policy contract |
| /reports/hard_candidate_ceiling | integer | migrate to v2 descriptor |
| /reports/max_top_n | integer | migrate to v2 descriptor |
| /selection/allow_near_resolution | boolean | migrate to v2 descriptor |
| /selection/enabled_categories | array<enum> | migrate to v2 descriptor |
| /selection/max_spread_bps | integer | migrate to v2 descriptor |
| /selection/max_time_to_resolution_secs | integer | migrate to v2 descriptor |
| /selection/min_liquidity_usd | string | migrate to v2 descriptor |
| /selection/min_time_to_resolution_secs | integer | migrate to v2 descriptor |
| /selection/min_volume_24h_usd | string | migrate to v2 descriptor |

### execution_risk_policy raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /breaker/cooldown_secs | integer | migrate to v2 descriptor |
| /breaker/daily_realized_loss_cap_usd | string | migrate to v2 descriptor |
| /breaker/venue_consecutive_failures_to_degrade | integer | migrate to v2 descriptor |
| /breaker/venue_consecutive_failures_to_halt | integer | migrate to v2 descriptor |
| /breaker/venue_error_rate_bps_to_halt | integer | migrate to v2 descriptor |
| /breaker/venue_min_window_samples | integer | migrate to v2 descriptor |
| /breaker/venue_window_secs | integer | migrate to v2 descriptor |
| /capital/max_open_intents | integer | migrate to v2 descriptor |
| /capital/max_reserved_usd | string | migrate to v2 descriptor |
| /entry_order_policy/max_slippage_bps | integer | migrate to v2 descriptor |
| /entry_order_policy/min_entry_book_depth_usd | string | migrate to v2 descriptor |
| /exit_monitor/enabled | boolean | migrate to v2 descriptor |
| /exit_monitor/monitor_secs | integer | migrate to v2 descriptor |
| /exit_monitor/opportunistic_sell/enabled | boolean | migrate to v2 descriptor |
| /exit_monitor/opportunistic_sell/shadow_mode | boolean | migrate to v2 descriptor |
| /exit_monitor/signal_recheck_secs | integer | migrate to v2 descriptor |
| /exit_monitor/signal_reinference/enabled | boolean | migrate to v2 descriptor |
| /exit_monitor/signal_reinference/shadow_mode | boolean | migrate to v2 descriptor |
| /portfolio/budget/max_single_recommendation_usd | string | migrate to v2 descriptor |
| /portfolio/budget/min_recommendation_usd | string | migrate to v2 descriptor |
| /portfolio/budget/total_budget_usd | string | migrate to v2 descriptor |
| /portfolio/constraints/correlation/cluster_threshold | string | delete/replace portfolio semantics |
| /portfolio/constraints/correlation/enabled | boolean | delete/replace portfolio semantics |
| /portfolio/constraints/correlation/lookback_days | integer | delete/replace portfolio semantics |
| /portfolio/constraints/correlation/min_observations | integer | delete/replace portfolio semantics |
| /portfolio/constraints/liquidity_usage_cap_pct | string | migrate to v2 descriptor |
| /portfolio/constraints/max_category_exposure_usd | string | migrate to v2 descriptor |
| /portfolio/constraints/max_correlated_exposure_usd | string | migrate to v2 descriptor |
| /portfolio/constraints/max_event_exposure_usd | string | migrate to v2 descriptor |
| /portfolio/constraints/max_market_exposure_usd | string | migrate to v2 descriptor |
| /portfolio/kelly_safety/binding_materiality_threshold | string | delete/replace portfolio semantics |
| /portfolio/kelly_safety/edge_uncertainty_floor | string | delete/replace portfolio semantics |
| /portfolio/kelly_safety/edge_uncertainty_k | string | delete/replace portfolio semantics |
| /portfolio/kelly_safety/max_aggregate_exposure_pct | string | delete/replace portfolio semantics |
| /portfolio/optimizer/integer_inclusion | boolean | delete/replace portfolio semantics |
| /portfolio/optimizer/objective_return_weight | string | delete/replace portfolio semantics |
| /portfolio/optimizer/solver | union | delete/replace portfolio semantics |
| /portfolio/sizing/confidence_weighting | enum | delete/replace portfolio semantics |
| /portfolio/sizing/drawdown_scaling | enum | delete/replace portfolio semantics |
| /portfolio/sizing/kelly_fraction | string | delete/replace portfolio semantics |
| /portfolio/sizing/max_position_pct | string | delete/replace portfolio semantics |
| /reconciliation/enabled | boolean | migrate to v2 descriptor |
| /reconciliation/interval_secs | integer | migrate to v2 descriptor |
| /reconciliation/stale_open_secs | integer | migrate to v2 descriptor |

### model_routing raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /model/active_exit_model_version_id | union | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/crypto/champion/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/crypto/shadow/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/pooled/champion/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/pooled/shadow/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/weather/champion/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/bound_at | string | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/config_revision | integer | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/generation | integer | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/model_version_id | string | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/source/source_kind | enum | migrate to v2 descriptor |
| /model/buy_routes/weather/shadow/source/feedback_cycle_id | string | migrate to v2 descriptor |
| /model/calibration/ci_confidence | string | migrate to v2 descriptor |
| /model/calibration/embargo_secs | integer | migrate to v2 descriptor |
| /model/calibration/method | union | migrate to v2 descriptor |
| /model/calibration/min_samples_isotonic | integer | migrate to v2 descriptor |
| /model/candidate_score_floor | string | migrate to v2 descriptor |
| /model/min_model_confidence | string | migrate to v2 descriptor |
| /model/shadow_diff_threshold | string | migrate to v2 descriptor |

### report_schedule raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /schedules/*/cadence/interval_secs | integer | migrate to v2 descriptor |
| /schedules/*/cadence/kind | enum | migrate to v2 descriptor |
| /schedules/*/cadence/expr | string | migrate to v2 descriptor |
| /schedules/*/cadence/timezone | string|null | migrate to v2 descriptor |
| /schedules/*/enabled | boolean | migrate to v2 descriptor |
| /schedules/*/knowledge_lag_secs | integer | migrate to v2 descriptor |
| /schedules/*/schedule_id | string | migrate to v2 descriptor |
| /schedules/*/top_n | integer | migrate to v2 descriptor |

### operational_control raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /entry_condition/backstop_interval_ms | integer | rename resource + redesign |
| /entry_condition/expiry_batch_limit | integer | rename resource + redesign |
| /entry_condition/lease_duration_secs | integer | rename resource + redesign |
| /entry_condition/lease_renew_interval_secs | integer | rename resource + redesign |
| /entry_condition/next_evaluation_delay_ms | integer | rename resource + redesign |
| /entry_condition/pass_limit | integer | rename resource + redesign |
| /kill_switch/emergency_exit/kind | union | rename resource + redesign |
| /kill_switch/emergency_exit/max_slippage_bps | integer | rename resource + redesign |
| /notifications/report_published | boolean | rename resource + redesign |
| /outcome_reconciliation/candidate_batch_size | integer | rename resource + redesign |
| /outcome_reconciliation/enabled | boolean | rename resource + redesign |
| /outcome_reconciliation/source_block_span | integer | rename resource + redesign |
| /outcome_reconciliation/sweep_secs | integer | rename resource + redesign |

### execution_authorization raw schema pointers

| RFC 6901 pointer | Current schema type | Target disposition |
|---|---|---|
| /auto_execution/max_orders_per_report | integer | rename resource + redesign |
| /auto_execution/max_total_usd_per_report | string | rename resource + redesign |
| /auto_execution/min_confidence | string | rename resource + redesign |
| /auto_execution/min_score | string | rename resource + redesign |
| /semi_auto/approval_ttl_secs | integer | rename resource + redesign |

## Deploy Config

Source-derived current leaf count: **310** across **61** config structs.
Dynamic maps use .*; array element contracts use []. Tagged enums are one descriptor path whose
variants must each receive a complete rendered example.

| # | TOML/descriptor path | Rust type | Current owner | Sensitivity | Target disposition |
|---:|---|---|---|---|---|
| 1 | deployment.environment | DeploymentEnvironment | deployment.rs | public | keep as required deploy descriptor |
| 2 | polymarket.clob_base_url | String | polymarket.rs | sensitive | keep as required deploy descriptor |
| 3 | polymarket.clob_ws_url | String | polymarket.rs | sensitive | keep as required deploy descriptor |
| 4 | polymarket.order_post_timeout_ms | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 5 | polymarket.clob_market_info_refresh_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 6 | polymarket.chain_id | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 7 | polymarket.onchain.rpc_endpoint | PolygonRpcEndpoint | polymarket.rs | public | keep as required deploy descriptor |
| 8 | polymarket.onchain.rpc_timeout_ms | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 9 | polymarket.relayer.base_url | String | polymarket.rs | sensitive | keep as required deploy descriptor |
| 10 | polymarket.relayer.api_key | Option<SecretText> | polymarket.rs | secret | keep as required deploy descriptor |
| 11 | polymarket.relayer.api_key_address | Option<String> | polymarket.rs | secret | keep as required deploy descriptor |
| 12 | polymarket.relayer.request_timeout_ms | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 13 | polymarket.settlement.claim_lease_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 14 | polymarket.settlement.semi_auto_authorization_ttl_secs | u64 | polymarket.rs | secret | keep as required deploy descriptor |
| 15 | polymarket.settlement.discovery_poll_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 16 | polymarket.settlement.submission_poll_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 17 | polymarket.settlement.max_claims_per_tick | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 18 | polymarket.settlement.rpc_concurrency | usize | polymarket.rs | public | keep as required deploy descriptor |
| 19 | polymarket.settlement.readiness_ui_cache_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 20 | polymarket.settlement.external_scan_block_span | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 21 | polymarket.settlement.retry_initial_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 22 | polymarket.settlement.retry_max_secs | u64 | polymarket.rs | public | keep as required deploy descriptor |
| 23 | market_data.websocket.reconnect_delay_ms | u64 | market_data.rs | public | keep as required deploy descriptor |
| 24 | market_data.websocket.max_reconnect_delay_ms | u64 | market_data.rs | public | keep as required deploy descriptor |
| 25 | market_data.websocket.max_subscriptions_per_connection | usize | market_data.rs | public | keep as required deploy descriptor |
| 26 | market_data.websocket.engine_max_subscription_tokens | usize | market_data.rs | public | keep as required deploy descriptor |
| 27 | market_data.websocket.engine_subscription_window_hours | u64 | market_data.rs | public | keep as required deploy descriptor |
| 28 | market_data.gamma.base_url | String | market_data.rs | sensitive | keep as required deploy descriptor |
| 29 | market_data.gamma.reconcile_interval_secs | u64 | market_data.rs | public | keep as required deploy descriptor |
| 30 | market_data.gamma.page_size | u32 | market_data.rs | public | keep as required deploy descriptor |
| 31 | market_data.gamma.max_keyset_pages | u32 | market_data.rs | public | keep as required deploy descriptor |
| 32 | market_data.gamma.max_keyset_requests | u32 | market_data.rs | public | keep as required deploy descriptor |
| 33 | market_data.data_api.base_url | String | market_data.rs | sensitive | keep as required deploy descriptor |
| 34 | market_data.data_api.page_size | u32 | market_data.rs | public | keep as required deploy descriptor |
| 35 | market_data.data_api.size_threshold | u32 | market_data.rs | public | keep as required deploy descriptor |
| 36 | market_data.finalized_exchange_history.enabled | bool | market_data.rs | public | required deploy descriptor |
| 37 | market_data.finalized_exchange_history.poll_secs | u64 | market_data.rs | public | required deploy descriptor |
| 38 | market_data.finalized_exchange_history.hypersync.provider_id | String | market_data.rs | public | required provider identity |
| 39 | market_data.finalized_exchange_history.hypersync.endpoint | String | market_data.rs | sensitive | primary extractor endpoint |
| 40 | market_data.finalized_exchange_history.hypersync.api_token | SecretText | market_data.rs | secret | primary extractor credential |
| 41 | market_data.finalized_exchange_history.attestor.provider_id | String | market_data.rs | public | independent witness identity |
| 42 | market_data.finalized_exchange_history.attestor.rpc_endpoint | PolygonRpcEndpoint | market_data.rs | sensitive | independent witness endpoint |
| 43 | market_data.finalized_exchange_history.connect_timeout_ms | u64 | market_data.rs | public | required deploy descriptor |
| 44 | market_data.finalized_exchange_history.request_timeout_ms | u64 | market_data.rs | public | required deploy descriptor |
| 45 | market_data.finalized_exchange_history.max_hypersync_response_body_bytes | usize | market_data.rs | public | required streamed HyperSync body budget |
| 46 | market_data.finalized_exchange_history.max_rpc_response_body_bytes | usize | market_data.rs | public | required streamed RPC body budget |
| 47 | market_data.finalized_exchange_history.max_canonical_chunk_bytes | usize | market_data.rs | public | required canonical chunk budget |
| 48 | domain_sources.binance.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 49 | domain_sources.binance.rest_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 50 | domain_sources.binance.websocket_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 51 | domain_sources.binance.archive_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 52 | domain_sources.binance.weight_budget_per_min | u32 | domain_sources.rs | public | keep as required deploy descriptor |
| 53 | domain_sources.binance.kline_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 54 | domain_sources.binance.agg_trade_recovery_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 55 | domain_sources.binance.websocket_rotation_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 56 | domain_sources.binance.batch_size | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 57 | domain_sources.binance.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 58 | domain_sources.binance.max_clock_skew_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 59 | domain_sources.binance_usdm_futures.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 60 | domain_sources.binance_usdm_futures.rest_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 61 | domain_sources.binance_usdm_futures.websocket_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 62 | domain_sources.binance_usdm_futures.archive_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 63 | domain_sources.binance_usdm_futures.weight_budget_per_min | u32 | domain_sources.rs | public | keep as required deploy descriptor |
| 64 | domain_sources.binance_usdm_futures.kline_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 65 | domain_sources.binance_usdm_futures.agg_trade_recovery_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 66 | domain_sources.binance_usdm_futures.websocket_rotation_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 67 | domain_sources.binance_usdm_futures.batch_size | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 68 | domain_sources.binance_usdm_futures.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 69 | domain_sources.binance_usdm_futures.max_clock_skew_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 70 | domain_sources.polymarket_rtds.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 71 | domain_sources.polymarket_rtds.websocket_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 72 | domain_sources.polymarket_rtds.connect_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 73 | domain_sources.polymarket_rtds.keepalive_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 74 | domain_sources.polymarket_rtds.max_clock_skew_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 75 | domain_sources.chainlink_data_streams.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 76 | domain_sources.chainlink_data_streams.rest_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 77 | domain_sources.chainlink_data_streams.websocket_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 78 | domain_sources.chainlink_data_streams.api_key | Option<SecretText> | domain_sources.rs | secret | keep as required deploy descriptor |
| 79 | domain_sources.chainlink_data_streams.api_secret | Option<SecretText> | domain_sources.rs | secret | keep as required deploy descriptor |
| 80 | domain_sources.chainlink_data_streams.feeds.*.feed_id | String | domain_sources.rs | public | keep as required deploy descriptor |
| 81 | domain_sources.chainlink_data_streams.feeds.*.decimals | u32 | domain_sources.rs | public | keep as required deploy descriptor |
| 82 | domain_sources.chainlink_data_streams.max_clock_skew_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 83 | domain_sources.chainlink_data_streams.rest_page_limit | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 84 | domain_sources.aviation_weather.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 85 | domain_sources.aviation_weather.base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 86 | domain_sources.aviation_weather.poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 87 | domain_sources.aviation_weather.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 88 | domain_sources.aviation_weather.day_close_grace_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 89 | domain_sources.ghcnh.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 90 | domain_sources.ghcnh.base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 91 | domain_sources.ghcnh.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 92 | domain_sources.ghcnh.refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 93 | domain_sources.ghcnh.calibration_years | u8 | domain_sources.rs | public | keep as required deploy descriptor |
| 94 | domain_sources.ghcnh.max_concurrency | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 95 | domain_sources.ghcnd.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 96 | domain_sources.ghcnd.base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 97 | domain_sources.ghcnd.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 98 | domain_sources.ghcnd.refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 99 | domain_sources.ghcnd.lookback_years | u8 | domain_sources.rs | public | keep as required deploy descriptor |
| 100 | domain_sources.ghcnd.max_concurrency | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 101 | domain_sources.gefs.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 102 | domain_sources.gefs.bucket_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 103 | domain_sources.gefs.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 104 | domain_sources.gefs.poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 105 | domain_sources.gefs.publication_lag_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 106 | domain_sources.gefs.max_lead_hours | u16 | domain_sources.rs | public | keep as required deploy descriptor |
| 107 | domain_sources.gefs.max_concurrency | usize | domain_sources.rs | public | keep as required deploy descriptor |
| 108 | domain_sources.hko_open_data.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 109 | domain_sources.hko_open_data.base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 110 | domain_sources.hko_open_data.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 111 | domain_sources.hko_open_data.daily_rainfall_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 112 | domain_sources.hko_open_data.daily_rainfall_lookback_days | u16 | domain_sources.rs | public | keep as required deploy descriptor |
| 113 | domain_sources.hko_open_data.daily_temperature_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 114 | domain_sources.hko_open_data.daily_temperature_lookback_months | u16 | domain_sources.rs | public | keep as required deploy descriptor |
| 115 | domain_sources.airnow.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 116 | domain_sources.airnow.reporting_area_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 117 | domain_sources.airnow.hourly_aq_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 118 | domain_sources.airnow.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 119 | domain_sources.airnow.poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 120 | domain_sources.airnow.correction_lookback_hours | u16 | domain_sources.rs | public | keep as required deploy descriptor |
| 121 | domain_sources.tornado.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 122 | domain_sources.tornado.spc_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 123 | domain_sources.tornado.ncei_csv_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 124 | domain_sources.tornado.ncei_time_series_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 125 | domain_sources.tornado.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 126 | domain_sources.tornado.spc_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 127 | domain_sources.tornado.ncei_refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 128 | domain_sources.tornado.ncei_time_series_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 129 | domain_sources.tornado.ncei_backfill_years | u8 | domain_sources.rs | public | keep as required deploy descriptor |
| 130 | domain_sources.nhc.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 131 | domain_sources.nhc.current_storms_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 132 | domain_sources.nhc.data_archive_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 133 | domain_sources.nhc.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 134 | domain_sources.nhc.advisory_poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 135 | domain_sources.nhc.best_track_refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 136 | domain_sources.nasa_gistemp.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 137 | domain_sources.nasa_gistemp.csv_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 138 | domain_sources.nasa_gistemp.annual_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 139 | domain_sources.nasa_gistemp.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 140 | domain_sources.nasa_gistemp.refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 141 | domain_sources.nsidc_sea_ice.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 142 | domain_sources.nsidc_sea_ice.north_daily_csv_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 143 | domain_sources.nsidc_sea_ice.south_daily_csv_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 144 | domain_sources.nsidc_sea_ice.north_monthly_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 145 | domain_sources.nsidc_sea_ice.south_monthly_base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 146 | domain_sources.nsidc_sea_ice.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 147 | domain_sources.nsidc_sea_ice.refresh_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 148 | domain_sources.nws_observation.enabled | bool | domain_sources.rs | public | keep as required deploy descriptor |
| 149 | domain_sources.nws_observation.base_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 150 | domain_sources.nws_observation.request_timeout_ms | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 151 | domain_sources.nws_observation.poll_secs | u64 | domain_sources.rs | public | keep as required deploy descriptor |
| 152 | domain_sources.nws_observation.lookback_observations | u16 | domain_sources.rs | public | keep as required deploy descriptor |
| 153 | domain_sources.weather_vertical_bindings.hko_rainfall[].site_key | String | domain_sources.rs | public | keep as required deploy descriptor |
| 154 | domain_sources.weather_vertical_bindings.hko_rainfall[].station_key | String | domain_sources.rs | public | keep as required deploy descriptor |
| 155 | domain_sources.weather_vertical_bindings.hko_rainfall[].daily_csv_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 156 | domain_sources.weather_vertical_bindings.hko_rainfall[].latitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 157 | domain_sources.weather_vertical_bindings.hko_rainfall[].longitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 158 | domain_sources.weather_vertical_bindings.hko_rainfall[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 159 | domain_sources.weather_vertical_bindings.hko_daily_temperature[].station | String | domain_sources.rs | public | keep as required deploy descriptor |
| 160 | domain_sources.weather_vertical_bindings.hko_daily_temperature[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 161 | domain_sources.weather_vertical_bindings.airnow_pm25_reporting_areas[].area | String | domain_sources.rs | public | keep as required deploy descriptor |
| 162 | domain_sources.weather_vertical_bindings.airnow_pm25_reporting_areas[].state | String | domain_sources.rs | public | keep as required deploy descriptor |
| 163 | domain_sources.weather_vertical_bindings.airnow_pm25_reporting_areas[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 164 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].contract_location | String | domain_sources.rs | public | keep as required deploy descriptor |
| 165 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].primary_resolution_url | String | domain_sources.rs | sensitive | keep as required deploy descriptor |
| 166 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].aqsid | String | domain_sources.rs | public | keep as required deploy descriptor |
| 167 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].site_name | String | domain_sources.rs | public | keep as required deploy descriptor |
| 168 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].state | String | domain_sources.rs | public | keep as required deploy descriptor |
| 169 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].latitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 170 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].longitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 171 | domain_sources.weather_vertical_bindings.airnow_pm25_sites[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 172 | domain_sources.weather_vertical_bindings.tornado_regions[].region_id | String | domain_sources.rs | public | keep as required deploy descriptor |
| 173 | domain_sources.weather_vertical_bindings.tornado_regions[].scope | TornadoRegionScopeConfig | domain_sources.rs | public | keep as required deploy descriptor |
| 174 | domain_sources.weather_vertical_bindings.tornado_regions[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 175 | domain_sources.weather_vertical_bindings.nhc_historical_storms[].basin | String | domain_sources.rs | public | keep as required deploy descriptor |
| 176 | domain_sources.weather_vertical_bindings.nhc_historical_storms[].storm_id | String | domain_sources.rs | public | keep as required deploy descriptor |
| 177 | domain_sources.weather_vertical_bindings.nws_wind_stations[].station | String | domain_sources.rs | public | keep as required deploy descriptor |
| 178 | domain_sources.weather_vertical_bindings.nws_wind_stations[].timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 179 | domain_sources.weather_stations.*.timezone | String | domain_sources.rs | public | keep as required deploy descriptor |
| 180 | domain_sources.weather_stations.*.latitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 181 | domain_sources.weather_stations.*.longitude | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 182 | domain_sources.weather_stations.*.elevation_meters | Decimal | domain_sources.rs | public | keep as required deploy descriptor |
| 183 | domain_sources.weather_stations.*.ghcnh_station_id | Option<String> | domain_sources.rs | public | keep as required deploy descriptor |
| 184 | domain_sources.weather_stations.*.ghcnd_station_id | Option<String> | domain_sources.rs | public | keep as required deploy descriptor |
| 185 | domain_sources.weather_stations.*.historical_binding_kind | WeatherHistoricalBindingKind | domain_sources.rs | public | keep as required deploy descriptor |
| 186 | observability.log_level | String | observability.rs | public | keep as required deploy descriptor |
| 187 | observability.log_json | bool | observability.rs | public | keep as required deploy descriptor |
| 188 | notifications.telegram.bot_token | SecretText | observability.rs | secret | keep as required deploy descriptor |
| 189 | notifications.telegram.chat_id | String | observability.rs | sensitive | keep as required deploy descriptor |
| 190 | notifications.webhook.url | SecretText | observability.rs | secret | keep as required deploy descriptor |
| 191 | notifications.webhook.authorization | SecretText | observability.rs | secret | keep as required deploy descriptor |
| 192 | db.postgres.host | String | db.rs | sensitive | keep as required deploy descriptor |
| 193 | db.postgres.port | u16 | db.rs | public | keep as required deploy descriptor |
| 194 | db.postgres.user | String | db.rs | sensitive | keep as required deploy descriptor |
| 195 | db.postgres.password | SecretText | db.rs | secret | keep as required deploy descriptor |
| 196 | db.postgres.database | String | db.rs | public | keep as required deploy descriptor |
| 197 | db.postgres.schema | String | db.rs | public | keep as required deploy descriptor |
| 198 | db.postgres.max_connections | u32 | db.rs | public | keep as required deploy descriptor |
| 199 | db.postgres.min_connections | u32 | db.rs | public | keep as required deploy descriptor |
| 200 | db.postgres.connect_timeout_secs | u64 | db.rs | public | keep as required deploy descriptor |
| 201 | db.postgres.idle_timeout_secs | u64 | db.rs | public | keep as required deploy descriptor |
| 202 | db.postgres.acquire_timeout_secs | u64 | db.rs | public | keep as required deploy descriptor |
| 203 | db.postgres.max_lifetime_secs | u64 | db.rs | public | keep as required deploy descriptor |
| 204 | db.postgres.statement_timeout_ms | u64 | db.rs | public | keep as required deploy descriptor |
| 205 | db.postgres.idle_in_transaction_timeout_ms | u64 | db.rs | public | keep as required deploy descriptor |
| 206 | db.postgres.lock_timeout_ms | u64 | db.rs | public | keep as required deploy descriptor |
| 207 | db.postgres.work_mem | String | db.rs | public | keep as required deploy descriptor |
| 208 | db.postgres.verify_session_params | bool | db.rs | public | keep as required deploy descriptor |
| 209 | db.postgres.statement_cache_capacity | u32 | db.rs | public | keep as required deploy descriptor |
| 210 | db.postgres.application_name | String | db.rs | public | keep as required deploy descriptor |
| 211 | db.clickhouse.deployment_id | String | db.rs | public | keep as required deploy descriptor |
| 212 | db.clickhouse.cluster_id | String | db.rs | public | keep as required deploy descriptor |
| 213 | db.clickhouse.url | String | db.rs | sensitive | keep as required deploy descriptor |
| 214 | db.clickhouse.database | String | db.rs | public | keep as required deploy descriptor |
| 215 | db.clickhouse.user | String | db.rs | sensitive | keep as required deploy descriptor |
| 216 | db.clickhouse.password | SecretText | db.rs | secret | keep as required deploy descriptor |
| 217 | db.clickhouse.flush_interval_secs | u64 | db.rs | public | keep as required deploy descriptor |
| 218 | db.clickhouse.batch_size | usize | db.rs | public | keep as required deploy descriptor |
| 219 | db.clickhouse.max_concurrent_inserts | usize | db.rs | public | keep as required deploy descriptor |
| 220 | cache.redis.host | String | cache.rs | sensitive | keep as required deploy descriptor |
| 221 | cache.redis.port | u16 | cache.rs | public | keep as required deploy descriptor |
| 222 | cache.redis.user | String | cache.rs | sensitive | keep as required deploy descriptor |
| 223 | cache.redis.password | SecretText | cache.rs | secret | keep as required deploy descriptor |
| 224 | cache.redis.database | u8 | cache.rs | public | keep as required deploy descriptor |
| 225 | cache.redis.pool_size | u32 | cache.rs | public | keep as required deploy descriptor |
| 226 | cache.redis.timeout_ms | u64 | cache.rs | public | keep as required deploy descriptor |
| 227 | cache.redis.connect_timeout_ms | u64 | cache.rs | public | keep as required deploy descriptor |
| 228 | cache.redis.key_prefix | String | cache.rs | public | keep as required deploy descriptor |
| 229 | cache.moka.max_capacity | u64 | cache.rs | public | keep as required deploy descriptor |
| 230 | cache.operation_timeout_ms | u64 | cache.rs | public | keep as required deploy descriptor |
| 231 | cache.fail_open | bool | cache.rs | public | keep as required deploy descriptor |
| 232 | cache.disabled | bool | cache.rs | public | keep as required deploy descriptor |
| 233 | cache.domains.*.timeout_ms | Option<u64> | cache.rs | public | keep as required deploy descriptor |
| 234 | cache.domains.*.fail_open | Option<bool> | cache.rs | public | keep as required deploy descriptor |
| 235 | cache.domains.*.disabled | bool | cache.rs | public | keep as required deploy descriptor |
| 236 | keys.private_key | Option<SecretText> | keys.rs | secret | keep as required deploy descriptor |
| 237 | web.listen_host | String | web.rs | sensitive | keep as required deploy descriptor |
| 238 | web.listen_port | u16 | web.rs | public | keep as required deploy descriptor |
| 239 | web.cors_allowed_origins[] | Vec<String> | web.rs | public | keep as required deploy descriptor |
| 240 | web.serve_static_ui | bool | web.rs | public | keep as required deploy descriptor |
| 241 | web.static_ui_dir | String | web.rs | public | keep as required deploy descriptor |
| 242 | web.password_crypto.max_in_flight | usize | web.rs | secret | keep as required deploy descriptor |
| 243 | web.password_crypto.deadline_ms | u64 | web.rs | secret | keep as required deploy descriptor |
| 244 | web.jwt.signing_key | SecretText | web.rs | secret | keep as required deploy descriptor |
| 245 | web.jwt.issuer | String | web.rs | sensitive | keep as required deploy descriptor |
| 246 | web.jwt.audience | String | web.rs | sensitive | keep as required deploy descriptor |
| 247 | web.jwt.access_ttl_secs | i64 | web.rs | public | keep as required deploy descriptor |
| 248 | web.jwt.refresh_ttl_secs | i64 | web.rs | public | keep as required deploy descriptor |
| 249 | web.jwt.absolute_session_ttl_secs | i64 | web.rs | public | keep as required deploy descriptor |
| 250 | quant.workers.report_schedule_poll_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 251 | quant.workers.report_run_lease_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 252 | quant.workers.report_run_heartbeat_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 253 | quant.workers.report_ad_hoc_queue_capacity | u64 | quant.rs | public | keep as required deploy descriptor |
| 254 | quant.workers.report_ad_hoc_queue_ttl_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 255 | quant.workers.report_expire_sweep_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 256 | quant.workers.intent_expire_sweep_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 257 | quant.workers.execution_dispatch_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 258 | quant.workers.execution_breaker_tick_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 259 | quant.workers.equity_snapshot_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 260 | quant.account.funder | Option<String> | quant.rs | sensitive | keep as required deploy descriptor |
| 261 | quant.account.wallet_kind | ExecutionWalletKind | quant.rs | public | keep as required deploy descriptor |
| 262 | quant.research_jobs.global_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 263 | quant.research_jobs.dataset_build_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 264 | quant.research_jobs.model_train_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 265 | quant.research_jobs.backtest_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 266 | quant.research_jobs.bias_table_fit_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 267 | quant.research_jobs.model_calibration_fit_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 268 | quant.research_jobs.cpcv_backtest_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 269 | quant.research_jobs.feature_parity_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 270 | quant.research_jobs.feature_parity_compute.page_size | u32 | quant.rs | public | keep as required deploy descriptor |
| 271 | quant.research_jobs.feature_parity_compute.max_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 272 | quant.research_jobs.feature_parity_compute.max_working_set_bytes | u64 | quant.rs | public | keep as required deploy descriptor |
| 273 | quant.research_jobs.feature_parity_compute.deadline_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 274 | quant.research_jobs.feedback_coverage_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 275 | quant.research_jobs.feedback_drift_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 276 | quant.research_jobs.feedback_learning_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 277 | quant.research_jobs.feedback_attribution_compute.page_size | u32 | quant.rs | public | keep as required deploy descriptor |
| 278 | quant.research_jobs.feedback_attribution_compute.max_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 279 | quant.research_jobs.feedback_attribution_compute.max_working_set_bytes | u64 | quant.rs | public | keep as required deploy descriptor |
| 280 | quant.research_jobs.feedback_attribution_compute.deadline_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 281 | quant.research_jobs.feedback_cycle_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 282 | quant.research_jobs.trade_policy_fit_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 283 | quant.research_jobs.trade_policy_validation_concurrency | usize | quant.rs | public | keep as required deploy descriptor |
| 284 | quant.research_jobs.lease_ttl_secs | i64 | quant.rs | public | keep as required deploy descriptor |
| 285 | quant.research_jobs.heartbeat_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 286 | quant.research_jobs.poll_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 287 | quant.research_jobs.max_recovery_attempts | i32 | quant.rs | public | keep as required deploy descriptor |
| 288 | quant.research_jobs.execution_retry_initial_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 289 | quant.research_jobs.execution_retry_max_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 290 | quant.research_jobs.max_spine_samples | u64 | quant.rs | public | keep as required deploy descriptor |
| 291 | quant.research_jobs.plan_sample_slices | u32 | quant.rs | public | keep as required deploy descriptor |
| 292 | quant.research_jobs.plan_sample_markets | u32 | quant.rs | public | keep as required deploy descriptor |
| 293 | quant.research_jobs.progress_min_interval_ms | u64 | quant.rs | public | keep as required deploy descriptor |
| 294 | quant.research_jobs.feedback_stuck_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 295 | quant.research_jobs.feedback_alert_timeout_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 296 | quant.research_jobs.feedback_alert_dedupe_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 297 | quant.research_jobs.shutdown_drain_secs | u64 | quant.rs | public | keep as required deploy descriptor |
| 298 | research.artifact_store.kind | ArtifactStoreKind | research.rs | public | keep as required deploy descriptor |
| 299 | research.artifact_store.bucket | String | research.rs | public | keep as required deploy descriptor |
| 300 | research.artifact_store.prefix | String | research.rs | public | keep as required deploy descriptor |
| 301 | research.artifact_store.region | String | research.rs | public | keep as required deploy descriptor |
| 302 | research.artifact_store.endpoint | Option<String> | research.rs | public | keep as required deploy descriptor |
| 303 | research.artifact_store.path_style | bool | research.rs | public | keep as required deploy descriptor |
| 304 | research.artifact_store.require_object_lock | bool | research.rs | public | keep as required deploy descriptor |
| 305 | research.artifact_store.require_versioning | bool | research.rs | public | keep as required deploy descriptor |
| 306 | research.evidence_attestation.signing_key | SecretText | research.rs | secret | keep as required deploy descriptor |
| 307 | research.evidence_attestation.previous_signing_keys[] | Vec<SecretText> | research.rs | secret | keep as required deploy descriptor |
| 308 | research.model_serving_registry.max_cached_contracts | u64 | research.rs | public | keep as required deploy descriptor |
| 309 | research.model_serving_registry.max_pending_loads | usize | research.rs | public | keep as required deploy descriptor |
| 310 | research.model_serving_registry.max_concurrent_loads | usize | research.rs | public | keep as required deploy descriptor |
| 311 | research.model_serving_registry.load_timeout_ms | u64 | research.rs | public | keep as required deploy descriptor |
| 312 | research.model_serving_registry.max_total_shadow_model_bytes | u64 | research.rs | public | keep as required deploy descriptor |

## W0 reconciliation rules

- Every current path must have one explicit target disposition; silent disappearance fails config audit.
- Every target Runtime editable pointer must render exactly one data-config-pointer control.
- Every target Deploy descriptor must render exactly once in both committed TOML profiles.
- Fields moved or deleted require a consumer/deletion test and a tombstone in the closure plan.
- The generated target inventory replaces this as-is inventory only after Rust/API/UI/TOML coverage gates pass.
