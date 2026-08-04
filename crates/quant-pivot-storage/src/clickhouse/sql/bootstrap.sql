CREATE TABLE IF NOT EXISTS book_microstructure_1m (
    `token_id` String,
    `market_id` Nullable(String),
    `bucket_time` DateTime64(3, 'UTC'),
    `best_bid_open` Nullable(Decimal(18, 8)),
    `best_bid_high` Nullable(Decimal(18, 8)),
    `best_bid_low` Nullable(Decimal(18, 8)),
    `best_bid_close` Nullable(Decimal(18, 8)),
    `best_ask_open` Nullable(Decimal(18, 8)),
    `best_ask_high` Nullable(Decimal(18, 8)),
    `best_ask_low` Nullable(Decimal(18, 8)),
    `best_ask_close` Nullable(Decimal(18, 8)),
    `spread_bps_min` Nullable(Decimal(18, 4)),
    `spread_bps_avg` Nullable(Decimal(18, 4)),
    `spread_bps_max` Nullable(Decimal(18, 4)),
    `mid_price_open` Nullable(Decimal(18, 8)),
    `mid_price_close` Nullable(Decimal(18, 8)),
    `top1_depth_usd_avg` Nullable(Decimal(38, 18)),
    `top5_depth_usd_avg` Nullable(Decimal(38, 18)),
    `top20_depth_usd_avg` Nullable(Decimal(38, 18)),
    `imbalance_avg` Nullable(Decimal(18, 8)),
    `update_count` UInt64,
    `snapshot_count` UInt64,
    `delta_count` UInt64,
    `delete_count` UInt64,
    `crossed_count` UInt64,
    `invalid_level_count` UInt64,
    `gap_count` UInt64,
    `last_trade_count` UInt64,
    `max_book_age_ms` UInt64,
    `schema_version` UInt32,
    `available_at` DateTime64(3, 'UTC') DEFAULT bucket_time,
    `bucket_date` Date MATERIALIZED toDate(bucket_time)
) ENGINE = MergeTree PARTITION BY toYYYYMM(bucket_date)
ORDER BY (token_id, bucket_time) SETTINGS index_granularity = 4096;
CREATE TABLE IF NOT EXISTS book_microstructure_1s (
    `token_id` String,
    `market_id` Nullable(String),
    `bucket_time` DateTime64(3, 'UTC'),
    `best_bid_open` Nullable(Decimal(18, 8)),
    `best_bid_high` Nullable(Decimal(18, 8)),
    `best_bid_low` Nullable(Decimal(18, 8)),
    `best_bid_close` Nullable(Decimal(18, 8)),
    `best_ask_open` Nullable(Decimal(18, 8)),
    `best_ask_high` Nullable(Decimal(18, 8)),
    `best_ask_low` Nullable(Decimal(18, 8)),
    `best_ask_close` Nullable(Decimal(18, 8)),
    `spread_bps_min` Nullable(Decimal(18, 4)),
    `spread_bps_avg` Nullable(Decimal(18, 4)),
    `spread_bps_max` Nullable(Decimal(18, 4)),
    `mid_price_open` Nullable(Decimal(18, 8)),
    `mid_price_close` Nullable(Decimal(18, 8)),
    `top1_depth_usd_avg` Nullable(Decimal(38, 18)),
    `top5_depth_usd_avg` Nullable(Decimal(38, 18)),
    `top20_depth_usd_avg` Nullable(Decimal(38, 18)),
    `imbalance_avg` Nullable(Decimal(18, 8)),
    `update_count` UInt64,
    `snapshot_count` UInt64,
    `delta_count` UInt64,
    `delete_count` UInt64,
    `crossed_count` UInt64,
    `invalid_level_count` UInt64,
    `gap_count` UInt64,
    `last_trade_count` UInt64,
    `max_book_age_ms` UInt64,
    `schema_version` UInt32,
    `available_at` DateTime64(3, 'UTC') DEFAULT bucket_time,
    `bucket_date` Date MATERIALIZED toDate(bucket_time)
) ENGINE = MergeTree PARTITION BY toYYYYMM(bucket_date)
ORDER BY (token_id, bucket_time) SETTINGS index_granularity = 4096;
CREATE TABLE IF NOT EXISTS market_resolution_event (
    `market_id` String,
    `token_ids` Array(String),
    `payout_ratios` Array(Decimal(20, 18)),
    `resolved_at` DateTime64(3, 'UTC'),
    `observed_at` DateTime64(3, 'UTC'),
    `source` Enum8(
        'WsSnapshot' = 1,
        'WsDelta' = 2,
        'WsBbo' = 3,
        'WsTickSize' = 4,
        'WsLastTrade' = 5,
        'ResolutionReconciliation' = 6,
        'QuantPipeline' = 7,
        'Execution' = 8,
        'WsShardStatus' = 9
    ),
    `source_block_number` UInt64,
    `source_block_hash` String,
    `source_transaction_hash` String,
    `source_log_index` UInt64,
    `source_checkpoint_hash` String,
    `resolution_fact_hash` String,
    `schema_version` UInt32,
    `resolved_date` Date MATERIALIZED toDate(resolved_at)
) ENGINE = MergeTree PARTITION BY toYYYYMM(resolved_date)
ORDER BY (
    market_id,
    resolved_at,
    observed_at,
    source_block_number,
    source_log_index,
    resolution_fact_hash
) SETTINGS non_replicated_deduplication_window = 10000, index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_book_l2_ledger (
    `stream_session_id` UUID,
    `shard_id` UInt32,
    `token_id` String,
    `market_id` Nullable(String),
    `token_sequence` UInt64,
    `event_type` Enum8(
        'Snapshot' = 1,
        'Delta' = 2,
        'TickSizeChange' = 3,
        'Gap' = 4,
        'LastTrade' = 5
    ),
    `bid_prices` Array(Decimal(18, 8)),
    `bid_sizes` Array(Decimal(38, 18)),
    `ask_prices` Array(Decimal(18, 8)),
    `ask_sizes` Array(Decimal(38, 18)),
    `old_tick_size` Nullable(Decimal(18, 8)),
    `new_tick_size` Nullable(Decimal(18, 8)),
    `trade_price` Nullable(Decimal(18, 8)),
    `trade_side` Nullable(Enum8('Unknown' = 0, 'Buy' = 1, 'Sell' = 2)),
    `trade_size` Nullable(Decimal(38, 18)),
    `fee_rate_bps` Nullable(Decimal(18, 4)),
    `venue_event_time` DateTime64(3, 'UTC'),
    `ingress_time` DateTime64(3, 'UTC'),
    `persisted_time` DateTime64(3, 'UTC'),
    `event_hash` FixedString(32),
    `schema_version` UInt32,
    `event_date` Date MATERIALIZED toDate(venue_event_time)
) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, stream_session_id, token_sequence) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_book_stream_session (
    `stream_session_id` UUID,
    `shard_id` UInt32,
    `ledger_sequence` UInt32,
    `state` Enum8('Open' = 1, 'Sealed' = 2, 'Invalidated' = 3),
    `end_reason` Enum8(
        'None' = 0,
        'Normal' = 1,
        'Resubscribe' = 2,
        'Overflow' = 3,
        'Disconnect' = 4,
        'Shutdown' = 5,
        'CrashRecovery' = 6
    ),
    `subscription_token_hash` String,
    `subscription_token_count` UInt32,
    `received_sequence_json` String CODEC(ZSTD(3)),
    `persisted_sequence_json` String CODEC(ZSTD(3)),
    `opened_at` DateTime64(3, 'UTC'),
    `recorded_at` DateTime64(3, 'UTC'),
    `schema_version` UInt32,
    `session_date` Date MATERIALIZED toDate(opened_at)
) ENGINE = MergeTree PARTITION BY toYYYYMM(session_date)
ORDER BY (stream_session_id, ledger_sequence) SETTINGS index_granularity = 4096;
CREATE TABLE IF NOT EXISTS quant_capital_allocation_event (
    `event_time` DateTime64(3, 'UTC'),
    `capital_allocation_id` String,
    `order_intent_id` String,
    `recommendation_id` String,
    `event_kind` Enum8(
        'submitted' = 1,
        'submission_result' = 2,
        'exit_submitted' = 3,
        'exit_submission_result' = 4,
        'reconciled' = 5,
        'operator_resolved' = 6,
        'unresolvable' = 7,
        'settlement_redeem_confirmed' = 8,
        'opened' = 9
    ),
    `state` Enum8(
        'allocated' = 1,
        'locked' = 2,
        'spent' = 3,
        'released' = 4,
        'impaired' = 5
    ),
    `allocated_usd` Decimal(38, 18),
    `locked_usd` Decimal(38, 18),
    `spent_usd` Decimal(38, 18),
    `released_usd` Decimal(38, 18),
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_crypto_price_report (
    `source_id` LowCardinality(String),
    `instrument_key` String,
    `source_sequence` UInt64,
    `price` Decimal(18, 8),
    `quantity` Nullable(Decimal(18, 8)),
    `event_time` DateTime64(3, 'UTC'),
    `published_at` DateTime64(3, 'UTC'),
    `available_at` DateTime64(3, 'UTC'),
    `valid_from` Nullable(DateTime64(3, 'UTC')),
    `observations_timestamp` Nullable(DateTime64(3, 'UTC')),
    `expires_at` Nullable(DateTime64(3, 'UTC')),
    `report_hash` String,
    `raw_report` String,
    `schema_version` UInt32
) ENGINE = MergeTree PARTITION BY toYYYYMM(event_time)
ORDER BY (
        source_id,
        instrument_key,
        source_sequence,
        event_time,
        report_hash
    ) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_domain_event (
    `event_id` UUID,
    `source` String,
    `event_type` LowCardinality(String),
    `subject` String,
    `event_time` DateTime64(3, 'UTC'),
    `published_at` DateTime64(3, 'UTC'),
    `available_at` DateTime64(3, 'UTC'),
    `schema_version` UInt32,
    `revision` UInt32,
    `supersedes_event_id` Nullable(UUID),
    `payload_hash` String,
    `source_checkpoint_hash` String,
    `payload_json` String
) ENGINE = ReplacingMergeTree(available_at) PARTITION BY toYYYYMM(event_time)
ORDER BY (
        subject,
        event_type,
        event_time,
        revision,
        event_id
    ) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_domain_observation (
    `family` LowCardinality(String),
    `source_id` LowCardinality(String),
    `instrument_key` LowCardinality(String),
    `metric` LowCardinality(String),
    `value` Decimal(18, 8),
    `event_time` DateTime64(3, 'UTC'),
    `publish_time` DateTime64(3, 'UTC'),
    `ingestion_time` DateTime64(3, 'UTC'),
    `schema_version` UInt32,
    `event_date` Date MATERIALIZED toDate(event_time)
) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date)
ORDER BY (instrument_key, metric, event_time) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_entry_condition_evaluation_event (
    `evaluation_id` String,
    `condition_instance_id` UUID,
    `base_revision` Int64,
    `applied_revision` Nullable(Int64),
    `trace_kind` LowCardinality(String),
    `evaluator_version` UInt32,
    `evaluated_at` DateTime64(3, 'UTC'),
    `state` LowCardinality(String),
    `truth` LowCardinality(String),
    `evaluation_hash` String,
    `input_fingerprint` String,
    `continuity_hash` String,
    `tree_json` String,
    `schema_version` UInt32
) ENGINE = ReplacingMergeTree(evaluated_at) PARTITION BY toYYYYMM(evaluated_at)
ORDER BY (condition_instance_id, evaluation_id) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_execution_event (
    `event_time` DateTime64(3, 'UTC'),
    `order_intent_id` String,
    `execution_order_id` String,
    `recommendation_id` String,
    `event_kind` Enum8(
        'submitted' = 1,
        'submission_result' = 2,
        'exit_submitted' = 3,
        'exit_submission_result' = 4,
        'reconciled' = 5,
        'operator_resolved' = 6,
        'unresolvable' = 7,
        'settlement_redeem_confirmed' = 8,
        'opened' = 9
    ),
    `market_id` String,
    `token_id` String,
    `side` Enum8('buy' = 1, 'sell' = 2),
    `price` Decimal(18, 8),
    `shares` Decimal(38, 18),
    `cost_usd` Decimal(38, 18),
    `venue_order_id` Nullable(String),
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_exit_signal_evaluation_event (
    `event_time` DateTime64(3, 'UTC'),
    `order_intent_id` String,
    `position_id` String,
    `market_id` String,
    `token_id` String,
    `evaluator_kind` Enum8('reinference' = 1, 'opportunistic' = 2),
    `verdict` Enum8(
        'thesis_invalidated' = 1,
        'opportunistic_sell' = 2,
        'holds' = 3,
        'indeterminate' = 4
    ),
    `model_version_id` Nullable(String),
    `mark_price` Nullable(Decimal(18, 8)),
    `entry_composite_score` Decimal(18, 8),
    `fresh_composite_score` Nullable(Decimal(18, 8)),
    `exit_alpha_bps` Nullable(Decimal(18, 8)),
    `confidence` Nullable(Decimal(18, 8)),
    `target_cumulative_exit_pct` Nullable(Decimal(18, 8)),
    `shadow` UInt8,
    `detail` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_factor_event (
    `event_time` DateTime64(3, 'UTC'),
    `decision_at` DateTime64(3, 'UTC'),
    `market_id` String,
    `factor_name` LowCardinality(String),
    `factor_family` LowCardinality(String),
    `value_state` Enum8(
        'scored' = 1,
        'missing_input' = 2,
        'not_applicable' = 3,
        'indeterminate' = 4
    ),
    `raw_value` Nullable(Decimal(18, 8)),
    `normalized_score` Nullable(Decimal(18, 8)),
    `normalization_source` Nullable(
        Enum8(
            'cross_section' = 1,
            'per_market' = 2,
            'frozen_reference_quantile' = 3
        )
    ),
    `confidence` Decimal(18, 8),
    `direction` Enum8('negative' = -1, 'neutral' = 0, 'positive' = 1),
    `model_run_id` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (
        model_run_id,
        market_id,
        factor_name,
        decision_at
    ) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_feature_event (
    `event_time` DateTime64(3, 'UTC'),
    `feature_vector_id` String,
    `decision_policy_snapshot_id` String,
    `decision_at` DateTime64(3, 'UTC'),
    `knowledge_cutoff` DateTime64(3, 'UTC'),
    `per_source_cutoffs_json` String,
    `market_id` String,
    `token_id` Nullable(String),
    `feature_schema_version` UInt32,
    `feature_schema_hash` String,
    `feature_hash` String,
    `decision_capture_hash` String,
    `feature_name` LowCardinality(String),
    `cell_state` Enum8(
        'observed' = 1,
        'substituted' = 2,
        'missing' = 3,
        'not_applicable' = 4
    ),
    `raw_value` Nullable(String),
    `value_kind` Enum8(
        'decimal' = 0,
        'probability' = 1,
        'bps' = 2,
        'usd' = 3,
        'count' = 4,
        'bool' = 5,
        'category' = 6
    ),
    `source_kind` Enum8(
        'book' = 1,
        'gamma_metadata' = 2,
        'clickhouse_fact' = 3,
        'trade_tape' = 4,
        'derived' = 5,
        'domain_crypto' = 6,
        'linkage' = 7,
        'domain_weather' = 8
    ),
    `evidence_source_kind` Nullable(
        Enum8(
            'book' = 1,
            'gamma_metadata' = 2,
            'clickhouse_fact' = 3,
            'trade_tape' = 4,
            'derived' = 5,
            'domain_crypto' = 6,
            'linkage' = 7,
            'domain_weather' = 8
        )
    ),
    `evidence_reference` Nullable(String),
    `evidence_effective_at` Nullable(DateTime64(3, 'UTC')),
    `evidence_available_at` Nullable(DateTime64(3, 'UTC')),
    `reason` LowCardinality(Nullable(String)),
    `staleness_ms` Nullable(UInt64),
    `data_quality` LowCardinality(String),
    `audit_fingerprint` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (
        feature_vector_id,
        feature_name,
        decision_at,
        ingestion_time
    ) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_feature_parity_event (
    `event_time` DateTime64(3, 'UTC'),
    `parity_event_id` String,
    `parity_run_id` String,
    `decision_at` DateTime64(3, 'UTC'),
    `stage` LowCardinality(String),
    `status` LowCardinality(String),
    `report_id` Nullable(String),
    `model_run_id` Nullable(String),
    `model_version_id` Nullable(String),
    `training_dataset_id` Nullable(String),
    `market_id` Nullable(String),
    `feature_name` Nullable(String),
    `reason` Nullable(String),
    `online_state` LowCardinality(Nullable(String)),
    `replay_state` LowCardinality(Nullable(String)),
    `online_value` Nullable(String),
    `replay_value` Nullable(String),
    `online_effective_at` Nullable(DateTime64(3, 'UTC')),
    `online_available_at` Nullable(DateTime64(3, 'UTC')),
    `online_cutoff` Nullable(DateTime64(3, 'UTC')),
    `replay_effective_at` Nullable(DateTime64(3, 'UTC')),
    `replay_available_at` Nullable(DateTime64(3, 'UTC')),
    `replay_cutoff` Nullable(DateTime64(3, 'UTC')),
    `feature_contract_hash` String,
    `transform_hash` String,
    `online_fingerprint` String,
    `replay_fingerprint` String,
    `detail_json` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(ingestion_time)
ORDER BY (parity_run_id, parity_event_id) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_model_input_event (
    `event_time` DateTime64(3, 'UTC'),
    `format_version` UInt32,
    `decision_at` DateTime64(3, 'UTC'),
    `knowledge_cutoff` DateTime64(3, 'UTC'),
    `model_run_id` String,
    `model_version_id` String,
    `recommendation_report_id` Nullable(String),
    `market_id` String,
    `feature_vector_id` String,
    `model_family` LowCardinality(String),
    `raw_input_name` LowCardinality(String),
    `raw_state` LowCardinality(String),
    `raw_value` Nullable(String),
    `encoded_column` LowCardinality(String),
    `encoded_value_bits` Nullable(UInt64),
    `input_contract_hash` String,
    `transform_hash` String,
    `training_input_hash` String,
    `audit_fingerprint` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (
        model_run_id,
        feature_vector_id,
        market_id,
        encoded_column,
        decision_at,
        ingestion_time
    ) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_position_event (
    `event_time` DateTime64(3, 'UTC'),
    `position_id` String,
    `order_intent_id` String,
    `market_id` String,
    `token_id` String,
    `event_kind` Enum8(
        'submitted' = 1,
        'submission_result' = 2,
        'exit_submitted' = 3,
        'exit_submission_result' = 4,
        'reconciled' = 5,
        'operator_resolved' = 6,
        'unresolvable' = 7,
        'settlement_redeem_confirmed' = 8,
        'opened' = 9
    ),
    `state` Enum8(
        'open' = 1,
        'closing' = 2,
        'closed' = 3,
        'settled' = 4
    ),
    `side` Enum8('yes' = 1, 'no' = 2),
    `shares` Decimal(38, 18),
    `avg_price` Decimal(18, 8),
    `cost_usd` Decimal(38, 18),
    `realized_pnl_usd` Decimal(38, 18),
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (market_id, token_id, event_time, ingestion_time) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_report_market_funnel (
    `event_time` DateTime64(3, 'UTC'),
    `recommendation_report_id` String,
    `market_selection_id` String,
    `profile_id` LowCardinality(String),
    `profile_version` UInt32,
    `profile_content_hash` String,
    `decision_policy_snapshot_id` String,
    `model_version_id` String,
    `model_run_id` Nullable(String),
    `market_id` String,
    `event_id` String,
    `primary_token_id` String,
    `terminal_stage` LowCardinality(String),
    `primary_reason` LowCardinality(String),
    `secondary_diagnostics_json` String,
    `feature_vector_id` Nullable(String),
    `signal_candidate_id` Nullable(String),
    `recommendation_id` Nullable(String),
    `row_hash` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(ingestion_time) PARTITION BY toYYYYMM(event_time)
ORDER BY (recommendation_report_id, market_id) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_report_recommendation_fact (
    `event_time` DateTime64(3, 'UTC'),
    `recommendation_report_id` String,
    `recommendation_id` String,
    `rank` UInt32,
    `market_id` String,
    `token_id` String,
    `side` Enum8('yes' = 1, 'no' = 2),
    `score` Decimal(18, 8),
    `risk_adjusted_score` Decimal(18, 8),
    `trade_plan_available` Bool,
    `suggested_usd` Nullable(Decimal(38, 18)),
    `valid_until` DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (recommendation_report_id, recommendation_id) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_serving_evidence_completion (
    `event_time` DateTime64(3, 'UTC'),
    `format_version` UInt32,
    `model_run_id` String,
    `decision_at` DateTime64(3, 'UTC'),
    `knowledge_cutoff` DateTime64(3, 'UTC'),
    `feature_vector_ids_json` String,
    `expected_feature_row_count` UInt64,
    `feature_rows_hash` String,
    `expected_model_input_row_count` UInt64,
    `model_input_rows_hash` String,
    `completion_hash` String,
    `ingestion_time` DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY (model_run_id, ingestion_time) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_signal_candidate_event (
    `event_time` DateTime64(3, 'UTC'),
    `signal_candidate_id` String,
    `model_run_id` String,
    `market_id` String,
    `token_id` String,
    `side` Enum8('yes' = 1, 'no' = 2),
    `score` Decimal(18, 8),
    `confidence` Decimal(18, 8),
    `entry_price` Decimal(18, 8),
    `target_price` Decimal(18, 8),
    `stop_price` Decimal(18, 8),
    `rank_before_portfolio` UInt32,
    `rejection_reason` LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (model_run_id, rank_before_portfolio, market_id) SETTINGS index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_trade_tape (
    `market_id` String,
    `token_id` String,
    `event_time` DateTime64(3, 'UTC'),
    `ingestion_time` DateTime64(3, 'UTC'),
    `stream_session_id` Nullable(UUID),
    `token_sequence` Nullable(UInt64),
    `participant_address` String,
    `participant_role` Enum8('Maker' = 1, 'Taker' = 2, 'Unknown' = 3),
    `side` Enum8('Buy' = 1, 'Sell' = 2, 'Unknown' = 3),
    `price` Decimal(18, 8),
    `size_shares` Decimal(38, 18),
    `notional_usd` Decimal(38, 18),
    `tx_hash` Nullable(String),
    `source_event_id` String,
    `source` Enum8('MarketWs' = 1, 'OnChainOrderFilled' = 2),
    `observed_field_flags` UInt16,
    `fee_rate_bps` Nullable(Decimal(18, 4)),
    `reconciliation_status` Enum8(
        'Pending' = 1,
        'Matched' = 2,
        'Unavailable' = 3,
        'Ambiguous' = 4,
        'OnChainOnly' = 5
    ),
    `matched_source_event_id` Nullable(String),
    `revision` UInt32,
    `reconciled_at` Nullable(DateTime64(3, 'UTC')),
    `raw_payload_json` Nullable(String) CODEC(ZSTD(3)),
    `schema_version` UInt32,
    `event_date` Date MATERIALIZED toDate(event_time)
) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date)
ORDER BY (
        market_id,
        token_id,
        event_time,
        source_event_id,
        participant_address
    ) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE MATERIALIZED VIEW IF NOT EXISTS quant_book_l2_trade_tape_mv TO quant_trade_tape AS
SELECT assumeNotNull(market_id) AS market_id,
    token_id,
    venue_event_time AS event_time,
    persisted_time AS ingestion_time,
    toNullable(stream_session_id) AS stream_session_id,
    toNullable(token_sequence) AS token_sequence,
    '' AS participant_address,
    CAST(
        'Unknown',
        'Enum8(\'Maker\' = 1, \'Taker\' = 2, \'Unknown\' = 3)'
    ) AS participant_role,
    multiIf(
        isNull(trade_side),
        CAST(
            'Unknown',
            'Enum8(\'Buy\' = 1, \'Sell\' = 2, \'Unknown\' = 3)'
        ),
        assumeNotNull(trade_side) = 'Buy',
        CAST(
            'Buy',
            'Enum8(\'Buy\' = 1, \'Sell\' = 2, \'Unknown\' = 3)'
        ),
        CAST(
            'Sell',
            'Enum8(\'Buy\' = 1, \'Sell\' = 2, \'Unknown\' = 3)'
        )
    ) AS side,
    assumeNotNull(trade_price) AS price,
    ifNull(trade_size, toDecimal128(0, 18)) AS size_shares,
    CAST(
        assumeNotNull(trade_price) * ifNull(trade_size, toDecimal128(0, 18)),
        'Decimal(38, 18)'
    ) AS notional_usd,
    CAST(NULL, 'Nullable(String)') AS tx_hash,
    concat('blake3:', lower(hex(event_hash))) AS source_event_id,
    CAST(
        'MarketWs',
        'Enum8(\'MarketWs\' = 1, \'OnChainOrderFilled\' = 2)'
    ) AS source,
    toUInt16(
        135 + if(isNotNull(trade_side), 32, 0) + if(isNotNull(trade_size), 256, 0) + if(isNotNull(fee_rate_bps), 512, 0)
    ) AS observed_field_flags,
    fee_rate_bps,
    if(
        isNotNull(trade_side)
        AND isNotNull(trade_size),
        CAST(
            'Pending',
            'Enum8(\'Pending\' = 1, \'Matched\' = 2, \'Unavailable\' = 3, \'Ambiguous\' = 4, \'OnChainOnly\' = 5)'
        ),
        CAST(
            'Unavailable',
            'Enum8(\'Pending\' = 1, \'Matched\' = 2, \'Unavailable\' = 3, \'Ambiguous\' = 4, \'OnChainOnly\' = 5)'
        )
    ) AS reconciliation_status,
    CAST(NULL, 'Nullable(String)') AS matched_source_event_id,
    toUInt32(1) AS revision,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS reconciled_at,
    CAST(NULL, 'Nullable(String)') AS raw_payload_json,
    toUInt32(1) AS schema_version
FROM quant_book_l2_ledger
WHERE event_type = 'LastTrade'
    AND isNotNull(market_id)
    AND isNotNull(trade_price);
CREATE TABLE IF NOT EXISTS quant_weather_forecast_fact (
    `source_id` LowCardinality(String),
    `instrument_key` String,
    `subject_key` LowCardinality(String),
    `variable` LowCardinality(String),
    `value` Decimal(18, 8),
    `unit` LowCardinality(String),
    `precision` Decimal(18, 8),
    `reference_time` DateTime64(3, 'UTC'),
    `valid_time` DateTime64(3, 'UTC'),
    `published_at` DateTime64(3, 'UTC'),
    `available_at` DateTime64(3, 'UTC'),
    `lead_hours` UInt16,
    `member` Nullable(UInt16),
    `revision` UInt32,
    `grid_binding_hash` String,
    `run_manifest_hash` String,
    `report_hash` String,
    `schema_version` UInt32
) ENGINE = MergeTree PARTITION BY toYYYYMM(reference_time)
ORDER BY (
        source_id,
        instrument_key,
        variable,
        reference_time,
        valid_time,
        ifNull(member, 65535),
        revision,
        report_hash
    ) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE TABLE IF NOT EXISTS quant_weather_observation_fact (
    `source_id` LowCardinality(String),
    `instrument_key` String,
    `subject_key` LowCardinality(String),
    `local_date` Int32,
    `report_kind` LowCardinality(String),
    `variable` LowCardinality(String),
    `value` Decimal(18, 8),
    `unit` LowCardinality(String),
    `precision` Decimal(18, 8),
    `observed_at` Int64,
    `valid_from` Nullable(Int64),
    `valid_to` Nullable(Int64),
    `published_at` DateTime64(3, 'UTC'),
    `available_at` DateTime64(3, 'UTC'),
    `revision` UInt32,
    `report_hash` String,
    `supersedes_report_hash` Nullable(String),
    `raw_report` String,
    `schema_version` UInt32
) ENGINE = MergeTree PARTITION BY intDiv(local_date, 3660)
ORDER BY (
        source_id,
        instrument_key,
        variable,
        observed_at,
        revision,
        report_hash
    ) SETTINGS non_replicated_deduplication_window = 10000,
    index_granularity = 8192;
CREATE MATERIALIZED VIEW IF NOT EXISTS book_microstructure_1m_mv TO book_microstructure_1m (
    `token_id` String,
    `market_id` Nullable(String),
    `bucket_time` DateTime('UTC'),
    `best_bid_open` Nullable(Decimal(18, 8)),
    `best_bid_high` Nullable(Decimal(18, 8)),
    `best_bid_low` Nullable(Decimal(18, 8)),
    `best_bid_close` Nullable(Decimal(18, 8)),
    `best_ask_open` Nullable(Decimal(18, 8)),
    `best_ask_high` Nullable(Decimal(18, 8)),
    `best_ask_low` Nullable(Decimal(18, 8)),
    `best_ask_close` Nullable(Decimal(18, 8)),
    `spread_bps_min` Nullable(Decimal(18, 4)),
    `spread_bps_avg` Nullable(Float64),
    `spread_bps_max` Nullable(Decimal(18, 4)),
    `mid_price_open` Nullable(Decimal(18, 8)),
    `mid_price_close` Nullable(Decimal(18, 8)),
    `top1_depth_usd_avg` Nullable(Float64),
    `top5_depth_usd_avg` Nullable(Float64),
    `top20_depth_usd_avg` Nullable(Float64),
    `imbalance_avg` Nullable(Float64),
    `update_count` UInt64,
    `snapshot_count` UInt64,
    `delta_count` UInt64,
    `delete_count` UInt64,
    `crossed_count` UInt64,
    `invalid_level_count` UInt64,
    `gap_count` UInt64,
    `last_trade_count` UInt64,
    `max_book_age_ms` UInt64,
    `schema_version` UInt32,
    `available_at` DateTime64(3, 'UTC')
) AS
SELECT token_id,
    any(market_id) AS market_id,
    toStartOfMinute(bucket_time) AS bucket_time,
    argMin(best_bid_open, bucket_time) AS best_bid_open,
    max(best_bid_high) AS best_bid_high,
    min(best_bid_low) AS best_bid_low,
    argMax(best_bid_close, bucket_time) AS best_bid_close,
    argMin(best_ask_open, bucket_time) AS best_ask_open,
    max(best_ask_high) AS best_ask_high,
    min(best_ask_low) AS best_ask_low,
    argMax(best_ask_close, bucket_time) AS best_ask_close,
    min(spread_bps_min) AS spread_bps_min,
    avg(spread_bps_avg) AS spread_bps_avg,
    max(spread_bps_max) AS spread_bps_max,
    argMin(mid_price_open, bucket_time) AS mid_price_open,
    argMax(mid_price_close, bucket_time) AS mid_price_close,
    avg(top1_depth_usd_avg) AS top1_depth_usd_avg,
    avg(top5_depth_usd_avg) AS top5_depth_usd_avg,
    avg(top20_depth_usd_avg) AS top20_depth_usd_avg,
    avg(imbalance_avg) AS imbalance_avg,
    sum(update_count) AS update_count,
    sum(snapshot_count) AS snapshot_count,
    sum(delta_count) AS delta_count,
    sum(delete_count) AS delete_count,
    sum(crossed_count) AS crossed_count,
    sum(invalid_level_count) AS invalid_level_count,
    sum(gap_count) AS gap_count,
    sum(last_trade_count) AS last_trade_count,
    max(max_book_age_ms) AS max_book_age_ms,
    max(schema_version) AS schema_version,
    max(available_at) AS available_at
FROM book_microstructure_1s
GROUP BY token_id,
    bucket_time;
