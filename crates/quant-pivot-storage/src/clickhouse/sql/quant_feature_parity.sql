CREATE TABLE IF NOT EXISTS quant_model_input_event
(
    event_time DateTime64(3, 'UTC'),
    format_version UInt32,
    decision_at DateTime64(3, 'UTC'),
    knowledge_cutoff DateTime64(3, 'UTC'),
    model_run_id String,
    model_version_id String,
    recommendation_report_id Nullable(String),
    market_id String,
    feature_vector_id String,
    model_family LowCardinality(String),
    raw_input_name LowCardinality(String),
    raw_state LowCardinality(String),
    raw_value Nullable(String),
    encoded_column LowCardinality(String),
    encoded_value_bits Nullable(UInt64),
    input_contract_hash String,
    transform_hash String,
    training_input_hash String,
    audit_fingerprint String,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (model_run_id, feature_vector_id, market_id, encoded_column, decision_at, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_serving_evidence_completion
(
    event_time DateTime64(3, 'UTC'),
    format_version UInt32,
    model_run_id String,
    decision_at DateTime64(3, 'UTC'),
    knowledge_cutoff DateTime64(3, 'UTC'),
    feature_vector_ids_json String,
    expected_feature_row_count UInt64,
    feature_rows_hash String,
    expected_model_input_row_count UInt64,
    model_input_rows_hash String,
    completion_hash String,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (model_run_id, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_feature_parity_event
(
    event_time DateTime64(3, 'UTC'),
    parity_event_id String,
    parity_run_id String,
    decision_at DateTime64(3, 'UTC'),
    stage LowCardinality(String),
    status LowCardinality(String),
    report_id Nullable(String),
    model_run_id Nullable(String),
    model_version_id Nullable(String),
    training_dataset_id Nullable(String),
    market_id Nullable(String),
    feature_name Nullable(String),
    reason Nullable(String),
    online_state LowCardinality(Nullable(String)),
    replay_state LowCardinality(Nullable(String)),
    online_value Nullable(String),
    replay_value Nullable(String),
    online_effective_at Nullable(DateTime64(3, 'UTC')),
    online_available_at Nullable(DateTime64(3, 'UTC')),
    online_cutoff Nullable(DateTime64(3, 'UTC')),
    replay_effective_at Nullable(DateTime64(3, 'UTC')),
    replay_available_at Nullable(DateTime64(3, 'UTC')),
    replay_cutoff Nullable(DateTime64(3, 'UTC')),
    feature_contract_hash String,
    transform_hash String,
    online_fingerprint String,
    replay_fingerprint String,
    detail_json String,
    ingestion_time DateTime64(3, 'UTC')
)
-- `parity_event_id` binds status and fingerprints, so distinct lifecycle
-- evidence has distinct keys. ReplacingMergeTree only deduplicates byte-
-- equivalent retry writes of the same immutable event identity.
ENGINE = ReplacingMergeTree(ingestion_time)
ORDER BY (parity_run_id, parity_event_id);
