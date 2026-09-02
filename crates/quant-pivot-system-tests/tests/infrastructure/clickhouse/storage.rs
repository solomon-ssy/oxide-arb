//! `ClickHouse` storage and schema system contracts.

use std::{slice, sync::Arc, time::Duration};

use crate::infrastructure_removal_catalog::CLICKHOUSE_REMOVED_TABLES_QUERY;
use chrono::Utc;
use clickhouse::Client;
use prometheus::IntCounter;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, ChBps, ChDecimal64, ChDigest, ChPrice, ChSchemaVersion, ChShares, ChUsd,
        CryptoPriceReportRow, DomainEventRow, MarketExecutionRow, QuantReportRecommendationFactRow,
    },
    config::ClickHouseConfig,
    enums::clickhouse::{
        ChAvailabilityBasis, ChCanonicalBookEventType, ChExchangeSide, ChExchangeVersion,
        ChLedgerTradeSide, ChOutcomeSide,
    },
    hashing::CanonicalDigest,
    types::{
        DomainInstrumentKey, DomainSourceId, EconomicTierId, MarketId, Price, RecommendationId,
        RecommendationReportId, ReportRouteRunId, Shares, TokenId, Usd,
    },
};
use quant_pivot_storage::{
    clickhouse::{
        ChWriteManager, ClickHousePool, ClickHouseQueryLimits, active_preproduction_query_count,
        bootstrap_schema, reset_preproduction_database, verify_schema,
    },
    write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability, AsyncWriterWorker},
};
use quant_pivot_system_tests::resources::fresh_clickhouse_config;
use rust_decimal_macros::dec;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const QUERY_LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(clickhouse::Row, serde::Deserialize)]
struct BootstrapObjectCounts {
    residue_count: u64,
    bootstrap_count: u64,
}

fn maintenance_config(config: &ClickHouseConfig) -> ClickHouseConfig {
    ClickHouseConfig {
        database: "default".to_owned(),
        ..config.clone()
    }
}

async fn setup_clickhouse() -> (ClickHousePool, Client, ClickHouseConfig, ()) {
    let config = fresh_clickhouse_config("storage");
    bootstrap_schema(&config)
        .await
        .expect("bootstrap fresh schema");
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.verify_schema().await.expect("verify schema");
    let client = pool.client().clone();
    (pool, client, config, ())
}

pub async fn fresh_bootstrap_creates_schema() {
    let config = fresh_clickhouse_config("first_deployment");

    let missing = verify_schema(&config)
        .await
        .expect_err("read-only startup must not create a missing database");
    assert!(missing.to_string().contains("does not exist"));

    let status = bootstrap_schema(&config)
        .await
        .expect("fresh schema bootstrap");
    assert!(status.required_object_count > 0);
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.verify_schema().await.expect("verify schema");

    let count: u64 = pool
        .client()
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind(&config.database)
        .fetch_one()
        .await
        .expect("database should exist");
    assert_eq!(count, 1);
}

pub async fn deployment_runtime_rejects_held() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    client
        .query(
            "CREATE TABLE quant_pivot_schema_deployment_lock (owner String) \
             ENGINE = TinyLog COMMENT 'test-deploy-owner'",
        )
        .execute()
        .await
        .expect("install deployment lock");

    let deploy_error = bootstrap_schema(&config)
        .await
        .expect_err("fresh bootstrap must not run while its lock is held");
    assert!(deploy_error.to_string().contains("deployment lock"));

    let runtime_error = verify_schema(&config)
        .await
        .expect_err("runtime must not start while schema deployment is active");
    assert!(runtime_error.to_string().contains("deployment lock"));
}

pub async fn bootstrap_rejects_nonempty() {
    let config = fresh_clickhouse_config("unmanaged_database");
    let maintenance = ClickHousePool::from_config(&maintenance_config(&config))
        .client()
        .clone();
    let create_database_sql = format!("CREATE DATABASE `{}`", config.database);
    maintenance
        .query(&create_database_sql)
        .execute()
        .await
        .expect("create unmanaged database");
    let client = ClickHousePool::from_config(&config).client().clone();
    client
        .query(
            "CREATE TABLE legacy_quant_recommendation_event \
             (event_time DateTime64(3), payload String) \
             ENGINE = MergeTree ORDER BY event_time",
        )
        .execute()
        .await
        .expect("create unmanaged legacy object");
    client
        .query(
            "INSERT INTO legacy_quant_recommendation_event VALUES \
             (now64(3), 'must-not-be-adopted-or-deleted')",
        )
        .execute()
        .await
        .expect("insert unmanaged legacy fact");

    let boot_error = bootstrap_schema(&config)
        .await
        .expect_err("nonempty unmanaged database must block clean boot");
    assert!(
        boot_error
            .to_string()
            .contains("requires an object-empty database")
    );
    let legacy_count: u64 = client
        .query("SELECT count() FROM legacy_quant_recommendation_event")
        .fetch_one()
        .await
        .expect("legacy rows remain intact");
    let boot_object_count: u64 = client
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = 'book_microstructure_1m'",
        )
        .fetch_one()
        .await
        .expect("fresh-bootstrap absence probe");
    assert_eq!(legacy_count, 1);
    assert_eq!(boot_object_count, 0);
}

pub async fn partial_bootstrap_is_rejected() {
    let config = fresh_clickhouse_config("partial_bootstrap");
    let maintenance = ClickHousePool::from_config(&maintenance_config(&config))
        .client()
        .clone();
    maintenance
        .query(&format!("CREATE DATABASE `{}`", config.database))
        .execute()
        .await
        .expect("create partial-bootstrap database");
    let client = ClickHousePool::from_config(&config).client().clone();
    client
        .query("CREATE TABLE partial_bootstrap_marker (value UInt8) ENGINE = TinyLog")
        .execute()
        .await
        .expect("create partial-bootstrap residue");

    let error = bootstrap_schema(&config)
        .await
        .expect_err("partial schema must never be resumed");
    assert!(
        error
            .to_string()
            .contains("requires an object-empty database")
    );
    let counts = client
        .query(
            "SELECT \
             countIf(name = 'partial_bootstrap_marker') AS residue_count, \
             countIf(name = 'book_microstructure_1m') AS bootstrap_count \
             FROM system.tables WHERE database = currentDatabase()",
        )
        .fetch_one::<BootstrapObjectCounts>()
        .await
        .expect("inspect rejected partial bootstrap");
    assert_eq!(counts.residue_count, 1);
    assert_eq!(counts.bootstrap_count, 0);
}

pub async fn preproduction_rejects_without_dropping() {
    let mut config = fresh_clickhouse_config("preproduction_reset");
    "quant_pivot".clone_into(&mut config.database);
    let maintenance = ClickHousePool::from_config(&maintenance_config(&config))
        .client()
        .clone();
    let create_database_sql = format!("CREATE DATABASE `{}`", config.database);
    maintenance
        .query(&create_database_sql)
        .execute()
        .await
        .expect("create reset target");
    let target = ClickHousePool::from_config(&config).client().clone();
    target
        .query("CREATE TABLE reset_marker (value UInt8) ENGINE = TinyLog")
        .execute()
        .await
        .expect("create reset marker");
    let active_client = target.clone();
    let active_query = tokio::spawn(async move {
        active_client
            .query("SELECT sleep(2)")
            .fetch_one::<u8>()
            .await
    });

    let observation_deadline = Instant::now() + QUERY_LIFECYCLE_TIMEOUT;
    loop {
        let active_queries = active_preproduction_query_count(&config)
            .await
            .expect("inspect active ClickHouse queries");
        if active_queries != 0 {
            break;
        }
        assert!(
            Instant::now() < observation_deadline,
            "the active target query did not become observable within {} seconds",
            QUERY_LIFECYCLE_TIMEOUT.as_secs()
        );
        sleep(QUERY_LIFECYCLE_POLL_INTERVAL).await;
    }

    let error = reset_preproduction_database(&config)
        .await
        .expect_err("an active target query must deny reset");
    assert!(error.to_string().contains("active project queries"));
    let marker_count: u64 = maintenance
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = ? AND name = 'reset_marker'",
        )
        .bind(&config.database)
        .fetch_one()
        .await
        .expect("inspect preserved reset marker");
    assert_eq!(marker_count, 1);
    active_query
        .await
        .expect("join active query")
        .expect("active query completes");

    let quiescence_deadline = Instant::now() + QUERY_LIFECYCLE_TIMEOUT;
    loop {
        let active_queries = active_preproduction_query_count(&config)
            .await
            .expect("inspect stopped ClickHouse query");
        if active_queries == 0 {
            break;
        }
        assert!(
            Instant::now() < quiescence_deadline,
            "{active_queries} ClickHouse queries remained observable {} seconds after the owner \
             task completed",
            QUERY_LIFECYCLE_TIMEOUT.as_secs()
        );
        sleep(QUERY_LIFECYCLE_POLL_INTERVAL).await;
    }

    reset_preproduction_database(&config)
        .await
        .expect("reset succeeds after the active owner stops");
    let database_count: u64 = maintenance
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind(&config.database)
        .fetch_one()
        .await
        .expect("inspect recreated reset target");
    let marker_count: u64 = maintenance
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = ? AND name = 'reset_marker'",
        )
        .bind(&config.database)
        .fetch_one()
        .await
        .expect("inspect removed reset marker");
    assert_eq!(database_count, 1);
    assert_eq!(marker_count, 0);
}

pub async fn clickhouse_health_check() {
    let (pool, client, config, _stack) = setup_clickhouse().await;
    pool.health_check().await.expect("health check should pass");
    let max_threads: u64 = client
        .query("SELECT toUInt64(getSetting('max_threads'))")
        .fetch_one()
        .await
        .expect("read ClickHouse max_threads setting");
    assert_eq!(
        max_threads,
        u64::try_from(config.max_threads_per_query).expect("max_threads fits u64")
    );
    let health_wait = pool
        .read_metrics()
        .admission_wait_seconds
        .get_metric_with_label_values(&["ch.storage.health.v1"])
        .expect("health read-admission histogram");
    let health_inflight = pool
        .read_metrics()
        .permits_used
        .get_metric_with_label_values(&["ch.storage.health.v1"])
        .expect("health read-admission gauge");
    assert!(health_wait.get_sample_count() >= 2);
    assert_eq!(health_inflight.get(), 0);
}

pub async fn second_bootstrap_is_rejected() {
    let (pool, _client, config, _stack) = setup_clickhouse().await;
    let error = bootstrap_schema(&config)
        .await
        .expect_err("an initialized schema must reject a second bootstrap");
    assert!(
        error
            .to_string()
            .contains("requires an object-empty database")
    );
    pool.verify_schema().await.expect("schema remains valid");
}

pub async fn removed_tables_absent() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let residue_count: u64 = client
        .query(CLICKHOUSE_REMOVED_TABLES_QUERY)
        .fetch_one()
        .await
        .expect("inspect removed ClickHouse tables");
    assert_eq!(
        residue_count, 0,
        "fresh ClickHouse catalog retained removed tables"
    );
}

pub async fn native_query_limits_rejects() {
    let (pool, _client, _config, _stack) = setup_clickhouse().await;
    let row_error = ClickHouseQueryLimits::new("ch.system.row_overflow", 1, 1_024)
        .query(&pool, "SELECT number FROM numbers(2)")
        .fetch_all::<u64>()
        .await
        .expect_err("row overflow must fail instead of truncating");
    assert!(
        row_error.to_string().contains("TOO_MANY_ROWS_OR_BYTES"),
        "unexpected row-overflow error: {row_error}"
    );

    let byte_error = ClickHouseQueryLimits::new("ch.system.byte_overflow", 1_000, 1)
        .query(&pool, "SELECT repeat('x', 1024) FROM numbers(100)")
        .fetch_all::<String>()
        .await
        .expect_err("byte overflow must fail instead of truncating");
    assert!(
        byte_error.to_string().contains("TOO_MANY_ROWS_OR_BYTES"),
        "unexpected byte-overflow error: {byte_error}"
    );
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TableDdl {
    statement: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct CanonicalInsertLog {
    query: String,
    async_insert: String,
}

pub async fn canonical_evidence_no_ttl() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;

    for table in [
        "quant_book_l2_ledger",
        "quant_book_stream_session",
        "quant_market_execution",
    ] {
        let ddl: TableDdl = client
            .query(&format!("SHOW CREATE TABLE {table}"))
            .fetch_one()
            .await
            .unwrap_or_else(|e| panic!("SHOW CREATE TABLE {table} failed: {e}"));
        assert!(
            !ddl.statement.contains(" TTL "),
            "{table} must be seal-first"
        );
    }
}

pub async fn canonical_insert_is_synchronous() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    let query_id = Uuid::now_v7().to_string();
    let token_id = TokenId::new(format!("canonical-sync-{query_id}"));
    let now = Utc::now().timestamp_millis();
    let row = BookL2LedgerRow {
        stream_session_id: Uuid::now_v7(),
        shard_id: 0,
        token_id: token_id.clone(),
        market_id: Some(MarketId::new(format!("market-{query_id}"))),
        token_sequence: 1,
        event_type: ChCanonicalBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(dec!(0.4)))],
        bid_sizes: vec![ChShares::from(Shares::new(dec!(2)))],
        ask_prices: vec![ChPrice::from(Price::new(dec!(0.6)))],
        ask_sizes: vec![ChShares::from(Shares::new(dec!(3)))],
        old_tick_size: None,
        new_tick_size: None,
        trade_price: None,
        trade_side: None,
        trade_size: None,
        fee_rate_bps: None,
        trade_transaction_hash: None,
        venue_event_time: now,
        ingress_time: now + 1,
        persisted_time: now + 2,
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()
    .expect("seal canonical synchronous fixture");
    let identified_client = client
        .clone()
        .with_setting("query_id", query_id.clone())
        .with_setting("insert_deduplicate", "0");

    ChWriteManager::new(2, &config.io)
        .write_canonical_ledger(&identified_client, slice::from_ref(&row))
        .await
        .expect("write canonical ledger through production manager");
    let retry_client = client
        .clone()
        .with_setting("query_id", format!("{query_id}-retry"))
        .with_setting("insert_deduplicate", "0");
    ChWriteManager::new(2, &config.io)
        .write_canonical_ledger(&retry_client, slice::from_ref(&row))
        .await
        .expect("retry canonical ledger through production manager");

    let observation_deadline = Instant::now() + QUERY_LIFECYCLE_TIMEOUT;
    let logged = loop {
        client
            .query("SYSTEM FLUSH LOGS")
            .execute()
            .await
            .expect("flush ClickHouse system logs");
        let logged = client
            .query(
                "SELECT query, Settings['async_insert'] AS async_insert \
                 FROM system.query_log \
                 WHERE query_id = ? AND type = 'QueryFinish' AND query_kind = 'Insert' \
                 ORDER BY event_time_microseconds DESC LIMIT 1",
            )
            .bind(&query_id)
            .fetch_optional::<CanonicalInsertLog>()
            .await
            .expect("inspect canonical insert query settings");
        if let Some(logged) = logged {
            break logged;
        }
        assert!(
            Instant::now() < observation_deadline,
            "canonical INSERT query_id {query_id} did not reach system.query_log"
        );
        sleep(QUERY_LIFECYCLE_POLL_INTERVAL).await;
    };

    assert!(
        logged.query.starts_with("INSERT INTO") && logged.query.contains("quant_book_l2_ledger"),
        "identified query was not the canonical ledger INSERT: {}",
        logged.query
    );
    assert_eq!(
        logged.async_insert, "0",
        "canonical ledger INSERT must override every server/profile default"
    );
    let persisted_count: u64 = client
        .query("SELECT count() FROM quant_book_l2_ledger WHERE token_id = ?")
        .bind(token_id.as_str())
        .fetch_one()
        .await
        .expect("read durable canonical fixture");
    assert_eq!(
        persisted_count, 1,
        "production manager must override a client-level dedupe=0 setting for exact retries"
    );
}

pub async fn resource_governance_is_exact() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let forbidden_count: u64 = client
        .query(
            "SELECT count() FROM system.tables WHERE database = 'system' \
             AND name IN ('metric_log', 'asynchronous_metric_log', 'text_log', 'trace_log', \
             'processors_profile_log', 'query_thread_log', 'query_views_log', \
             'query_metric_log', 'part_log', 'background_schedule_pool_log', \
             'asynchronous_insert_log')",
        )
        .fetch_one()
        .await
        .expect("inspect forbidden ClickHouse system logs");
    let query_log_count: u64 = client
        .query("SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_log'")
        .fetch_one()
        .await
        .expect("inspect retained ClickHouse query log");
    let merge_tree_setting_count: u64 = client
        .query(
            "SELECT count() FROM system.merge_tree_settings WHERE \
             (name = 'number_of_free_entries_in_pool_to_execute_mutation' AND value = '1') OR \
             (name = 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' \
              AND value = '1') OR \
             (name = 'number_of_free_entries_in_pool_to_lower_max_size_of_merge' AND value = '1')",
        )
        .fetch_one()
        .await
        .expect("inspect ClickHouse MergeTree governance settings");
    assert_eq!(forbidden_count, 0);
    assert_eq!(query_log_count, 1);
    assert_eq!(merge_tree_setting_count, 3);
}

pub async fn runtime_schema_rejects_ttl() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    client
        .query(
            "ALTER TABLE quant_book_l2_ledger \
             MODIFY TTL venue_event_time + INTERVAL 200 DAY DELETE \
             SETTINGS materialize_ttl_after_modify = 0",
        )
        .execute()
        .await
        .expect("install unmanaged TTL");

    let error = verify_schema(&config)
        .await
        .expect_err("runtime contract must reject unmanaged TTL");
    assert!(error.to_string().contains("unmanaged table TTL"));
}

pub async fn runtime_schema_rejects_drift() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    client
        .query("CREATE TABLE unmanaged_runtime_probe (value UInt8) ENGINE = TinyLog")
        .execute()
        .await
        .expect("install unmanaged runtime object");

    let error = verify_schema(&config)
        .await
        .expect_err("runtime contract must reject unmanaged objects");
    assert!(error.to_string().contains("outside the boot manifest"));
    assert!(error.to_string().contains("unmanaged_runtime_probe"));
}

pub async fn runtime_verification_rejects_drift() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    client
        .query(
            "ALTER TABLE quant_market_execution \
             RENAME COLUMN builder TO unmanaged_builder",
        )
        .execute()
        .await
        .expect("install semantic column drift");

    let error = verify_schema(&config)
        .await
        .expect_err("runtime contract must reject unmanaged columns");
    assert!(error.to_string().contains("semantic schema drift"));
    assert!(error.to_string().contains("quant_market_execution"));
}

pub async fn clickhouse_fact_uses_columns() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let expected: [(&str, &[&str]); 8] = [
        (
            "quant_book_l2_ledger",
            &[
                "`bid_prices` Array(Decimal(18, 8))",
                "`bid_sizes` Array(Decimal(38, 18))",
                "`token_sequence` UInt64",
                "`event_hash` FixedString(32)",
            ],
        ),
        (
            "quant_signal_candidate_event",
            &[
                "`entry_price` Decimal(18, 8)",
                "`score` Decimal(18, 8)",
                "`confidence` Decimal(18, 8)",
            ],
        ),
        (
            "quant_execution_event",
            &[
                "`price` Decimal(18, 8)",
                "`shares` Decimal(38, 18)",
                "`cost_usd` Decimal(38, 18)",
            ],
        ),
        (
            "quant_model_input_event",
            &[
                "`raw_state` LowCardinality(String)",
                "`encoded_value_bits` Nullable(UInt64)",
                "`audit_fingerprint` String",
            ],
        ),
        (
            "quant_feature_parity_event",
            &[
                "`stage` LowCardinality(String)",
                "`online_state` LowCardinality(Nullable(String))",
                "`replay_state` LowCardinality(Nullable(String))",
                "ReplacingMergeTree(ingestion_time)",
            ],
        ),
        (
            "quant_market_execution",
            &["ENGINE = MergeTree", "model_available_at"],
        ),
        (
            "quant_domain_observation",
            &["ENGINE = MergeTree", "ingestion_time"],
        ),
        (
            "quant_crypto_price_report",
            &[
                "`gap_generation` UInt64",
                "ORDER BY (source_id, instrument_key, gap_generation, source_sequence",
            ],
        ),
    ];

    for (table, fragments) in expected {
        let ddl: TableDdl = client
            .query(&format!("SHOW CREATE TABLE {table}"))
            .fetch_one()
            .await
            .unwrap_or_else(|e| panic!("SHOW CREATE TABLE {table} failed: {e}"));
        for fragment in fragments {
            assert!(
                ddl.statement.contains(fragment),
                "table {table} should contain `{fragment}`; got:\n{}",
                ddl.statement
            );
        }
        if matches!(table, "quant_market_execution" | "quant_domain_observation") {
            assert!(
                !ddl.statement.contains("ReplacingMergeTree"),
                "PIT source table {table} must retain every ingested revision"
            );
        }
    }
}

pub async fn crypto_price_matches_schema() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let now = Utc::now().timestamp_millis();
    let row = CryptoPriceReportRow {
        source_id: DomainSourceId::binance(),
        instrument_key: DomainInstrumentKey::new("BINANCE_AGG_TRADE:BTCUSDT"),
        gap_generation: 3,
        source_sequence: 1,
        price: ChDecimal64::from(dec!(50000)),
        quantity: Some(ChDecimal64::from(dec!(0.01))),
        event_time: now,
        published_at: now,
        available_at: now,
        valid_from: None,
        observations_timestamp: None,
        expires_at: None,
        report_hash: CanonicalDigest::content_hash_json(&serde_json::json!({
            "source": "integration-test",
            "sequence": 1,
        }))
        .expect("canonical report hash"),
        raw_report: r#"{"test":true}"#.to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };

    let mut insert = client
        .insert::<CryptoPriceReportRow>("quant_crypto_price_report")
        .await
        .expect("start crypto report insert");
    insert
        .write(&row)
        .await
        .expect("serialize crypto report row");
    insert.end().await.expect("commit crypto report row");

    let count: u64 = client
        .query(
            "SELECT count() FROM quant_crypto_price_report \
             WHERE source_id = 'binance' AND gap_generation = 3 AND source_sequence = 1",
        )
        .fetch_one()
        .await
        .expect("read inserted crypto report");
    assert_eq!(count, 1);
}

pub async fn domain_event_matches_schema() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let now = Utc::now().timestamp_millis();
    let row = DomainEventRow {
        event_id: Uuid::now_v7(),
        source: "binance".to_owned(),
        event_type: "crypto.price_transition".to_owned(),
        subject: "BINANCE_AGG_TRADE:BTCUSDT".to_owned(),
        event_time: now,
        published_at: now,
        available_at: now,
        schema_version: ChSchemaVersion::FIRST,
        revision: 1,
        supersedes_event_id: Some(Uuid::now_v7()),
        payload_hash: CanonicalDigest::content_hash_json(&serde_json::json!({
            "event": "integration-test",
        }))
        .expect("canonical payload hash"),
        source_checkpoint_hash: CanonicalDigest::content_hash_json(&serde_json::json!({
            "checkpoint": 1,
        }))
        .expect("canonical checkpoint hash"),
        payload_json: r#"{"test":true}"#.to_owned(),
    };

    let mut insert = client
        .insert::<DomainEventRow>("quant_domain_event")
        .await
        .expect("start domain event insert");
    insert
        .write(&row)
        .await
        .expect("serialize domain event row");
    insert.end().await.expect("commit domain event row");

    let count: u64 = client
        .query("SELECT count() FROM quant_domain_event")
        .fetch_one()
        .await
        .expect("read inserted domain event");
    assert_eq!(count, 1);
}

pub async fn report_fact_accepts_snapshot() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let lifecycle_status_columns: u64 = client
        .query(
            "SELECT count() FROM system.columns \
             WHERE database = currentDatabase() \
             AND table = 'quant_report_recommendation_fact' AND name = 'status'",
        )
        .fetch_one()
        .await
        .expect("inspect recommendation decision schema");
    assert_eq!(
        lifecycle_status_columns, 0,
        "immutable recommendation facts must not mirror live lifecycle status"
    );
    let now = Utc::now().timestamp_millis();
    let report_id = RecommendationReportId::from_v7();
    let recommendation_id = RecommendationId::from_v7();
    let decision = QuantReportRecommendationFactRow {
        event_time: now,
        recommendation_report_id: report_id,
        recommendation_id,
        report_route_run_id: ReportRouteRunId::from_v7(),
        economic_tier_id: EconomicTierId::from_v7(),
        route: "pooled".to_owned(),
        rank: 1,
        market_id: MarketId::new("report-fact-schema-market"),
        token_id: TokenId::new("report-fact-schema-token"),
        side: ChOutcomeSide::Yes,
        profit_probability_bps: 7_200,
        nominal_expected_net_usd: ChUsd::from(Usd::new(dec!(18))),
        robust_expected_net_usd: ChUsd::from(Usd::new(dec!(12))),
        max_loss_usd: ChUsd::from(Usd::new(dec!(25))),
        cvar_contribution_usd: ChUsd::from(Usd::new(dec!(10))),
        capital_occupancy_usd_hours: ChUsd::from(Usd::new(dec!(25))),
        marginal_portfolio_value_usd: ChUsd::from(Usd::new(dec!(11))),
        hard_reserved_cash_usd: ChUsd::from(Usd::new(dec!(25))),
        valid_until: now + 60_000,
    };
    let mut decision_insert = client
        .insert::<QuantReportRecommendationFactRow>("quant_report_recommendation_fact")
        .await
        .expect("start recommendation decision insert");
    decision_insert
        .write(&decision)
        .await
        .expect("write recommendation decision");
    decision_insert
        .end()
        .await
        .expect("commit recommendation decision");

    let decision_count: u64 = client
        .query(
            "SELECT count() FROM quant_report_recommendation_fact \
             WHERE recommendation_report_id = ? AND recommendation_id = ?",
        )
        .bind(report_id)
        .bind(recommendation_id)
        .fetch_one()
        .await
        .expect("read recommendation decision");
    assert_eq!(decision_count, 1);

    let ddl: TableDdl = client
        .query("SHOW CREATE TABLE quant_report_recommendation_fact")
        .fetch_one()
        .await
        .expect("show recommendation decision fact");
    assert!(
        !ddl.statement.contains("`status`"),
        "immutable recommendation decision fact must not mirror PG lifecycle"
    );
}

fn sample_execution(token_id: &str, received_at: i64) -> MarketExecutionRow {
    let digest = ChDigest::new(*blake3::hash(token_id.as_bytes()).as_bytes());
    MarketExecutionRow {
        execution_id: digest,
        match_id: None,
        maker_order_filled_event_id: digest,
        market_id: MarketId::new("market-integration"),
        token_id: TokenId::new(token_id),
        order_hash: format!("0x{}", "b".repeat(64)),
        contract_key: "ctf_exchange_v2".to_owned(),
        exchange_version: ChExchangeVersion::V2,
        contract_address: "0xE111180000d2663C0091e4f400237545B87B996B".to_owned(),
        transaction_hash: format!("0x{}", "a".repeat(64)),
        block_number: u64::try_from(received_at).unwrap_or_default(),
        transaction_index: 0,
        log_index: 0,
        maker_address: format!("0x{}", "1".repeat(40)),
        taker_address: format!("0x{}", "2".repeat(40)),
        side: ChExchangeSide::Buy,
        price: ChPrice::from(Price::new(dec!(0.95))),
        size_shares: ChShares::from(Shares::new(dec!(10))),
        notional_usd: ChUsd::from(Usd::new(dec!(9.5))),
        fee_usd: ChUsd::from(Usd::ZERO),
        builder: None,
        effective_at: received_at,
        observed_at: received_at,
        model_available_at: received_at,
        availability_basis: ChAvailabilityBasis::BlockConfirmation,
        availability_policy_hash: digest,
        chunk_id: Uuid::from_u128(1),
        schema_version: MarketExecutionRow::SCHEMA_VERSION,
    }
}

pub async fn execution_history_direct_roundtrip() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let row = sample_execution("tok-direct", Utc::now().timestamp_millis());
    let mut insert = client
        .insert::<MarketExecutionRow>("quant_market_execution")
        .await
        .expect("insert start");
    insert.write(&row).await.expect("write row");
    insert.end().await.expect("end insert");

    let count: u64 = client
        .query("SELECT count() FROM quant_market_execution WHERE token_id = 'tok-direct'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 1);
}

pub async fn last_trade_is_signal() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    let now = Utc::now().timestamp_millis();
    let stream_session_id = Uuid::now_v7();
    let row = BookL2LedgerRow {
        stream_session_id,
        shard_id: 3,
        token_id: TokenId::new("tok-ledger-last-trade"),
        market_id: Some(MarketId::new("market-ledger-last-trade")),
        token_sequence: 41,
        event_type: ChCanonicalBookEventType::LastTrade,
        bid_prices: Vec::new(),
        bid_sizes: Vec::new(),
        ask_prices: Vec::new(),
        ask_sizes: Vec::new(),
        old_tick_size: None,
        new_tick_size: None,
        trade_price: Some(ChPrice::from(Price::new(dec!(0.4115)))),
        trade_side: Some(ChLedgerTradeSide::Sell),
        trade_size: Some(ChShares::from(Shares::new(dec!(9)))),
        fee_rate_bps: Some(ChBps::from(dec!(2.5))),
        trade_transaction_hash: None,
        venue_event_time: now,
        ingress_time: now + 1,
        persisted_time: now + 2,
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()
    .expect("seal LastTrade ledger row");
    let write_manager = ChWriteManager::new(2, &config.io);

    for _ in 0..2 {
        write_manager
            .write_canonical_ledger(&client, slice::from_ref(&row))
            .await
            .expect("synchronous deduplicated ledger insert");
    }

    let ledger_count: u64 = client
        .query(
            "SELECT count() FROM quant_book_l2_ledger \
             WHERE token_id = 'tok-ledger-last-trade'",
        )
        .fetch_one()
        .await
        .expect("count deduplicated LastTrade ledger rows");
    let trade_count: u64 = client
        .query(
            "SELECT count() FROM quant_market_execution \
             WHERE token_id = 'tok-ledger-last-trade'",
        )
        .fetch_one()
        .await
        .expect("count materialized LastTrade rows");
    assert_eq!(ledger_count, 1, "retried ledger block must deduplicate");
    assert_eq!(
        trade_count, 0,
        "CLOB LastTrade is reconciliation-only and must not become an authoritative execution"
    );
}

/// Build a tick-event `AsyncWriter` whose flush sink is `ChWriteManager::write_batch`.
fn execution_writer(
    client: Client,
    write_manager: Arc<ChWriteManager>,
) -> (
    AsyncWriter<MarketExecutionRow>,
    AsyncWriterWorker<MarketExecutionRow>,
) {
    AsyncWriter::new(
        AsyncWriterConfig::new("quant_market_execution")
            .capacity(10_000)
            .batch_size(10_000)
            .flush_interval(Duration::from_hours(1)),
        move |rows: Vec<MarketExecutionRow>| {
            let write_manager = Arc::clone(&write_manager);
            let client = client.clone();
            Box::pin(async move {
                write_manager
                    .write_batch(&client, "quant_market_execution", rows)
                    .await
            })
        },
        IntCounter::new("test_async_writer_drops", "test drop counter").expect("counter"),
        AsyncWriterObservability::default(),
    )
}

pub async fn async_writer_shutdown_buffer() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    let shutdown = CancellationToken::new();
    let write_manager = Arc::new(ChWriteManager::new(4, &config.io));

    let (writer, worker) = execution_writer(client.clone(), write_manager);
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    let now = Utc::now().timestamp_millis();
    for i in 0..3 {
        assert!(writer.write(sample_execution(&format!("tok-drain-{i}"), now + i * 1000)));
    }

    shutdown.cancel();
    let _ = handle.await;

    let count: u64 = client
        .query("SELECT count() FROM quant_market_execution WHERE token_id LIKE 'tok-drain-%'")
        .fetch_one()
        .await
        .expect("count rows");
    assert_eq!(count, 3, "shutdown should flush all buffered rows");
}

pub async fn async_writer_channel_buffer() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    let write_manager = Arc::new(ChWriteManager::new(4, &config.io));

    let (writer, worker) = execution_writer(client.clone(), write_manager);
    // Shutdown never fires; dropping the writer must still drain the tail.
    let handle = tokio::spawn(worker.run(CancellationToken::new()));

    let now = Utc::now().timestamp_millis();
    assert!(writer.write(sample_execution("tok-close-1", now)));
    assert!(writer.write(sample_execution("tok-close-2", now + 1_000)));

    drop(writer);
    let _ = handle.await;

    let count: u64 = client
        .query("SELECT count() FROM quant_market_execution WHERE token_id LIKE 'tok-close-%'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 2, "dropping sender should flush buffered rows");
}
