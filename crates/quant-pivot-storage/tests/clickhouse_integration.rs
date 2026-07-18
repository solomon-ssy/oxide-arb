//! `ClickHouse` integration tests (requires Docker).

use chrono::Utc;
use prometheus::IntCounter;
use quant_pivot_models::{
    clickhouse::{
        ChDecimal64, ChPrice, ChProbability, ChSchemaVersion, ChShares, ChUsd,
        CryptoPriceReportRow, DomainEventRow, QuantRecommendationAttributionEventRow,
        QuantReportRecommendationFactRow, TradeTapeRow,
    },
    config::{ClickHouseConfig, SchemaMigrationConfig},
    enums::clickhouse::{
        ChOutcomeSide, ChRecommendationAttributionOutcome, ChTradeParticipantRole,
        ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    },
    hashing::CanonicalDigest,
    types::{
        DomainInstrumentKey, DomainSourceId, MarketId, Price, RecommendationId,
        RecommendationReportId, Shares, TokenId, Usd,
    },
};
use quant_pivot_storage::{
    clickhouse::{
        ChWriteManager, ClickHousePool, apply_offline_schema_migrations,
        apply_online_schema_migrations, verify_schema,
    },
    write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability, AsyncWriterWorker},
};
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration};
use testcontainers::{
    ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

fn test_ch_config(port: u16) -> ClickHouseConfig {
    ClickHouseConfig {
        deployment_id: "clickhouse-integration".into(),
        cluster_id: "testcontainer".into(),
        url: format!("http://localhost:{port}"),
        database: "default".into(),
        user: "default".into(),
        password: "".into(),
        migration: SchemaMigrationConfig::default(),
        batch_size: 100,
        flush_interval_secs: 5,
        max_concurrent_inserts: 4,
    }
}

async fn setup_clickhouse() -> (
    ClickHousePool,
    clickhouse::Client,
    u16,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
) {
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "26.5")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = test_ch_config(port);
    apply_offline_schema_migrations(&config)
        .await
        .expect("deploy schema");
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.verify_schema().await.expect("verify schema");
    let client = pool.client().clone();
    (pool, client, port, container)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn first_deployment_creates_missing_database_and_schema() {
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "26.5")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = ClickHouseConfig {
        database: "quant_pivot_bootstrap_it".into(),
        ..test_ch_config(port)
    };

    let missing = verify_schema(&config)
        .await
        .expect_err("read-only startup must not create a missing database");
    assert!(missing.to_string().contains("does not exist"));

    assert_eq!(
        apply_offline_schema_migrations(&config)
            .await
            .expect("first schema migration")
            .current_version,
        5
    );
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.verify_schema().await.expect("verify schema");

    let count: u64 = pool
        .client()
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind("quant_pivot_bootstrap_it")
        .fetch_one()
        .await
        .expect("database should exist");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn deployment_and_runtime_fail_closed_while_schema_lock_is_held() {
    let (_pool, client, port, _container) = setup_clickhouse().await;
    client
        .query(
            "CREATE TABLE quant_pivot_schema_deployment_lock (owner String) \
             ENGINE = TinyLog COMMENT 'test-deploy-owner'",
        )
        .execute()
        .await
        .expect("install deployment lock");

    let deploy_error = apply_offline_schema_migrations(&test_ch_config(port))
        .await
        .expect_err("second schema deployer must not run concurrently");
    assert!(deploy_error.to_string().contains("deployment lock"));

    let runtime_error = verify_schema(&test_ch_config(port))
        .await
        .expect_err("runtime must not start while schema deployment is active");
    assert!(runtime_error.to_string().contains("deployment lock"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn offline_report_lifecycle_migration_rejects_non_empty_legacy_facts() {
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "26.5")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = test_ch_config(port);

    let online_error = apply_online_schema_migrations(&config)
        .await
        .expect_err("destructive report lifecycle migration requires offline rollout");
    assert!(online_error.to_string().contains("explicit offline"));

    let client = ClickHousePool::from_config(&config).client().clone();
    client
        .query(
            "INSERT INTO quant_recommendation_event VALUES \
             (now64(3), 'legacy-report', 'legacy-recommendation', 1, 'market', 'token', \
              'yes', 0.7, 0.6, true, 10, now64(3) + INTERVAL 1 HOUR, 'published')",
        )
        .execute()
        .await
        .expect("insert legacy recommendation fact");

    let offline_error = apply_offline_schema_migrations(&config)
        .await
        .expect_err("non-empty legacy table must block destructive migration");
    assert!(
        offline_error
            .to_string()
            .contains("requires empty table `quant_recommendation_event`")
    );
    let legacy_count: u64 = client
        .query("SELECT count() FROM quant_recommendation_event")
        .fetch_one()
        .await
        .expect("legacy rows remain intact");
    let new_table_count: u64 = client
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = 'quant_report_recommendation_fact'",
        )
        .fetch_one()
        .await
        .expect("new table absence probe");
    assert_eq!(legacy_count, 1);
    assert_eq!(new_table_count, 0);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_health_check() {
    let (pool, _client, _port, _container) = setup_clickhouse().await;
    pool.health_check().await.expect("health check should pass");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_schema_idempotent() {
    let (pool, _client, port, _container) = setup_clickhouse().await;
    apply_online_schema_migrations(&test_ch_config(port))
        .await
        .expect("second schema deployment should be idempotent");
    pool.verify_schema().await.expect("schema remains valid");
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TableDdl {
    statement: String,
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn canonical_evidence_tables_have_no_delete_ttl() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;

    for table in [
        "quant_book_l2_event",
        "quant_book_l2_checkpoint",
        "quant_book_stream_session",
        "quant_trade_tape",
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_schema_verification_rejects_unmanaged_raw_ttl() {
    let (_pool, client, port, _container) = setup_clickhouse().await;
    client
        .query(
            "ALTER TABLE quant_book_l2_event \
             MODIFY TTL venue_event_time + INTERVAL 200 DAY DELETE \
             SETTINGS materialize_ttl_after_modify = 0",
        )
        .execute()
        .await
        .expect("install unmanaged TTL");

    let error = verify_schema(&test_ch_config(port))
        .await
        .expect_err("runtime contract must reject unmanaged TTL");
    assert!(error.to_string().contains("unmanaged table TTL"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_schema_verification_rejects_migration_ledger_drift() {
    let (_pool, client, port, _container) = setup_clickhouse().await;
    client
        .query(
            "INSERT INTO quant_pivot_schema_migration \
             SELECT 1, 'cloud_baseline', 'blake3:tampered', now64(3) + INTERVAL 1 SECOND",
        )
        .execute()
        .await
        .expect("insert conflicting migration ledger row");

    let error = verify_schema(&test_ch_config(port))
        .await
        .expect_err("runtime contract must reject immutable migration drift");
    assert!(error.to_string().contains("distinct definitions"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn runtime_schema_verification_rejects_semantic_column_drift() {
    let (_pool, client, port, _container) = setup_clickhouse().await;
    client
        .query(
            "ALTER TABLE quant_trade_tape \
             ADD COLUMN IF NOT EXISTS unmanaged_probe Nullable(String)",
        )
        .execute()
        .await
        .expect("install semantic schema drift");

    let error = verify_schema(&test_ch_config(port))
        .await
        .expect_err("runtime contract must reject unmanaged columns");
    assert!(error.to_string().contains("semantic schema drift"));
    assert!(error.to_string().contains("quant_trade_tape"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_fact_contract_uses_decimal_and_enum_columns() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let expected: [(&str, &[&str]); 8] = [
        (
            "quant_book_l2_event",
            &[
                "`bid_prices` Array(Decimal(18, 8))",
                "`bid_sizes` Array(Decimal(38, 18))",
                "`token_sequence` UInt64",
            ],
        ),
        (
            "quant_book_l2_checkpoint",
            &[
                "`bids_json` String CODEC(ZSTD(3))",
                "`asks_json` String CODEC(ZSTD(3))",
                "`source_event_hash` String",
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
            "quant_trade_tape",
            &["ENGINE = MergeTree", "ingestion_time"],
        ),
        (
            "quant_domain_observation",
            &["ENGINE = MergeTree", "ingestion_time"],
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
        if matches!(table, "quant_trade_tape" | "quant_domain_observation") {
            assert!(
                !ddl.statement.contains("ReplacingMergeTree"),
                "PIT source table {table} must retain every ingested revision"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn crypto_price_report_rust_row_matches_clickhouse_schema() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let now = Utc::now().timestamp_millis();
    let row = CryptoPriceReportRow {
        source_id: DomainSourceId::binance(),
        instrument_key: DomainInstrumentKey::new("BINANCE_AGG_TRADE:BTCUSDT"),
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
             WHERE source_id = 'binance' AND source_sequence = 1",
        )
        .fetch_one()
        .await
        .expect("read inserted crypto report");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn domain_event_rust_row_matches_clickhouse_schema() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let now = Utc::now().timestamp_millis();
    let row = DomainEventRow {
        event_id: uuid::Uuid::now_v7(),
        source: "binance".to_owned(),
        event_type: "crypto.price_transition".to_owned(),
        subject: "BINANCE_AGG_TRADE:BTCUSDT".to_owned(),
        event_time: now,
        published_at: now,
        available_at: now,
        schema_version: ChSchemaVersion::FIRST,
        revision: 1,
        supersedes_event_id: Some(uuid::Uuid::now_v7()),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn report_fact_schema_accepts_decision_snapshot_and_superseded_censor() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
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
        recommendation_report_id: report_id.clone(),
        recommendation_id: recommendation_id.clone(),
        rank: 1,
        market_id: MarketId::new("report-fact-schema-market"),
        token_id: TokenId::new("report-fact-schema-token"),
        side: ChOutcomeSide::Yes,
        score: ChProbability::from(dec!(0.72)),
        risk_adjusted_score: ChProbability::from(dec!(0.68)),
        trade_plan_available: true,
        suggested_usd: Some(ChUsd::from(Usd::new(dec!(25)))),
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

    let attribution = QuantRecommendationAttributionEventRow {
        event_time: now,
        recommendation_id: recommendation_id.clone(),
        outcome: ChRecommendationAttributionOutcome::SupersededUnfilled,
        realized_pnl_usd: ChUsd::from(Usd::ZERO),
        max_adverse_excursion_bps: None,
        max_favorable_excursion_bps: ChDecimal64::from(dec!(0)),
        label_available_at: now,
        ingestion_time: now,
    };
    let mut attribution_insert = client
        .insert::<QuantRecommendationAttributionEventRow>("quant_recommendation_attribution_event")
        .await
        .expect("start superseded censor insert");
    attribution_insert
        .write(&attribution)
        .await
        .expect("write superseded censor");
    attribution_insert
        .end()
        .await
        .expect("commit superseded censor");

    let decision_count: u64 = client
        .query(
            "SELECT count() FROM quant_report_recommendation_fact \
             WHERE recommendation_report_id = ? AND recommendation_id = ?",
        )
        .bind(report_id)
        .bind(recommendation_id.clone())
        .fetch_one()
        .await
        .expect("read recommendation decision");
    let outcome_code: i8 = client
        .query(
            "SELECT toInt8(outcome) FROM quant_recommendation_attribution_event \
             WHERE recommendation_id = ?",
        )
        .bind(recommendation_id)
        .fetch_one()
        .await
        .expect("read superseded censor outcome code");
    assert_eq!(decision_count, 1);
    assert_eq!(outcome_code, 6);

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

fn sample_trade(token_id: &str, received_at: i64) -> TradeTapeRow {
    TradeTapeRow {
        market_id: MarketId::new("market-integration"),
        token_id: TokenId::new(token_id),
        event_time: received_at,
        ingestion_time: received_at,
        stream_session_id: None,
        token_sequence: Some(1),
        participant_address: String::new(),
        participant_role: ChTradeParticipantRole::Unknown,
        side: ChTradeSide::Buy,
        price: ChPrice::from(Price::new(dec!(0.95))),
        size_shares: ChShares::from(Shares::new(dec!(10))),
        notional_usd: ChUsd::from(Usd::new(dec!(9.5))),
        tx_hash: None,
        source_event_id: format!("ws:{token_id}:{received_at}"),
        source: ChTradeTapeSource::MarketWs,
        observed_field_flags: u16::MAX,
        fee_rate_bps: None,
        reconciliation_status: ChTradeReconciliationStatus::Pending,
        matched_source_event_id: None,
        revision: 1,
        reconciled_at: None,
        raw_payload_json: Some(r#"{"test":true}"#.into()),
        schema_version: ChSchemaVersion(2),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn trade_tape_direct_insert_roundtrip() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let row = sample_trade("tok-direct", Utc::now().timestamp_millis());
    let mut insert = client
        .insert::<TradeTapeRow>("quant_trade_tape")
        .await
        .expect("insert start");
    insert.write(&row).await.expect("write row");
    insert.end().await.expect("end insert");

    let count: u64 = client
        .query("SELECT count() FROM quant_trade_tape WHERE token_id = 'tok-direct'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 1);
}

/// Build a tick-event `AsyncWriter` whose flush sink is `ChWriteManager::write_batch`.
fn trade_writer(
    client: clickhouse::Client,
    write_manager: Arc<ChWriteManager>,
) -> (AsyncWriter<TradeTapeRow>, AsyncWriterWorker<TradeTapeRow>) {
    AsyncWriter::new(
        AsyncWriterConfig::new("quant_trade_tape")
            .capacity(10_000)
            .batch_size(10_000)
            .flush_interval(Duration::from_hours(1)),
        move |rows: Vec<TradeTapeRow>| {
            let write_manager = Arc::clone(&write_manager);
            let client = client.clone();
            Box::pin(async move {
                write_manager
                    .write_batch(&client, "quant_trade_tape", rows)
                    .await
            })
        },
        IntCounter::new("test_async_writer_drops", "test drop counter").expect("counter"),
        AsyncWriterObservability::default(),
    )
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn async_writer_shutdown_drains_buffer() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let shutdown = CancellationToken::new();
    let write_manager = Arc::new(ChWriteManager::new(4));

    let (writer, worker) = trade_writer(client.clone(), write_manager);
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    let now = Utc::now().timestamp_millis();
    for i in 0..3 {
        assert!(writer.write(sample_trade(&format!("tok-drain-{i}"), now + i * 1000)));
    }

    shutdown.cancel();
    let _ = handle.await;

    let count: u64 = client
        .query("SELECT count() FROM quant_trade_tape WHERE token_id LIKE 'tok-drain-%'")
        .fetch_one()
        .await
        .expect("count rows");
    assert_eq!(count, 3, "shutdown should flush all buffered rows");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn async_writer_channel_close_drains_buffer() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let write_manager = Arc::new(ChWriteManager::new(4));

    let (writer, worker) = trade_writer(client.clone(), write_manager);
    // Shutdown never fires; dropping the writer must still drain the tail.
    let handle = tokio::spawn(worker.run(CancellationToken::new()));

    let now = Utc::now().timestamp_millis();
    assert!(writer.write(sample_trade("tok-close-1", now)));
    assert!(writer.write(sample_trade("tok-close-2", now + 1_000)));

    drop(writer);
    let _ = handle.await;

    let count: u64 = client
        .query("SELECT count() FROM quant_trade_tape WHERE token_id LIKE 'tok-close-%'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 2, "dropping sender should flush buffered rows");
}
