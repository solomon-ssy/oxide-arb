//! `ClickHouse` storage and schema system contracts.

use std::{slice, sync::Arc, time::Duration};

use chrono::Utc;
use clickhouse::Client;
use prometheus::IntCounter;
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, ChBps, ChDecimal64, ChDigest, ChPrice, ChProbability, ChSchemaVersion,
        ChShares, ChUsd, CryptoPriceReportRow, DomainEventRow, QuantReportRecommendationFactRow,
        TradeTapeRow,
    },
    config::ClickHouseConfig,
    domain::data_plane::trade_tape_coverage::{
        FEE_RATE, MARKET_ID, PRICE, SIDE, SIZE, TOKEN_ID, TRADE_ID,
    },
    enums::clickhouse::{
        ChCanonicalBookEventType, ChLedgerTradeSide, ChOutcomeSide, ChTradeParticipantRole,
        ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, MarketId, Price, RecommendationId,
        RecommendationReportId, Shares, TokenId, Usd,
    },
};
use quant_pivot_storage::{
    clickhouse::{
        ChWriteManager, ClickHousePool, ClickHouseQueryLimits, active_preproduction_query_count,
        apply_offline_schema_migrations, apply_online_schema_migrations,
        reset_preproduction_database, verify_schema,
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

fn maintenance_config(config: &ClickHouseConfig) -> ClickHouseConfig {
    ClickHouseConfig {
        database: "default".to_owned(),
        ..config.clone()
    }
}

async fn setup_clickhouse() -> (ClickHousePool, Client, ClickHouseConfig, ()) {
    let config = fresh_clickhouse_config("storage");
    apply_offline_schema_migrations(&config)
        .await
        .expect("deploy schema");
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.verify_schema().await.expect("verify schema");
    let client = pool.client().clone();
    (pool, client, config, ())
}

pub async fn first_creates_missing_schema() {
    let config = fresh_clickhouse_config("first_deployment");

    let missing = verify_schema(&config)
        .await
        .expect_err("read-only startup must not create a missing database");
    assert!(missing.to_string().contains("does not exist"));

    assert_eq!(
        apply_online_schema_migrations(&config)
            .await
            .expect("first schema migration")
            .current_version,
        1
    );
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

    let deploy_error = apply_offline_schema_migrations(&config)
        .await
        .expect_err("second schema deployer must not run concurrently");
    assert!(deploy_error.to_string().contains("deployment lock"));

    let runtime_error = verify_schema(&config)
        .await
        .expect_err("runtime must not start while schema deployment is active");
    assert!(runtime_error.to_string().contains("deployment lock"));
}

pub async fn clean_boot_rejects_database() {
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

    let boot_error = apply_offline_schema_migrations(&config)
        .await
        .expect_err("nonempty unmanaged database must block clean boot");
    assert!(
        boot_error
            .to_string()
            .contains("contains unmanaged pre-baseline objects")
    );
    let legacy_count: u64 = client
        .query("SELECT count() FROM legacy_quant_recommendation_event")
        .fetch_one()
        .await
        .expect("legacy rows remain intact");
    let migration_ledger_count: u64 = client
        .query(
            "SELECT count() FROM system.tables \
             WHERE database = currentDatabase() AND name = 'quant_pivot_schema_migration'",
        )
        .fetch_one()
        .await
        .expect("migration-ledger absence probe");
    assert_eq!(legacy_count, 1);
    assert_eq!(migration_ledger_count, 0);
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
    let (pool, _client, _config, _stack) = setup_clickhouse().await;
    pool.health_check().await.expect("health check should pass");
}

pub async fn clickhouse_schema_idempotent() {
    let (pool, _client, config, _stack) = setup_clickhouse().await;
    apply_online_schema_migrations(&config)
        .await
        .expect("second schema deployment should be idempotent");
    pool.verify_schema().await.expect("schema remains valid");
}

pub async fn native_query_limits_rejects() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let row_error = ClickHouseQueryLimits::new("ch.system.row_overflow", 1, 1_024)
        .query(&client, "SELECT number FROM numbers(2)")
        .fetch_all::<u64>()
        .await
        .expect_err("row overflow must fail instead of truncating");
    assert!(
        row_error.to_string().contains("TOO_MANY_ROWS_OR_BYTES"),
        "unexpected row-overflow error: {row_error}"
    );

    let byte_error = ClickHouseQueryLimits::new("ch.system.byte_overflow", 1_000, 1)
        .query(&client, "SELECT repeat('x', 1024) FROM numbers(100)")
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

pub async fn canonical_evidence_no_ttl() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;

    for table in [
        "quant_book_l2_ledger",
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
        .query(
            "INSERT INTO quant_pivot_schema_migration \
             SELECT 1, 'cloud_baseline', 'blake3:tampered', now64(3) + INTERVAL 1 SECOND",
        )
        .execute()
        .await
        .expect("insert conflicting migration ledger row");

    let error = verify_schema(&config)
        .await
        .expect_err("runtime contract must reject immutable migration drift");
    assert!(error.to_string().contains("distinct definitions"));
}

pub async fn runtime_verification_rejects_drift() {
    let (_pool, client, config, _stack) = setup_clickhouse().await;
    client
        .query(
            "ALTER TABLE quant_trade_tape \
             ADD COLUMN IF NOT EXISTS unmanaged_probe Nullable(String)",
        )
        .execute()
        .await
        .expect("install semantic schema drift");

    let error = verify_schema(&config)
        .await
        .expect_err("runtime contract must reject unmanaged columns");
    assert!(error.to_string().contains("semantic schema drift"));
    assert!(error.to_string().contains("quant_trade_tape"));
}

pub async fn clickhouse_fact_uses_columns() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
    let expected: [(&str, &[&str]); 7] = [
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

pub async fn crypto_price_matches_schema() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
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

pub async fn trade_tape_direct_roundtrip() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
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

pub async fn last_trade_projects_once() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
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
        venue_event_time: now,
        ingress_time: now + 1,
        persisted_time: now + 2,
        event_hash: ChDigest::new([0; 32]),
        schema_version: BookL2LedgerRow::SCHEMA_VERSION,
    }
    .seal()
    .expect("seal LastTrade ledger row");
    let expected_source_event_id = ContentHash::from(row.event_hash).to_string();
    let write_manager = ChWriteManager::new(1);

    for _ in 0..2 {
        write_manager
            .write_borrowed_batch(&client, "quant_book_l2_ledger", slice::from_ref(&row))
            .await
            .expect("acknowledged async ledger insert");
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
            "SELECT count() FROM quant_trade_tape \
             WHERE token_id = 'tok-ledger-last-trade'",
        )
        .fetch_one()
        .await
        .expect("count materialized LastTrade rows");
    assert_eq!(ledger_count, 1, "retried ledger block must deduplicate");
    assert_eq!(
        trade_count, 1,
        "dependent materialized view must deduplicate"
    );

    let projected = client
        .query(
            "SELECT ?fields FROM quant_trade_tape \
             WHERE token_id = 'tok-ledger-last-trade' LIMIT 1",
        )
        .fetch_one::<TradeTapeRow>()
        .await
        .expect("read materialized trade tape row");
    let expected_coverage = TRADE_ID | MARKET_ID | TOKEN_ID | PRICE | SIDE | SIZE | FEE_RATE;
    assert_eq!(
        projected.market_id,
        MarketId::new("market-ledger-last-trade")
    );
    assert_eq!(projected.token_id, TokenId::new("tok-ledger-last-trade"));
    assert_eq!(projected.event_time, now);
    assert_eq!(projected.ingestion_time, now + 2);
    assert_eq!(projected.stream_session_id, Some(stream_session_id));
    assert_eq!(projected.token_sequence, Some(41));
    assert_eq!(projected.participant_address, "");
    assert_eq!(projected.participant_role, ChTradeParticipantRole::Unknown);
    assert_eq!(projected.side, ChTradeSide::Sell);
    assert_eq!(projected.price, ChPrice::from(Price::new(dec!(0.4115))));
    assert_eq!(projected.size_shares, ChShares::from(Shares::new(dec!(9))));
    assert_eq!(projected.notional_usd, ChUsd::from(Usd::new(dec!(3.7035))));
    assert_eq!(projected.tx_hash, None);
    assert_eq!(projected.source_event_id, expected_source_event_id);
    assert_eq!(projected.source, ChTradeTapeSource::MarketWs);
    assert_eq!(projected.observed_field_flags, expected_coverage);
    assert_eq!(projected.fee_rate_bps, Some(ChBps::from(dec!(2.5))));
    assert_eq!(
        projected.reconciliation_status,
        ChTradeReconciliationStatus::Pending
    );
    assert_eq!(projected.matched_source_event_id, None);
    assert_eq!(projected.revision, 1);
    assert_eq!(projected.reconciled_at, None);
    assert_eq!(projected.raw_payload_json, None);
    assert_eq!(projected.schema_version, ChSchemaVersion::FIRST);
}

/// Build a tick-event `AsyncWriter` whose flush sink is `ChWriteManager::write_batch`.
fn trade_writer(
    client: Client,
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

pub async fn async_writer_shutdown_buffer() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
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

pub async fn async_writer_channel_buffer() {
    let (_pool, client, _config, _stack) = setup_clickhouse().await;
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
