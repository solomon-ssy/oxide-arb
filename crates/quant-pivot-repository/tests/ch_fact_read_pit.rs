//! `ClickHouse` point-in-time read integration tests (book tie-breaker + resolution).

use chrono::Utc;
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, ChDecimal64, ChPrice, ChSchemaVersion,
        WeatherForecastFactRow, WeatherObservationFactRow,
    },
    config::{ClickHouseConfig, SchemaMigrationConfig},
    enums::clickhouse::ChFactSource,
    types::{
        ContentHash, DomainInstrumentKey, DomainSourceId, MarketId, Price, Shares, TokenId, Usd,
    },
};
use quant_pivot_repository::{
    clickhouse::ChQuantFactReadRepository, traits::QuantFactReadRepository,
};
use quant_pivot_storage::clickhouse::{ClickHousePool, apply_offline_schema_migrations};
use rust_decimal::Decimal;
use std::{sync::Arc, time::Duration};
use testcontainers::{
    ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use uuid::Uuid;

fn test_ch_config(port: u16) -> ClickHouseConfig {
    ClickHouseConfig {
        deployment_id: "ch-fact-read-pit".into(),
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
    Arc<ClickHousePool>,
    clickhouse::Client,
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
        .expect("schema deploy");
    let pool = Arc::new(ClickHousePool::connect(&config).await.expect("connect"));
    pool.verify_schema().await.expect("schema verify");
    let client = pool.client().clone();
    (pool, client, container)
}

async fn insert_book_rows(client: &clickhouse::Client, rows: &[BookL2CheckpointRow]) {
    let mut insert = client
        .insert::<BookL2CheckpointRow>("quant_book_l2_checkpoint")
        .await
        .expect("insert");
    for row in rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("end insert");
}

async fn insert_microstructure_rows(client: &clickhouse::Client, rows: &[BookMicrostructureRow]) {
    let mut insert = client
        .insert::<BookMicrostructureRow>("book_microstructure_1s")
        .await
        .expect("insert microstructure");
    for row in rows {
        insert.write(row).await.expect("write microstructure row");
    }
    insert.end().await.expect("end microstructure insert");
}

/// Wait until inserted checkpoint rows are query-visible.
///
/// Fresh `MergeTree` parts can lag briefly behind HTTP insert ack on a cold
/// testcontainer; PIT reads that race this window return `None`.
async fn wait_for_book_snapshot_rows(client: &clickhouse::Client, token: &TokenId, expected: u64) {
    const ATTEMPTS: usize = 40;
    const PAUSE: Duration = Duration::from_millis(50);
    for attempt in 1..=ATTEMPTS {
        let count: u64 = client
            .query("SELECT count() FROM quant_book_l2_checkpoint WHERE token_id = ?")
            .bind(token.clone())
            .fetch_one()
            .await
            .expect("count quant_book_l2_checkpoint");
        if count >= expected {
            return;
        }
        assert!(
            attempt < ATTEMPTS,
            "checkpoint rows for {} not visible after insert \
             (count={count}, expected>={expected}, attempts={ATTEMPTS})",
            token.as_str()
        );
        tokio::time::sleep(PAUSE).await;
    }
}

/// Current epoch millis for deterministic point-in-time visibility checks.
fn fresh_event_time_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn content_hash(digit: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", digit.to_string().repeat(64)))
        .expect("valid content hash")
}

fn book_row(
    token: &str,
    event_time_ms: i64,
    ingestion_time_ms: i64,
    sequence: u64,
    mid: Decimal,
) -> BookL2CheckpointRow {
    let source_event_hash =
        ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("source event hash");
    let checkpoint_hash =
        ContentHash::parse(format!("blake3:{}", "2".repeat(64))).expect("checkpoint hash");
    BookL2CheckpointRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new("0xchpit")),
        stream_session_id: Uuid::nil(),
        token_sequence: sequence,
        bids_json: format!(r#"[["{mid}","100"]]"#),
        asks_json: r#"[["0.52","100"]]"#.to_owned(),
        book_version: 1,
        source_event_hash,
        checkpoint_hash,
        event_time: event_time_ms,
        created_at: ingestion_time_ms,
        schema_version: ChSchemaVersion(2),
    }
}

fn microstructure_row(
    token_id: &TokenId,
    market_id: &MarketId,
    bucket_time: i64,
    available_at: i64,
    mid: Decimal,
) -> BookMicrostructureRow {
    let price = ChPrice::from(Price::new(mid));
    BookMicrostructureRow {
        token_id: token_id.clone(),
        market_id: Some(market_id.clone()),
        bucket_time,
        best_bid_open: None,
        best_bid_high: None,
        best_bid_low: None,
        best_bid_close: None,
        best_ask_open: None,
        best_ask_high: None,
        best_ask_low: None,
        best_ask_close: None,
        spread_bps_min: None,
        spread_bps_avg: None,
        spread_bps_max: None,
        mid_price_open: Some(price),
        mid_price_close: Some(price),
        top1_depth_usd_avg: None,
        top5_depth_usd_avg: None,
        top20_depth_usd_avg: None,
        imbalance_avg: None,
        update_count: 1,
        snapshot_count: 1,
        delta_count: 0,
        delete_count: 0,
        crossed_count: 0,
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: 0,
        schema_version: ChSchemaVersion::FIRST,
        available_at,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ch_read_orders_by_event_time_with_tiebreaker() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let token = TokenId::new("ch-pit-yes");
    let event_time = fresh_event_time_ms();

    insert_book_rows(
        &client,
        &[
            book_row(
                token.as_str(),
                event_time,
                event_time + 1,
                1,
                Decimal::new(49, 2),
            ),
            book_row(
                token.as_str(),
                event_time,
                event_time + 2,
                1,
                Decimal::new(50, 2),
            ),
        ],
    )
    .await;
    wait_for_book_snapshot_rows(&client, &token, 2).await;

    let before_late_arrival = read
        .book_checkpoint_at(&token, event_time + 5, event_time + 1)
        .await
        .expect("read before late arrival")
        .expect("earlier visible revision");
    assert_eq!(
        before_late_arrival.bids_json, r#"[["0.49","100"]]"#,
        "a backdated revision must not be visible before its ingestion time"
    );

    let row = read
        .book_checkpoint_at(&token, event_time + 5, event_time + 5)
        .await
        .expect("read")
        .unwrap_or_else(|| {
            panic!(
                "PIT book_checkpoint_at returned None for token={} as_of={}",
                token.as_str(),
                event_time + 5
            )
        });
    assert_eq!(
        row.bids_json, r#"[["0.50","100"]]"#,
        "tie-breaker must prefer later ingestion_time at same event_time"
    );
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn historical_scans_reject_rows_not_yet_available() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let event_time = fresh_event_time_ms();

    let market_id = MarketId::new("0xavailability-axis");
    let token_id = TokenId::new("availability-axis-yes");
    let late_ingestion = event_time + 10_000;
    let mut late_book = book_row(
        token_id.as_str(),
        event_time,
        late_ingestion,
        1,
        Decimal::new(50, 2),
    );
    late_book.market_id = Some(market_id.clone());
    insert_book_rows(&client, std::slice::from_ref(&late_book)).await;
    wait_for_book_snapshot_rows(&client, &token_id, 1).await;

    assert!(
        read.observed_markets_between(event_time - 1, event_time + 1, event_time)
            .await
            .expect("markets before ingestion")
            .is_empty(),
        "a historical candidate must not exist before its book ingestion time"
    );
    assert_eq!(
        read.observed_markets_between(event_time - 1, event_time + 1, late_ingestion)
            .await
            .expect("markets after ingestion"),
        vec![market_id.clone()]
    );

    let visible_at = event_time + 1_000;
    let corrected_at = event_time + 2_000;
    insert_microstructure_rows(
        &client,
        &[
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                visible_at,
                Decimal::new(40, 2),
            ),
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                corrected_at,
                Decimal::new(60, 2),
            ),
        ],
    )
    .await;

    let before_correction = read
        .mid_price_series(
            vec![token_id.clone()],
            event_time - 1,
            event_time + 1,
            visible_at,
            60,
        )
        .await
        .expect("mid series before correction");
    assert_eq!(before_correction.len(), 1);
    assert_eq!(
        before_correction[0].mid_price.map(ChPrice::to_price),
        Some(Price::new(Decimal::new(40, 2)))
    );

    let after_correction = read
        .mid_price_series(
            vec![token_id],
            event_time - 1,
            event_time + 1,
            corrected_at,
            60,
        )
        .await
        .expect("mid series after correction");
    assert_eq!(after_correction.len(), 1);
    assert_eq!(
        after_correction[0].mid_price.map(ChPrice::to_price),
        Some(Price::new(Decimal::new(60, 2)))
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn resolution_at_is_pit_bounded() {
    use quant_pivot_models::clickhouse::MarketResolutionRow;

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let market_id = MarketId::new("0xchpit-res");
    let yes = TokenId::new("ch-pit-yes");
    let no = TokenId::new("ch-pit-no");

    let early = fresh_event_time_ms();
    let late = early + 10_000;
    let as_of = early + 5_000;

    let rows = vec![
        MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: yes.clone(),
            winning_outcome: "Yes".to_owned(),
            asset_token_ids: vec![yes.clone(), no.clone()],
            resolved_at: early,
            observed_at: early,
            source: ChFactSource::WsMarketResolved,
            sequence: 1,
            schema_version: ChSchemaVersion::FIRST,
        },
        MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: no.clone(),
            winning_outcome: "No".to_owned(),
            asset_token_ids: vec![yes.clone(), no.clone()],
            // A correction whose economic time is in range but whose writer
            // observation is not yet visible at `as_of`.
            resolved_at: early + 1_000,
            observed_at: as_of + 1_000,
            source: ChFactSource::WsMarketResolved,
            sequence: 3,
            schema_version: ChSchemaVersion::FIRST,
        },
        MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: no.clone(),
            winning_outcome: "No".to_owned(),
            asset_token_ids: vec![yes.clone(), no.clone()],
            resolved_at: late,
            observed_at: late,
            source: ChFactSource::WsMarketResolved,
            sequence: 2,
            schema_version: ChSchemaVersion::FIRST,
        },
    ];
    let mut insert = client
        .insert::<MarketResolutionRow>("market_resolution_event")
        .await
        .expect("insert");
    for row in &rows {
        insert.write(row).await.expect("write");
    }
    insert.end().await.expect("end");

    let resolved = read
        .resolution_at(&market_id, as_of, as_of)
        .await
        .expect("read")
        .expect("resolution");
    assert_eq!(resolved.resolved_at, early);
    assert_eq!(resolved.winning_token_id, yes);

    let corrected = read
        .resolution_at(&market_id, as_of, as_of + 1_000)
        .await
        .expect("read corrected")
        .expect("corrected resolution");
    assert_eq!(corrected.resolved_at, early + 1_000);
    assert_eq!(corrected.winning_token_id, no);

    let before_correction = read
        .resolutions_between(vec![market_id.clone()], early, as_of, as_of)
        .await
        .expect("bounded resolution range");
    assert_eq!(before_correction.len(), 1);
    let after_correction = read
        .resolutions_between(vec![market_id], early, as_of, as_of + 1_000)
        .await
        .expect("visible corrected range");
    assert_eq!(after_correction.len(), 2);
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn weather_long_form_facts_are_pit_visible_and_revision_preserving() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let observed_at = fresh_event_time_ms();
    let first_visible_at = observed_at + 1_000;
    let correction_visible_at = observed_at + 2_000;
    let station = "KJFK".to_owned();
    let observation_instrument = DomainInstrumentKey::new("AVIATION_WEATHER:KJFK");
    let original_hash = content_hash('3');

    let original = WeatherObservationFactRow {
        source_id: DomainSourceId::aviation_weather(),
        instrument_key: observation_instrument.clone(),
        subject_key: station.clone(),
        local_date: Utc::now().date_naive().into(),
        report_kind: "metar".to_owned(),
        variable: "temperature".to_owned(),
        value: ChDecimal64::from(Decimal::new(21_25, 2)),
        unit: "celsius".to_owned(),
        precision: ChDecimal64::from(Decimal::new(25, 2)),
        observed_at,
        valid_from: Some(observed_at),
        valid_to: None,
        published_at: observed_at,
        available_at: first_visible_at,
        revision: 1,
        report_hash: original_hash.clone(),
        supersedes_report_hash: None,
        raw_report: "METAR KJFK TEST".to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };
    let correction = WeatherObservationFactRow {
        report_kind: "correction".to_owned(),
        value: ChDecimal64::from(Decimal::new(22_00, 2)),
        published_at: observed_at + 1_500,
        available_at: correction_visible_at,
        revision: 2,
        report_hash: content_hash('4'),
        supersedes_report_hash: Some(original_hash),
        raw_report: "METAR KJFK COR TEST".to_owned(),
        ..original.clone()
    };
    let precipitation = WeatherObservationFactRow {
        source_id: DomainSourceId::new("nws_precipitation"),
        instrument_key: DomainInstrumentKey::new("NWS_PRECIPITATION:KJFK"),
        report_kind: "historical_ghcnh".to_owned(),
        variable: "precipitation".to_owned(),
        value: ChDecimal64::from(Decimal::new(12_70, 2)),
        unit: "millimeter".to_owned(),
        precision: ChDecimal64::from(Decimal::new(10, 2)),
        available_at: observed_at + 500,
        report_hash: content_hash('5'),
        raw_report: "PRECIP KJFK TEST".to_owned(),
        ..original.clone()
    };
    let mut observation_insert = client
        .insert::<WeatherObservationFactRow>("quant_weather_observation_fact")
        .await
        .expect("insert weather observations");
    for row in [&original, &original, &correction, &precipitation] {
        observation_insert
            .write(row)
            .await
            .expect("write weather observation");
    }
    observation_insert
        .end()
        .await
        .expect("end weather observation insert");

    assert_preposted_nhc_advisory_pit(&client, &read, observed_at, first_visible_at).await;

    let before_correction = read
        .weather_observation_facts_between(
            vec![station.clone()],
            observed_at - 1,
            observed_at + 1,
            first_visible_at,
            first_visible_at,
        )
        .await
        .expect("read observations before correction");
    assert_eq!(before_correction.len(), 2);
    assert_eq!(
        before_correction
            .iter()
            .filter(|row| row.variable == "temperature")
            .count(),
        1,
        "an exact writer retry must not duplicate one immutable report"
    );
    assert!(
        before_correction
            .iter()
            .any(|row| row.variable == "precipitation" && row.unit == "millimeter"),
        "the long-form table must retain a non-temperature variable"
    );

    let after_correction = read
        .weather_observation_facts_between(
            vec![station.clone()],
            observed_at - 1,
            observed_at + 1,
            correction_visible_at,
            correction_visible_at,
        )
        .await
        .expect("read observations after correction");
    assert_eq!(after_correction.len(), 3);
    assert!(
        after_correction.iter().any(|row| {
            row.variable == "temperature"
                && row.revision == 2
                && row.supersedes_report_hash.as_ref() == Some(&original.report_hash)
        }),
        "a visible correction must retain explicit supersession provenance"
    );

    assert_weather_forecast_pit(
        &client,
        &read,
        &station,
        observed_at,
        first_visible_at,
        correction_visible_at,
    )
    .await;
}

async fn assert_preposted_nhc_advisory_pit(
    client: &clickhouse::Client,
    read: &ChQuantFactReadRepository,
    base_time: i64,
    first_visible_at: i64,
) {
    let advisory_issuance = base_time + 5_000;
    let advisory = WeatherObservationFactRow {
        source_id: DomainSourceId::nhc_advisory(),
        instrument_key: DomainInstrumentKey::new("NHC_ADVISORY:eastern_pacific:EP052026"),
        subject_key: "EP052026".to_owned(),
        local_date: Utc::now().date_naive().into(),
        report_kind: "nhc_advisory".to_owned(),
        variable: "cyclone_intensity".to_owned(),
        value: ChDecimal64::from(Decimal::new(55, 0)),
        unit: "knot".to_owned(),
        precision: ChDecimal64::from(Decimal::ONE),
        observed_at: advisory_issuance,
        valid_from: Some(advisory_issuance),
        valid_to: None,
        published_at: base_time + 500,
        available_at: first_visible_at,
        revision: 0,
        report_hash: content_hash('6'),
        supersedes_report_hash: None,
        raw_report: "NHC EP052026 ADVISORY 013".to_owned(),
        schema_version: ChSchemaVersion::FIRST,
    };
    let mut insert = client
        .insert::<WeatherObservationFactRow>("quant_weather_observation_fact")
        .await
        .expect("insert preposted NHC advisory");
    insert.write(&advisory).await.expect("write NHC advisory");
    insert.end().await.expect("end NHC advisory insert");

    let before_issuance = read
        .weather_observation_facts_between(
            vec![advisory.subject_key.clone()],
            base_time,
            advisory_issuance,
            advisory_issuance - 1,
            advisory_issuance - 1,
        )
        .await
        .expect("read before nominal advisory issuance");
    assert!(
        before_issuance.is_empty(),
        "a preposted NHC file must not enter the observation window before nominal issuance"
    );
    let at_issuance = read
        .weather_observation_facts_between(
            vec![advisory.subject_key.clone()],
            base_time,
            advisory_issuance + 1,
            advisory_issuance,
            advisory_issuance,
        )
        .await
        .expect("read at nominal advisory issuance");
    assert_eq!(at_issuance.len(), 1);
    assert_eq!(at_issuance[0].report_hash, advisory.report_hash);
}

async fn assert_weather_forecast_pit(
    client: &clickhouse::Client,
    read: &ChQuantFactReadRepository,
    station: &str,
    reference_time: i64,
    first_visible_at: i64,
    correction_visible_at: i64,
) {
    let valid_time = reference_time + 86_400_000;
    let deterministic = WeatherForecastFactRow {
        source_id: DomainSourceId::gefs(),
        instrument_key: DomainInstrumentKey::new("GEFS:KJFK"),
        subject_key: station.to_owned(),
        variable: "temperature_maximum".to_owned(),
        value: ChDecimal64::from(Decimal::new(23_50, 2)),
        unit: "celsius".to_owned(),
        precision: ChDecimal64::from(Decimal::new(10, 2)),
        reference_time,
        valid_time,
        published_at: reference_time + 500,
        available_at: first_visible_at,
        lead_hours: 24,
        member: None,
        revision: 1,
        grid_binding_hash: content_hash('6'),
        run_manifest_hash: content_hash('7'),
        report_hash: content_hash('8'),
        schema_version: ChSchemaVersion::FIRST,
    };
    let ensemble_member = WeatherForecastFactRow {
        value: ChDecimal64::from(Decimal::new(24_25, 2)),
        available_at: correction_visible_at,
        member: Some(0),
        report_hash: content_hash('9'),
        ..deterministic.clone()
    };
    let mut forecast_insert = client
        .insert::<WeatherForecastFactRow>("quant_weather_forecast_fact")
        .await
        .expect("insert weather forecasts");
    for row in [&deterministic, &ensemble_member] {
        forecast_insert
            .write(row)
            .await
            .expect("write weather forecast");
    }
    forecast_insert
        .end()
        .await
        .expect("end weather forecast insert");

    let forecast_before_member = read
        .weather_forecast_facts_between(
            vec![station.to_owned()],
            valid_time - 1,
            valid_time + 1,
            reference_time,
            first_visible_at,
        )
        .await
        .expect("read forecast before ensemble member");
    assert_eq!(forecast_before_member.len(), 1);
    assert_eq!(forecast_before_member[0].member, None);

    let forecast_after_member = read
        .weather_forecast_facts_between(
            vec![station.to_owned()],
            valid_time - 1,
            valid_time + 1,
            reference_time,
            correction_visible_at,
        )
        .await
        .expect("read forecast after ensemble member");
    assert_eq!(forecast_after_member.len(), 2);
    assert!(
        forecast_after_member
            .iter()
            .any(|row| row.member == Some(0))
    );
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn trade_tape_preserves_prior_revisions_after_merge() {
    use quant_pivot_models::clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd, TradeTapeRow};
    use quant_pivot_models::enums::clickhouse::{
        ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    };

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(Arc::clone(&pool));
    let market_id = MarketId::new("0xtrade-tape-dedupe");
    let token_id = TokenId::new("tok-dedupe");
    // Keep the test partition close to wall clock for compact integration scans.
    let event_time = fresh_event_time_ms();

    let base = TradeTapeRow {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time,
        ingestion_time: event_time,
        stream_session_id: None,
        token_sequence: None,
        participant_address: "0xparticipant".to_owned(),
        participant_role: ChTradeParticipantRole::Maker,
        side: ChTradeSide::Buy,
        price: ChPrice::from(Price::new(Decimal::new(55, 2))),
        size_shares: ChShares::from(Shares::new(Decimal::from(10))),
        notional_usd: ChUsd::from(Usd::new(Decimal::from(5))),
        tx_hash: Some("0xtx".to_owned()),
        source_event_id: "trade-dedupe-1".to_owned(),
        source: ChTradeTapeSource::OnChainOrderFilled,
        observed_field_flags: u16::MAX,
        fee_rate_bps: None,
        reconciliation_status: ChTradeReconciliationStatus::OnChainOnly,
        matched_source_event_id: None,
        revision: 1,
        reconciled_at: None,
        raw_payload_json: None,
        schema_version: ChSchemaVersion(2),
    };
    let mut stale = base.clone();
    stale.ingestion_time = event_time - 1_000;
    let mut fresh = base.clone();
    fresh.ingestion_time = event_time + 1_000;
    fresh.revision = 2;
    fresh.reconciliation_status = ChTradeReconciliationStatus::Matched;
    fresh.matched_source_event_id = Some("ws:trade-dedupe-1".to_owned());
    fresh.reconciled_at = Some(fresh.ingestion_time);

    let mut insert = client
        .insert::<TradeTapeRow>("quant_trade_tape")
        .await
        .expect("insert");
    insert.write(&stale).await.expect("write stale");
    insert.write(&fresh).await.expect("write fresh");
    insert.end().await.expect("end");
    client
        .query("OPTIMIZE TABLE quant_trade_tape FINAL")
        .execute()
        .await
        .expect("merge trade tape parts");

    let rows_before_revision = read
        .trade_tape_window_by_market(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1,
        )
        .await
        .expect("read before revision");
    assert!(rows_before_revision.is_empty());

    let rows = read
        .trade_tape_window_by_market(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1_000,
        )
        .await
        .expect("read after revision");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ingestion_time, fresh.ingestion_time);
}
