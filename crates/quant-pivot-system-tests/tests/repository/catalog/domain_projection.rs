//! Domain-projection persistence system contracts.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_models::{
    domain::data_plane::{CryptoPriceReport, DomainSourceCheckpoint},
    entities::{
        quant_crypto_price_projection::Entity,
        quant_domain_source_cursor::Entity as DomainSourceCursorEntity,
    },
    hashing::CanonicalDigest,
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId, Shares, Usd},
};
use quant_pivot_repository::{
    postgres::PgDomainProjectionRepository, traits::DomainProjectionRepository,
};
use quant_pivot_system_tests::postgres::setup_pg;
use rust_decimal_macros::dec;
use sea_orm::EntityTrait;

fn report(source_sequence: u64) -> CryptoPriceReport {
    let now = Utc::now();
    CryptoPriceReport {
        source_id: DomainSourceId::binance_agg_trade(),
        instrument_key: DomainInstrumentKey::binance_agg_trade(
            &BinanceSymbol::parse("BTCUSDT").expect("valid test symbol"),
        ),
        source_sequence,
        price: Usd::new(dec!(50000)),
        quantity: Some(Shares::new(dec!(0.01))),
        event_time: now,
        published_at: now,
        available_at: now,
        valid_from: None,
        observations_timestamp: None,
        expires_at: None,
        report_hash: CanonicalDigest::content_hash_json(&serde_json::json!({
            "source_sequence": source_sequence,
        }))
        .expect("canonical report hash"),
        raw_report: r#"{"test":true}"#.to_owned(),
    }
}

pub async fn crypto_source_sequence_bigint() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgDomainProjectionRepository::new(db.clone());
    let source_sequence = 5_123_456_789_u64;
    let report = report(source_sequence);
    let checkpoint = DomainSourceCheckpoint::BinanceAggTrade {
        aggregate_trade_id: source_sequence,
        event_time: report.event_time,
    };

    let projection = repo
        .apply_crypto_report(report, checkpoint, 0, true)
        .await
        .expect("apply crypto projection");

    assert_eq!(projection.source_sequence, source_sequence);
    let stored = Entity::find_by_id((
        DomainSourceId::binance_agg_trade(),
        DomainInstrumentKey::binance_agg_trade(
            &BinanceSymbol::parse("BTCUSDT").expect("valid test symbol"),
        ),
    ))
    .one(&db)
    .await
    .expect("read projection")
    .expect("projection row");
    assert_eq!(stored.source_sequence, 5_123_456_789_i64);
}

pub async fn crypto_rejected_before_write() {
    let (pool, _container) = setup_pg().await;
    let repo = PgDomainProjectionRepository::new(pool.connection().clone());
    let report = report(u64::MAX);
    let checkpoint = DomainSourceCheckpoint::BinanceAggTrade {
        aggregate_trade_id: u64::MAX,
        event_time: report.event_time,
    };

    let error = repo
        .apply_crypto_report(report, checkpoint, 0, true)
        .await
        .expect_err("unsigned sequence outside BIGINT must fail closed");

    assert!(error.to_string().contains("crypto source sequence"));
}

pub async fn crypto_generation_monotonic() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgDomainProjectionRepository::new(db.clone());
    let first = report(10);
    let source_id = first.source_id.clone();
    let instrument_key = first.instrument_key.clone();
    let first_checkpoint = checkpoint(&first);
    repo.apply_crypto_report(first, first_checkpoint, 0, true)
        .await
        .expect("initial projection");

    let gap = repo
        .mark_crypto_source_gap(&source_id, &instrument_key, Utc::now())
        .await
        .expect("durable gap");
    assert_eq!(gap, 1);
    assert_eq!(
        repo.mark_crypto_source_gap(&source_id, &instrument_key, Utc::now())
            .await
            .expect("idempotent unhealthy gap"),
        1
    );

    let next = report(11);
    for invalid_generation in [0, 2] {
        let error = repo
            .apply_crypto_report(next.clone(), checkpoint(&next), invalid_generation, true)
            .await
            .expect_err("generation mismatch must fail closed");
        assert!(error.to_string().contains("gap generation"));
    }
    let stored = Entity::find_by_id((source_id.clone(), instrument_key.clone()))
        .one(&db)
        .await
        .expect("read projection")
        .expect("projection row");
    assert_eq!(stored.source_sequence, 10);
    assert_eq!(stored.gap_generation, 1);
    assert!(!stored.source_healthy);

    let recovered = repo
        .apply_crypto_report(next.clone(), checkpoint(&next), 1, true)
        .await
        .expect("matching generation recovers");
    assert_eq!(recovered.source_sequence, 11);
    assert_eq!(recovered.gap_generation, 1);
    assert!(recovered.source_healthy);
}

pub async fn crypto_equivocation_rejected() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgDomainProjectionRepository::new(db.clone());
    let first = report(42);
    let source_id = first.source_id.clone();
    let instrument_key = first.instrument_key.clone();
    let first_checkpoint = checkpoint(&first);
    repo.apply_crypto_report(first.clone(), first_checkpoint.clone(), 0, true)
        .await
        .expect("initial projection");

    let replay = CryptoPriceReport {
        available_at: first.available_at + Duration::milliseconds(1),
        ..first.clone()
    };
    let replayed = repo
        .apply_crypto_report(replay, first_checkpoint.clone(), 0, true)
        .await
        .expect("exact source replay is idempotent");
    assert_eq!(replayed.report_hash, first.report_hash);

    let equivocation = CryptoPriceReport {
        price: Usd::new(dec!(50001)),
        report_hash: CanonicalDigest::content_hash_json(&serde_json::json!({
            "source_sequence": first.source_sequence,
            "equivocation": true,
        }))
        .expect("equivocation hash"),
        available_at: first.available_at + Duration::milliseconds(2),
        ..first.clone()
    };
    let error = repo
        .apply_crypto_report(equivocation, first_checkpoint.clone(), 0, true)
        .await
        .expect_err("same checkpoint with a different hash must fail closed");
    assert!(error.to_string().contains("equivocation"));

    let stored = Entity::find_by_id((source_id.clone(), instrument_key.clone()))
        .one(&db)
        .await
        .expect("read projection")
        .expect("projection row");
    assert_eq!(stored.report_hash, first.report_hash);
    let cursor = DomainSourceCursorEntity::find_by_id((source_id, instrument_key))
        .one(&db)
        .await
        .expect("read cursor")
        .expect("cursor row");
    assert_eq!(
        cursor.checkpoint_hash,
        CanonicalDigest::content_hash_json(&first_checkpoint).expect("hash")
    );
}

pub async fn crypto_rtds_order_monotonic() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgDomainProjectionRepository::new(db.clone());
    let source_time = Utc::now();
    let first = rtds_report(source_time, source_time, "first");
    repo.apply_crypto_report(first.clone(), rtds_checkpoint(&first), 0, true)
        .await
        .expect("initial RTDS projection");

    let correction = rtds_report(
        source_time,
        source_time + Duration::milliseconds(1),
        "correction",
    );
    let corrected = repo
        .apply_crypto_report(correction.clone(), rtds_checkpoint(&correction), 0, true)
        .await
        .expect("newer RTDS envelope advances");
    assert_eq!(corrected.report_hash, correction.report_hash);

    let stale = CryptoPriceReport {
        available_at: correction.available_at + Duration::milliseconds(1),
        report_hash: CanonicalDigest::content_hash_json(&"stale").expect("stale hash"),
        raw_report: "stale".to_owned(),
        ..first.clone()
    };
    assert!(
        repo.apply_crypto_report(stale.clone(), rtds_checkpoint(&stale), 0, true)
            .await
            .expect_err("older RTDS envelope must not regain authority")
            .to_string()
            .contains("regressed")
    );

    let equivocation = CryptoPriceReport {
        available_at: correction.available_at + Duration::milliseconds(2),
        report_hash: CanonicalDigest::content_hash_json(&"equivocation")
            .expect("equivocation hash"),
        raw_report: "equivocation".to_owned(),
        ..correction.clone()
    };
    assert!(
        repo.apply_crypto_report(
            equivocation.clone(),
            rtds_checkpoint(&equivocation),
            0,
            true,
        )
        .await
        .expect_err("same RTDS tuple with another hash must fail closed")
        .to_string()
        .contains("equivocation")
    );
}

const fn checkpoint(report: &CryptoPriceReport) -> DomainSourceCheckpoint {
    DomainSourceCheckpoint::BinanceAggTrade {
        aggregate_trade_id: report.source_sequence,
        event_time: report.event_time,
    }
}

fn rtds_report(
    event_time: DateTime<Utc>,
    published_at: DateTime<Utc>,
    raw_report: &str,
) -> CryptoPriceReport {
    CryptoPriceReport {
        source_id: DomainSourceId::polymarket_rtds_binance(),
        instrument_key: DomainInstrumentKey::new("RTDS:BINANCE:BTCUSDT"),
        source_sequence: u64::try_from(event_time.timestamp_millis()).expect("timestamp"),
        price: Usd::new(dec!(50000)),
        quantity: None,
        event_time,
        published_at,
        available_at: published_at,
        valid_from: None,
        observations_timestamp: None,
        expires_at: None,
        report_hash: CanonicalDigest::content_hash_json(&raw_report).expect("report hash"),
        raw_report: raw_report.to_owned(),
    }
}

const fn rtds_checkpoint(report: &CryptoPriceReport) -> DomainSourceCheckpoint {
    DomainSourceCheckpoint::PolymarketRtds {
        source_timestamp: report.event_time,
        envelope_timestamp: report.published_at,
        report_hash: report.report_hash,
    }
}
