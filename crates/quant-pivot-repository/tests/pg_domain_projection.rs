//! `PostgreSQL` domain-projection storage boundary integration tests.

use chrono::Utc;
use quant_pivot_models::{
    domain::{CryptoPriceReport, DomainSourceCheckpoint},
    entities::quant_crypto_price_projection,
    hashing::CanonicalDigest,
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId, Shares, Usd},
};
use quant_pivot_repository::{
    postgres::PgDomainProjectionRepository, traits::DomainProjectionRepository,
};
use quant_pivot_test_support::pg::setup_pg;
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn crypto_source_sequence_roundtrips_through_postgres_bigint() {
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
    let stored = quant_crypto_price_projection::Entity::find_by_id((
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn crypto_source_sequence_above_postgres_bigint_is_rejected_before_write() {
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
