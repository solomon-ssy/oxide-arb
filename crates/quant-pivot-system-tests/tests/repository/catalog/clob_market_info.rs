//! CLOB market-info append-only and concurrent idempotency contracts.

use std::{slice, sync::Arc};

use chrono::{Duration, TimeZone, Utc};
use futures_util::{StreamExt, TryStreamExt, stream};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    enums::common::{MarketCategory, TickSize},
    hashing::CanonicalDigest,
    types::{
        ClobFeeDetails, ClobMarketInfoVersion, ClobMarketInfoVersionId, ClobTokenDescriptor,
        MarketId, TokenId,
    },
};
use quant_pivot_repository::{
    postgres::{PgClobMarketInfoRepository, PgEventRepository, PgMarketRepository},
    traits::{ClobMarketInfoRepository, EventRepository, MarketRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::catalog_fixtures::{make_event, make_market},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn observation(
    market_id: &MarketId,
    version_id: ClobMarketInfoVersionId,
    revision: u32,
) -> ClobMarketInfoVersion {
    let effective_at = Utc
        .with_ymd_and_hms(2026, 8, 1, 0, 0, 0)
        .single()
        .expect("observation time");
    let raw_payload = serde_json::json!({
        "market_id": market_id,
        "revision": revision,
    });
    ClobMarketInfoVersion {
        version_id,
        market_id: market_id.clone(),
        tokens: vec![
            ClobTokenDescriptor {
                token_id: TokenId::new("clob-info-yes"),
                outcome: "Yes".to_owned(),
            },
            ClobTokenDescriptor {
                token_id: TokenId::new("clob-info-no"),
                outcome: "No".to_owned(),
            },
        ],
        tick_size: TickSize::Hundredth,
        minimum_order_size: dec!(1),
        neg_risk: false,
        taker_order_delay_enabled: false,
        minimum_order_age_secs: None,
        blockaid_check_enabled: false,
        fee_details: ClobFeeDetails {
            rate: Decimal::ZERO,
            exponent: 1,
            taker_only: true,
        },
        builder_maker_fee_rate_bps: 0,
        builder_taker_fee_rate_bps: 0,
        effective_at,
        available_at: effective_at,
        payload_hash: CanonicalDigest::content_hash_json(&raw_payload).expect("payload hash"),
        raw_payload,
    }
}

pub async fn concurrent_retries_deduplicate() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let event_id = "event-clob-info-concurrency";
    let market_id = MarketId::new("market-clob-info-concurrency");
    PgEventRepository::new(db.clone())
        .upsert(make_event(
            event_id,
            "CLOB info concurrency",
            "clob-info-concurrency",
            MarketCategory::Weather,
        ))
        .await
        .expect("seed event");
    PgMarketRepository::new(db.clone())
        .upsert(make_market(
            market_id.as_str(),
            event_id,
            "Will the concurrent observation deduplicate?",
            "clob-info-concurrent-observation",
            MarketCategory::Weather,
            None,
        ))
        .await
        .expect("seed market");

    let repository = Arc::new(PgClobMarketInfoRepository::new(db));
    let version_id = ClobMarketInfoVersionId::from_v7();
    let expected = observation(&market_id, version_id, 1);
    let inserted = stream::iter(0..32)
        .map(|_| {
            let repository = Arc::clone(&repository);
            let observation = expected.clone();
            async move { repository.insert_observation(observation).await }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await
        .expect("concurrent exact retries");
    assert_eq!(inserted.len(), 32);
    assert!(inserted.iter().all(|value| value == &expected));

    let stored = repository
        .window(
            slice::from_ref(&market_id),
            expected.effective_at - Duration::seconds(1),
            expected.effective_at + Duration::seconds(1),
            expected.available_at,
        )
        .await
        .expect("load deduplicated observation");
    assert_eq!(stored, vec![expected.clone()]);

    let collision = repository
        .insert_observation(observation(&market_id, version_id, 2))
        .await
        .expect_err("version collision with different content must fail closed");
    assert!(matches!(collision, StorageError::InvariantViolation { .. }));
}
