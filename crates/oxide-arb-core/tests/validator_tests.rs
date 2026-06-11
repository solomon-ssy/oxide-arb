//! Validator rejection paths and happy-path coverage.

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_core::{
    execution::validator::Validator,
    observability::metrics_hub::MetricsHub,
    pipeline::{book_store::BookStore, staleness_classifier::StalenessClassifier},
};
use oxide_arb_error::trading::TradingError;
use oxide_arb_models::{
    domain::book::BookLevel,
    runtime_config::{
        EndgameLatencyConfig, ExecutionRuntimeConfig, MarketDataRuntimeConfig, TradeTimeoutConfig,
    },
    types::{Price, Shares, TokenId},
};
use oxide_arb_test_support::fixtures::sample_opportunity;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn level(price: rust_decimal::Decimal) -> BookLevel {
    BookLevel::from_decimal(Price::new(price), Shares::new(dec!(1000))).unwrap()
}

fn execution_config(
    max_book_to_order_ms: u64,
    max_slippage_bps: rust_decimal::Decimal,
) -> ExecutionRuntimeConfig {
    ExecutionRuntimeConfig {
        timeout: TradeTimeoutConfig {
            max_validation_slippage_bps: max_slippage_bps,
            ..Default::default()
        },
        endgame_latency: EndgameLatencyConfig {
            max_book_to_order_ms,
            ..Default::default()
        },
        ..Default::default()
    }
}

struct ValidatorFixture {
    validator: Validator,
    book_store: Arc<BookStore>,
    metrics: Arc<MetricsHub>,
    yes: TokenId,
    no: TokenId,
}

fn fixture(max_book_to_order_ms: u64, max_slippage_bps: rust_decimal::Decimal) -> ValidatorFixture {
    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let classifier = StalenessClassifier::new(&MarketDataRuntimeConfig::default());
    let validator = Validator::new(
        Arc::clone(&book_store),
        classifier,
        &execution_config(max_book_to_order_ms, max_slippage_bps),
        Arc::clone(&metrics),
    );
    ValidatorFixture {
        validator,
        book_store,
        metrics,
        yes: TokenId::new("yes-token"),
        no: TokenId::new("no-token"),
    }
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0).to_u64().unwrap_or(0)
}

fn seed_fresh_books(f: &ValidatorFixture, yes_ask: rust_decimal::Decimal) {
    let ts = now_ms();
    f.book_store
        .apply_snapshot(&f.yes, vec![], vec![level(yes_ask)], ts, None);
    f.book_store.apply_snapshot(
        &f.no,
        vec![level(dec!(0.07))],
        vec![level(dec!(0.08))],
        ts,
        None,
    );
}

#[test]
fn rejects_when_book_not_available() {
    let f = fixture(5_000, dec!(50));
    let opp = sample_opportunity();
    let err = f.validator.validate(&opp, &f.yes, &f.no, 1, 1).unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("book not available")));
}

#[test]
fn rejects_version_regression() {
    let f = fixture(5_000, dec!(50));
    seed_fresh_books(&f, dec!(0.92));
    let opp = sample_opportunity();
    let err = f
        .validator
        .validate(&opp, &f.yes, &f.no, 99, 1)
        .unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("version regressed")));
    assert_eq!(f.metrics.book_freshness_rejected.get(), 1);
}

#[test]
fn rejects_stale_book_age() {
    let f = fixture(100, dec!(50));
    let old_ts = now_ms().saturating_sub(500);
    f.book_store
        .apply_snapshot(&f.yes, vec![], vec![level(dec!(0.92))], old_ts, None);
    f.book_store.apply_snapshot(
        &f.no,
        vec![level(dec!(0.07))],
        vec![level(dec!(0.08))],
        old_ts,
        None,
    );
    let opp = sample_opportunity();
    let err = f.validator.validate(&opp, &f.yes, &f.no, 1, 1).unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("book age")));
    assert_eq!(f.metrics.book_freshness_rejected.get(), 1);
}

#[test]
fn rejects_staleness_above_acceptable() {
    let cfg = MarketDataRuntimeConfig {
        staleness_acceptable_ms: 1_000,
        staleness_stale_ms: 2_000,
        staleness_expired_ms: 10_000,
        ..Default::default()
    };

    let metrics = Arc::new(MetricsHub::new());
    let book_store = Arc::new(BookStore::new(Arc::clone(&metrics)));
    let validator = Validator::new(
        Arc::clone(&book_store),
        StalenessClassifier::new(&cfg),
        &execution_config(60_000, dec!(50)),
        Arc::clone(&metrics),
    );
    let yes = TokenId::new("yes-token");
    let no = TokenId::new("no-token");
    let stale_ts = now_ms().saturating_sub(5_000);
    book_store.apply_snapshot(&yes, vec![], vec![level(dec!(0.92))], stale_ts, None);
    book_store.apply_snapshot(
        &no,
        vec![level(dec!(0.07))],
        vec![level(dec!(0.08))],
        stale_ts,
        None,
    );

    let opp = sample_opportunity();
    let err = validator.validate(&opp, &yes, &no, 1, 1).unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("staleness")));
    assert_eq!(metrics.validation_failures.get(), 1);
}

#[test]
fn rejects_missing_top_of_book() {
    let f = fixture(5_000, dec!(50));
    let ts = now_ms();
    f.book_store
        .apply_snapshot(&f.yes, vec![], vec![], ts, None);
    f.book_store.apply_snapshot(
        &f.no,
        vec![level(dec!(0.07))],
        vec![level(dec!(0.08))],
        ts,
        None,
    );
    let opp = sample_opportunity();
    let err = f.validator.validate(&opp, &f.yes, &f.no, 1, 1).unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("no price")));
}

#[test]
fn rejects_excessive_slippage() {
    let f = fixture(5_000, dec!(10));
    seed_fresh_books(&f, dec!(0.99));
    let opp = sample_opportunity();
    let err = f.validator.validate(&opp, &f.yes, &f.no, 1, 1).unwrap_err();
    assert!(matches!(err, TradingError::Validation(msg) if msg.contains("slippage")));
    assert_eq!(f.metrics.validation_failures.get(), 1);
}

#[test]
fn happy_path_returns_validation_result() {
    let f = fixture(5_000, dec!(500));
    seed_fresh_books(&f, dec!(0.92));
    let opp = sample_opportunity();
    let result = f
        .validator
        .validate(&opp, &f.yes, &f.no, 1, 1)
        .expect("validation should pass");
    assert_eq!(result.current_price.inner(), dec!(0.92));
    assert!(result.slippage_bps.inner() <= dec!(500));
}
