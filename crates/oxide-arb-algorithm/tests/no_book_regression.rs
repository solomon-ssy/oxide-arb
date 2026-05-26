//! Regression tests for strict NO-token orderbook handling.

use std::sync::Arc;

use chrono::{Duration, Utc};
use oxide_arb_algorithm::{
    calibration::ResolutionCalibrator, endgame::EndgameDetector, fee::FeeEstimator,
};
use oxide_arb_models::{
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        OrderbookSide,
        book::{BookLevel, BookSnapshot, EndgameBookPair, EndgameBookSnapshot},
    },
    enums::common::{MarketCategory, StalenessLevel},
    types::{EventId, MarketId, Price, Shares, TokenId, Usd},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

struct ZeroFeeEstimator;

impl FeeEstimator for ZeroFeeEstimator {
    fn estimate_fee(
        &self,
        _shares: Shares,
        _price: Price,
        _category: MarketCategory,
        _token_id: &TokenId,
    ) -> Usd {
        Usd::ZERO
    }
}

fn level(price: Decimal, size: Decimal) -> BookLevel {
    BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
}

const fn side(levels: Vec<BookLevel>) -> OrderbookSide {
    OrderbookSide {
        levels,
        timestamp_ms: 0,
    }
}

fn token_snapshot(bids: &[BookLevel], asks: &[BookLevel]) -> Arc<BookSnapshot> {
    Arc::new(BookSnapshot::new(Arc::from(bids), Arc::from(asks), 0))
}

fn pair_from_snapshot(snapshot: &EndgameBookSnapshot) -> EndgameBookPair {
    EndgameBookPair {
        yes: token_snapshot(&snapshot.yes_bids.levels, &snapshot.yes_asks.levels),
        no: token_snapshot(&snapshot.no_bids.levels, &snapshot.no_asks.levels),
    }
}

fn make_book(
    yes_bids: Vec<BookLevel>,
    yes_asks: Vec<BookLevel>,
    no_bids: Vec<BookLevel>,
    no_asks: Vec<BookLevel>,
) -> EndgameBookPair {
    pair_from_snapshot(&EndgameBookSnapshot {
        yes_bids: side(yes_bids),
        yes_asks: side(yes_asks),
        no_bids: side(no_bids),
        no_asks: side(no_asks),
    })
}

fn detector() -> EndgameDetector<ZeroFeeEstimator> {
    let calibration = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(calibration.clone()));
    let fees = ZeroFeeEstimator;
    let detection = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };

    EndgameDetector::new(&detection, &calibration, calibrator, fees)
}

fn detect(book: &EndgameBookPair) -> Option<oxide_arb_models::domain::Opportunity> {
    let detector = detector();
    let direction = detector.detect_direction(book.view())?;
    detector.detect_with_direction(
        &MarketId::new("m-no"),
        &EventId::new("e-no"),
        &TokenId::new("yes-no"),
        &TokenId::new("no-no"),
        book,
        direction,
        MarketCategory::Sports,
        StalenessLevel::Fresh,
        Some(Utc::now() + Duration::hours(6)),
        Utc::now(),
    )
}

#[test]
fn no_convergence_buys_no_from_no_ask_book() {
    let book = make_book(
        vec![level(dec!(0.02), dec!(5000))],
        vec![level(dec!(0.03), dec!(5000))],
        vec![level(dec!(0.96), dec!(200))],
        vec![level(dec!(0.97), dec!(200))],
    );

    let opp = detect(&book).expect("NO convergence should produce an opportunity");

    assert!(!opp.meta.predicted_yes);
    assert_eq!(opp.token_id, TokenId::new("no-no"));
    assert_eq!(opp.entry_price.inner(), dec!(0.97));
    assert_eq!(opp.shares.inner(), dec!(200));
    assert_eq!(opp.total_cost.inner(), dec!(194));
}

#[test]
fn no_depth_is_not_inferred_from_yes_depth() {
    let book = make_book(
        vec![level(dec!(0.02), dec!(5000))],
        vec![level(dec!(0.03), dec!(5000))],
        vec![level(dec!(0.96), dec!(200))],
        vec![level(dec!(0.97), dec!(200))],
    );

    let opp = detect(&book).expect("NO convergence should use real NO depth");

    assert_eq!(
        opp.shares.inner(),
        dec!(200),
        "shares must come from NO book, not YES book"
    );
}

#[test]
fn yes_low_without_no_high_does_not_trigger_no_convergence() {
    let book = make_book(
        vec![level(dec!(0.02), dec!(5000))],
        vec![level(dec!(0.03), dec!(5000))],
        vec![level(dec!(0.88), dec!(1000))],
        vec![level(dec!(0.90), dec!(1000))],
    );

    assert!(
        detect(&book).is_none(),
        "YES low-threshold alone must not synthesize a NO signal"
    );
}

#[test]
fn yes_convergence_still_uses_yes_ask_book() {
    let book = make_book(
        vec![level(dec!(0.96), dec!(1000))],
        vec![level(dec!(0.97), dec!(1000))],
        vec![level(dec!(0.02), dec!(5000))],
        vec![level(dec!(0.03), dec!(5000))],
    );

    let opp = detect(&book).expect("YES convergence should still work");

    assert!(opp.meta.predicted_yes);
    assert_eq!(opp.token_id, TokenId::new("yes-no"));
    assert_eq!(opp.entry_price.inner(), dec!(0.97));
}
