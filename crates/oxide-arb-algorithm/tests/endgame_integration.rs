//! Integration tests for the endgame detection pipeline.

use std::sync::Arc;

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use oxide_arb_algorithm::{
    calibration::{CalibrationEntry, ResolutionCalibrator},
    endgame::{EndgameDetectInput, EndgameDetector},
    fee::FeeEstimator,
};
use oxide_arb_models::{
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        OrderbookSide,
        book::{BookLevel, BookSnapshot, EndgameBookPair, EndgameBookSnapshot},
        calibration::BucketKey,
    },
    enums::calibration::{DurationBucket, PriceZone},
    enums::common::{MarketCategory, StalenessLevel},
    types::{EventId, MarketId, Price, Shares, TokenId, Usd},
};

/// Zero-fee estimator for deterministic test results.
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

fn side(price: Decimal, size: Decimal) -> OrderbookSide {
    OrderbookSide {
        levels: vec![BookLevel::from_decimal_unchecked(
            Price::new(price),
            Shares::new(size),
        )],
        timestamp_ms: 0,
    }
}

const fn empty_side() -> OrderbookSide {
    OrderbookSide {
        levels: vec![],
        timestamp_ms: 0,
    }
}

fn token_snapshot(bids: &[BookLevel], asks: &[BookLevel]) -> Arc<BookSnapshot> {
    Arc::new(BookSnapshot::new(Arc::from(bids), Arc::from(asks), 0, 0))
}

fn pair_from_snapshot(snapshot: &EndgameBookSnapshot) -> EndgameBookPair {
    EndgameBookPair {
        yes: token_snapshot(&snapshot.yes_bids.levels, &snapshot.yes_asks.levels),
        no: token_snapshot(&snapshot.no_bids.levels, &snapshot.no_asks.levels),
    }
}

fn make_book(yes_ask_price: Decimal, yes_ask_size: Decimal) -> EndgameBookPair {
    pair_from_snapshot(&EndgameBookSnapshot {
        yes_bids: OrderbookSide {
            levels: vec![],
            timestamp_ms: 0,
        },
        yes_asks: side(yes_ask_price, yes_ask_size),
        no_bids: side(dec!(0.02), yes_ask_size),
        no_asks: side(dec!(0.03), yes_ask_size),
    })
}

fn make_detector() -> EndgameDetector<ZeroFeeEstimator> {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let fee_estimator = ZeroFeeEstimator;

    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };

    EndgameDetector::new(&config, &cal_config, calibrator, fee_estimator)
}

struct DetectCase<'a> {
    market_id: &'a MarketId,
    event_id: &'a EventId,
    token_yes: &'a TokenId,
    token_no: &'a TokenId,
    book: &'a EndgameBookPair,
    category: MarketCategory,
    staleness: StalenessLevel,
    settlement_deadline: Option<chrono::DateTime<Utc>>,
    now: chrono::DateTime<Utc>,
}

fn detect(
    detector: &EndgameDetector<ZeroFeeEstimator>,
    case: &DetectCase<'_>,
) -> Option<oxide_arb_models::domain::Opportunity> {
    let direction = detector.detect_direction(case.book.view())?;
    detector.detect_with_direction(
        &EndgameDetectInput {
            market_id: case.market_id,
            event_id: case.event_id,
            token_yes: case.token_yes,
            token_no: case.token_no,
            book: case.book,
            direction,
            category: case.category,
            staleness: case.staleness,
            settlement_deadline: case.settlement_deadline,
        },
        case.now,
    )
}

// ── Happy Path ───────────────────────────────────────────────────────

#[test]
fn happy_path_detects_yes_convergence() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m1"),
            event_id: &EventId::new("e1"),
            token_yes: &TokenId::new("yes-1"),
            token_no: &TokenId::new("no-1"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_some());
    let opp = opp.unwrap();
    assert!(opp.meta.predicted_yes);
    assert!(opp.net_profit.inner() > Decimal::ZERO);
    assert_eq!(opp.entry_price.inner(), dec!(0.97));
}

// ── No Convergence ───────────────────────────────────────────────────

#[test]
fn no_convergence_below_threshold() {
    let detector = make_detector();
    let book = make_book(dec!(0.93), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m1"),
            event_id: &EventId::new("e1"),
            token_yes: &TokenId::new("yes-1"),
            token_no: &TokenId::new("no-1"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_none());
}

// ── Short Convergence Duration ───────────────────────────────────────

#[test]
fn short_convergence_rejected() {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let fee_estimator = ZeroFeeEstimator;

    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 300,
        ..Default::default()
    };

    let detector = EndgameDetector::new(&config, &cal_config, calibrator, fee_estimator);
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m2"),
            event_id: &EventId::new("e2"),
            token_yes: &TokenId::new("yes-2"),
            token_no: &TokenId::new("no-2"),
            book: &book,
            category: MarketCategory::Sports,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(
        opp.is_none(),
        "First scan with min_convergence=300s should return None"
    );
}

// ── Empty Book ───────────────────────────────────────────────────────

#[test]
fn empty_book_returns_none() {
    let detector = make_detector();
    let book = pair_from_snapshot(&EndgameBookSnapshot {
        yes_bids: empty_side(),
        yes_asks: empty_side(),
        no_bids: empty_side(),
        no_asks: empty_side(),
    });
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m3"),
            event_id: &EventId::new("e3"),
            token_yes: &TokenId::new("yes-3"),
            token_no: &TokenId::new("no-3"),
            book: &book,
            category: MarketCategory::Other,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_none());
}

// ── Settlement Too Far ───────────────────────────────────────────────

#[test]
fn settlement_too_far_rejected() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(48);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m4"),
            event_id: &EventId::new("e4"),
            token_yes: &TokenId::new("yes-4"),
            token_no: &TokenId::new("no-4"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_none());
}

// ── No Settlement Deadline ───────────────────────────────────────────

#[test]
fn no_deadline_returns_none() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m5"),
            event_id: &EventId::new("e5"),
            token_yes: &TokenId::new("yes-5"),
            token_no: &TokenId::new("no-5"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: None,
            now: Utc::now(),
        },
    );

    assert!(opp.is_none());
}

// ── Calibration Fallback Chain ───────────────────────────────────────

#[test]
fn calibration_fallback_produces_tier4_for_empty_calibrator() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m6"),
            event_id: &EventId::new("e6"),
            token_yes: &TokenId::new("yes-6"),
            token_no: &TokenId::new("no-6"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_some());
    assert_eq!(opp.unwrap().calibration.fallback_tier, 4);
}

// ── Calibration With Data Uses Tier 1 ────────────────────────────────

#[test]
fn calibration_with_data_uses_tier1() {
    let cal_config = CalibrationConfig {
        min_sample_size: 5,
        ..CalibrationConfig::default()
    };

    let key = BucketKey {
        category: MarketCategory::Geopolitics,
        price_zone: PriceZone::Z97,
        duration_bucket: DurationBucket::Short,
    };
    let entries = vec![CalibrationEntry {
        bucket_key: key,
        total_count: 20,
        correct_count: 19,
        alpha_prior: dec!(2),
        beta_prior: dec!(0.2),
        fallback_tier: 1,
    }];

    let calibrator = Arc::new(ResolutionCalibrator::from_entries(
        entries,
        cal_config.clone(),
    ));
    let fee_estimator = ZeroFeeEstimator;
    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };

    let detector = EndgameDetector::new(&config, &cal_config, calibrator, fee_estimator);
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detect(
        &detector,
        &DetectCase {
            market_id: &MarketId::new("m7"),
            event_id: &EventId::new("e7"),
            token_yes: &TokenId::new("yes-7"),
            token_no: &TokenId::new("no-7"),
            book: &book,
            category: MarketCategory::Geopolitics,
            staleness: StalenessLevel::Fresh,
            settlement_deadline: Some(deadline),
            now,
        },
    );

    assert!(opp.is_some());
    assert_eq!(opp.unwrap().calibration.fallback_tier, 1);
}
