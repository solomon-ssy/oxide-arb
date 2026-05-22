//! Integration tests for the endgame detection pipeline.

use std::sync::Arc;

use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use oxide_arb_algorithm::{
    calibration::{CalibrationEntry, ResolutionCalibrator},
    endgame::EndgameDetector,
    fee::FeeEstimator,
};
use oxide_arb_models::{
    config::{CalibrationConfig, EndgameDetectionConfig},
    domain::{
        BookLevel, EndgameBookSnapshot, OrderbookSide,
        calibration::{BucketKey, DurationBucket, PriceZone},
    },
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
        levels: vec![BookLevel {
            price: Price::new(price),
            size: Shares::new(size),
        }],
        timestamp_ms: 0,
    }
}

const fn empty_side() -> OrderbookSide {
    OrderbookSide {
        levels: vec![],
        timestamp_ms: 0,
    }
}

fn make_book(yes_ask_price: Decimal, yes_ask_size: Decimal) -> EndgameBookSnapshot {
    EndgameBookSnapshot {
        yes_bids: OrderbookSide {
            levels: vec![],
            timestamp_ms: 0,
        },
        yes_asks: side(yes_ask_price, yes_ask_size),
        no_bids: side(dec!(0.02), yes_ask_size),
        no_asks: side(dec!(0.03), yes_ask_size),
    }
}

fn make_detector() -> EndgameDetector {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let fee_estimator: Arc<dyn FeeEstimator> = Arc::new(ZeroFeeEstimator);

    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };

    EndgameDetector::new(config, &cal_config, calibrator, fee_estimator)
}

// ── Happy Path ───────────────────────────────────────────────────────

#[test]
fn happy_path_detects_yes_convergence() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detector.detect(
        &MarketId::new("m1"),
        &EventId::new("e1"),
        &TokenId::new("yes-1"),
        &TokenId::new("no-1"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
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

    let opp = detector.detect(
        &MarketId::new("m1"),
        &EventId::new("e1"),
        &TokenId::new("yes-1"),
        &TokenId::new("no-1"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
    );

    assert!(opp.is_none());
}

// ── Short Convergence Duration ───────────────────────────────────────

#[test]
fn short_convergence_rejected() {
    let cal_config = CalibrationConfig::default();
    let calibrator = Arc::new(ResolutionCalibrator::empty(cal_config.clone()));
    let fee_estimator: Arc<dyn FeeEstimator> = Arc::new(ZeroFeeEstimator);

    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 300,
        ..Default::default()
    };

    let detector = EndgameDetector::new(config, &cal_config, calibrator, fee_estimator);
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detector.detect(
        &MarketId::new("m2"),
        &EventId::new("e2"),
        &TokenId::new("yes-2"),
        &TokenId::new("no-2"),
        &book,
        MarketCategory::Sports,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
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
    let book = EndgameBookSnapshot {
        yes_bids: empty_side(),
        yes_asks: empty_side(),
        no_bids: empty_side(),
        no_asks: empty_side(),
    };
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detector.detect(
        &MarketId::new("m3"),
        &EventId::new("e3"),
        &TokenId::new("yes-3"),
        &TokenId::new("no-3"),
        &book,
        MarketCategory::Other,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
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

    let opp = detector.detect(
        &MarketId::new("m4"),
        &EventId::new("e4"),
        &TokenId::new("yes-4"),
        &TokenId::new("no-4"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
    );

    assert!(opp.is_none());
}

// ── No Settlement Deadline ───────────────────────────────────────────

#[test]
fn no_deadline_returns_none() {
    let detector = make_detector();
    let book = make_book(dec!(0.97), dec!(1000));

    let opp = detector.detect(
        &MarketId::new("m5"),
        &EventId::new("e5"),
        &TokenId::new("yes-5"),
        &TokenId::new("no-5"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        None,
        Utc::now(),
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

    let opp = detector.detect(
        &MarketId::new("m6"),
        &EventId::new("e6"),
        &TokenId::new("yes-6"),
        &TokenId::new("no-6"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
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
    let fee_estimator: Arc<dyn FeeEstimator> = Arc::new(ZeroFeeEstimator);
    let config = EndgameDetectionConfig {
        min_convergence_duration_secs: 0,
        ..Default::default()
    };

    let detector = EndgameDetector::new(config, &cal_config, calibrator, fee_estimator);
    let book = make_book(dec!(0.97), dec!(1000));
    let now = Utc::now();
    let deadline = now + Duration::hours(12);

    let opp = detector.detect(
        &MarketId::new("m7"),
        &EventId::new("e7"),
        &TokenId::new("yes-7"),
        &TokenId::new("no-7"),
        &book,
        MarketCategory::Geopolitics,
        StalenessLevel::Fresh,
        Some(deadline),
        now,
    );

    assert!(opp.is_some());
    assert_eq!(opp.unwrap().calibration.fallback_tier, 1);
}
