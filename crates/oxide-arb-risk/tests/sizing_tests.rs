//! Kelly calculator and multi-constraint sizer tests.
//!
//! Hand-calculated results validated against the mathematical model:
//! Kelly f* = (p * b - q) / b, where b = `net_odds`, q = 1 - `p_effective`.

use oxide_arb_models::{
    domain::risk::ProbabilityInput,
    runtime_config::{KellyConfig, RiskConfig},
    types::Usd,
};
use oxide_arb_risk::sizing::QuarterKellyCalculator;
use rust_decimal_macros::dec;

fn test_config() -> RiskConfig {
    RiskConfig {
        kelly_fraction: dec!(0.25),
        kelly: KellyConfig {
            max_kelly: dec!(0.25),
            min_edge_bps: dec!(200),
            min_probability_confidence: dec!(0.3),
            min_calibration_samples: 10,
            max_probability_staleness_secs: 7200,
        },
        ..RiskConfig::default()
    }
}

const fn test_probability() -> ProbabilityInput {
    ProbabilityInput {
        calibrated_win_prob: dec!(0.95),
        fill_prob: dec!(0.90),
        calibration_confidence: dec!(0.85),
        sample_size: 50,
        model_staleness_secs: 300,
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.005),
    }
}

// ── Hand-calculated Kelly ──────────────────────────────────────────────────

#[test]
fn kelly_hand_calculated_with_haircuts() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);

    // Use high-confidence parameters where effective_p > entry_price.
    let prob = ProbabilityInput {
        calibrated_win_prob: dec!(0.99),
        fill_prob: dec!(0.99),
        calibration_confidence: dec!(0.99),
        sample_size: 100,
        model_staleness_secs: 0, // no staleness haircut
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.005),
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);

    // effective_p = 0.99 * 0.99 * 0.99 * 1.0 = 0.970299
    // gross_odds = (1 - 0.92) / 0.92 = 0.086957
    // net_odds = 0.086957 - 0.005 - 0.005 = 0.076957
    // edge_bps = (0.970299 - 0.92) / 0.92 * 10000 ≈ 546 bps (>> min 200)
    // q = 1 - 0.970299 = 0.029701
    // kelly_raw = (0.970299 * 0.076957 - 0.029701) / 0.076957
    //           = (0.074680 - 0.029701) / 0.076957 ≈ 0.5844
    // kelly_fractional = min(0.5844 * 0.25, 0.25) = 0.1461
    // bet = 1000 * 0.1461 ≈ $146.11

    assert_eq!(result.binding_reason, "kelly");
    assert!(result.bet_usd > Usd::ZERO, "bet should be positive");

    let bet_val = result.bet_usd.inner();
    assert!(
        bet_val >= dec!(140) && bet_val <= dec!(155),
        "expected bet ~$146, got ${bet_val}"
    );

    // Verify intermediate values are sane
    assert!(result.effective_win_prob > dec!(0.96));
    assert!(result.net_odds > dec!(0.07));
    assert!(result.edge_bps > dec!(500));
}

#[test]
fn kelly_with_moderate_haircuts_lower_entry() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = test_probability(); // p=0.95, fill=0.90, conf=0.85, staleness=300s
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.50); // low entry gives large gross_odds

    let result = calc.calculate(&prob, entry_price, bankroll);

    // effective_p = 0.95 * 0.90 * 0.85 * (1 - 300/7200) ≈ 0.6963
    // gross_odds = (1-0.50)/0.50 = 1.0
    // net_odds = 1.0 - 0.005 - 0.005 = 0.99
    // edge_bps = (0.6963-0.50)/0.50*10000 ≈ 3926
    // kelly_raw = (0.6963*0.99 - 0.3037)/0.99 ≈ 0.3895
    // kelly_fractional = min(0.3895*0.25, 0.25) = 0.0974
    // bet = 1000 * 0.0974 ≈ $97.4

    assert_eq!(result.binding_reason, "kelly");
    assert!(result.bet_usd > Usd::ZERO);
    let bet_val = result.bet_usd.inner();
    assert!(
        bet_val >= dec!(90) && bet_val <= dec!(105),
        "expected bet ~$97, got ${bet_val}"
    );
}

// ── No edge returns zero ───────────────────────────────────────────────────

#[test]
fn no_edge_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        calibrated_win_prob: dec!(0.50), // barely at entry price level
        fill_prob: dec!(0.50),
        calibration_confidence: dec!(0.50),
        sample_size: 50,
        model_staleness_secs: 300,
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.005),
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);

    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_ne!(result.binding_reason, "kelly");
}

// ── Certainty gives max kelly ──────────────────────────────────────────────

#[test]
fn certainty_gives_max_kelly() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        calibrated_win_prob: dec!(1.0),
        fill_prob: dec!(1.0),
        calibration_confidence: dec!(1.0),
        sample_size: 1000,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.001),
        expected_failure_cost_pct: dec!(0.001),
    };
    let bankroll = Usd::new(dec!(10000));
    let entry_price = dec!(0.50);

    let result = calc.calculate(&prob, entry_price, bankroll);

    assert_eq!(result.binding_reason, "kelly");
    // With certainty, kelly should be capped at max_kelly
    assert_eq!(result.kelly_fractional, dec!(0.25));
    assert_eq!(result.bet_usd, Usd::new(dec!(2500)));
}

// ── Excessive fees returns zero ────────────────────────────────────────────

#[test]
fn excessive_fees_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        calibrated_win_prob: dec!(0.95),
        fill_prob: dec!(0.90),
        calibration_confidence: dec!(0.85),
        sample_size: 50,
        model_staleness_secs: 300,
        // Fees consume all the edge
        expected_slippage_pct: dec!(0.50),
        expected_failure_cost_pct: dec!(0.50),
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);

    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "negative_odds_after_costs");
}

// ── Zero bankroll returns zero ─────────────────────────────────────────────

#[test]
fn zero_bankroll_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = test_probability();
    let bankroll = Usd::ZERO;
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);

    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "invalid_input");
}

// ── Invalid entry price returns zero ───────────────────────────────────────

#[test]
fn invalid_entry_price_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = test_probability();
    let bankroll = Usd::new(dec!(1000));

    // entry_price >= 1.0 is invalid
    let result = calc.calculate(&prob, dec!(1.0), bankroll);
    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "invalid_input");

    // entry_price <= 0.0 is invalid
    let result = calc.calculate(&prob, dec!(0), bankroll);
    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "invalid_input");
}

// ── Low confidence returns zero ────────────────────────────────────────────

#[test]
fn low_confidence_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        calibration_confidence: dec!(0.1), // below min of 0.3
        ..test_probability()
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);
    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "low_confidence");
}

// ── Insufficient samples returns zero ──────────────────────────────────────

#[test]
fn insufficient_samples_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        sample_size: 5, // below min of 10
        ..test_probability()
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);
    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "insufficient_samples");
}

// ── Stale model returns zero ───────────────────────────────────────────────

#[test]
fn stale_model_returns_zero() {
    let config = test_config();
    let calc = QuarterKellyCalculator::new(&config);
    let prob = ProbabilityInput {
        model_staleness_secs: 10000, // above max of 7200
        ..test_probability()
    };
    let bankroll = Usd::new(dec!(1000));
    let entry_price = dec!(0.92);

    let result = calc.calculate(&prob, entry_price, bankroll);
    assert_eq!(result.bet_usd, Usd::ZERO);
    assert_eq!(result.binding_reason, "stale_model");
}
