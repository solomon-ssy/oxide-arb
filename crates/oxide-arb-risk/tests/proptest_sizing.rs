//! Property-based tests for position sizing.
//!
//! Validates invariants that must hold for ALL valid inputs:
//! - Kelly bet is always non-negative
//! - Kelly bet never exceeds bankroll
//! - Kelly bet is monotone in win probability
//! - Daily loss accumulator is always non-negative

use oxide_arb_models::config::{KellyConfig, RiskConfig};
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::enums::common::TradeOutcome;
use oxide_arb_models::types::Usd;
use oxide_arb_risk::accounting::DailyAccounting;
use oxide_arb_risk::sizing::QuarterKellyCalculator;
use proptest::prelude::*;
use rust_decimal::Decimal;
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

// ── Kelly bet is always non-negative ───────────────────────────────────────

proptest! {
    #[test]
    fn kelly_bet_non_negative(
        win_prob in 0.01f64..1.0,
        fill_prob in 0.01f64..1.0,
        confidence in 0.3f64..1.0,
        entry_price in 0.01f64..0.99,
        bankroll in 1.0f64..100_000.0,
    ) {
        let config = test_config();
        let calc = QuarterKellyCalculator::new(&config);

        let prob = ProbabilityInput {
            calibrated_win_prob: Decimal::from_f64_retain(win_prob).unwrap(),
            fill_prob: Decimal::from_f64_retain(fill_prob).unwrap(),
            calibration_confidence: Decimal::from_f64_retain(confidence).unwrap(),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        };

        let entry = Decimal::from_f64_retain(entry_price).unwrap();
        let bank = Usd::new(Decimal::from_f64_retain(bankroll).unwrap());

        let result = calc.calculate(&prob, entry, bank);
        prop_assert!(result.bet_usd >= Usd::ZERO, "bet was negative: {}", result.bet_usd);
    }
}

// ── Kelly bet within bankroll ──────────────────────────────────────────────

proptest! {
    #[test]
    fn kelly_bet_within_bankroll(
        win_prob in 0.01f64..1.0,
        fill_prob in 0.01f64..1.0,
        confidence in 0.3f64..1.0,
        entry_price in 0.01f64..0.99,
        bankroll in 1.0f64..100_000.0,
    ) {
        let config = test_config();
        let calc = QuarterKellyCalculator::new(&config);

        let prob = ProbabilityInput {
            calibrated_win_prob: Decimal::from_f64_retain(win_prob).unwrap(),
            fill_prob: Decimal::from_f64_retain(fill_prob).unwrap(),
            calibration_confidence: Decimal::from_f64_retain(confidence).unwrap(),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        };

        let entry = Decimal::from_f64_retain(entry_price).unwrap();
        let bank = Usd::new(Decimal::from_f64_retain(bankroll).unwrap());

        let result = calc.calculate(&prob, entry, bank);
        prop_assert!(
            result.bet_usd <= bank,
            "bet {} exceeded bankroll {}", result.bet_usd, bank
        );
    }
}

// ── Kelly bet is monotone in win probability ───────────────────────────────

proptest! {
    #[test]
    fn kelly_monotone_in_win_prob(
        base_prob in 0.5f64..0.9,
        delta in 0.01f64..0.09,
        fill_prob in 0.5f64..1.0,
        confidence in 0.5f64..1.0,
        entry_price in 0.5f64..0.95,
        bankroll in 100.0f64..10_000.0,
    ) {
        let config = test_config();
        let calc = QuarterKellyCalculator::new(&config);

        let low_prob = base_prob;
        let high_prob = (base_prob + delta).min(0.999);

        let prob_low = ProbabilityInput {
            calibrated_win_prob: Decimal::from_f64_retain(low_prob).unwrap(),
            fill_prob: Decimal::from_f64_retain(fill_prob).unwrap(),
            calibration_confidence: Decimal::from_f64_retain(confidence).unwrap(),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        };

        let entry = Decimal::from_f64_retain(entry_price).unwrap();
        let bank = Usd::new(Decimal::from_f64_retain(bankroll).unwrap());

        let result_low = calc.calculate(&prob_low, entry, bank);

        let prob_high = ProbabilityInput {
            calibrated_win_prob: Decimal::from_f64_retain(high_prob).unwrap(),
            ..prob_low
        };

        let result_high = calc.calculate(&prob_high, entry, bank);

        prop_assert!(
            result_high.bet_usd >= result_low.bet_usd,
            "higher win_prob ({high_prob}) gave lower bet ({}) than ({low_prob}) bet ({})",
            result_high.bet_usd, result_low.bet_usd
        );
    }
}

// ── Daily loss accumulator is always non-negative ──────────────────────────

proptest! {
    #[test]
    fn daily_loss_non_negative(
        profits in proptest::collection::vec(-50.0f64..50.0, 1..20),
    ) {
        let mut daily = DailyAccounting::new(Usd::new(dec!(10000)));

        for p in &profits {
            let profit = Decimal::from_f64_retain(*p).unwrap();
            let outcome = if *p >= 0.0 { TradeOutcome::Success } else { TradeOutcome::Miss };
            daily.record_trade(
                Usd::new(profit),
                Usd::new(dec!(0.1)),
                Usd::new(dec!(10)),
                outcome,
            );
        }

        prop_assert!(
            daily.daily_loss() >= Usd::ZERO,
            "daily_loss was negative: {}", daily.daily_loss()
        );
    }
}
