use std::mem::size_of;

use chrono::{Duration, TimeZone, Utc};
use quant_pivot_error::feedback::FeedbackError;
use quant_pivot_models::{
    domain::ports::{
        FeedbackComparisonContract, FeedbackComparisonExecutionPort, FeedbackComparisonJobParams,
    },
    types::{Bps, ContentHash, Usd, builtin_research_profiles},
};
use quant_pivot_research::{
    backtest::PortfolioReturnObservation,
    feedback_comparison::{
        FeedbackComparisonArtifact, RomanoWolfCandidateInput, RomanoWolfOutcome, RomanoWolfStepdown,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

const fn hash(seed: u8) -> ContentHash {
    ContentHash::from_bytes([seed; 32])
}

fn accepts_execution_port(_: Option<&dyn FeedbackComparisonExecutionPort>) {}

fn contract() -> FeedbackComparisonContract {
    FeedbackComparisonContract::try_from_policy(
        &builtin_research_profiles()
            .expect("built-in profiles")
            .into_iter()
            .next()
            .expect("pooled profile")
            .spec
            .feedback_policy,
    )
    .expect("canonical comparison contract")
}

fn observations(values_bps: &[Decimal]) -> Vec<PortfolioReturnObservation> {
    let start = Utc
        .with_ymd_and_hms(2026, 7, 1, 0, 0, 0)
        .single()
        .expect("valid observation start");
    let capital = dec!(10000);
    values_bps
        .iter()
        .enumerate()
        .map(|(index, value)| PortfolioReturnObservation {
            decision_at: start
                + Duration::minutes(
                    i64::try_from(index)
                        .expect("fixture index fits i64")
                        .checked_mul(5)
                        .expect("fixture timestamp offset"),
                ),
            realized_pnl_usd: Usd::new(
                value
                    .checked_mul(capital)
                    .and_then(|amount| amount.checked_div(dec!(10000)))
                    .expect("fixture PnL"),
            ),
            capital_base_usd: Usd::new(capital),
            net_return_bps: Bps::new(*value),
        })
        .collect()
}

#[test]
fn f09_surface_is_linked() {
    let _ = size_of::<FeedbackComparisonContract>();
    let _ = size_of::<FeedbackComparisonJobParams>();
    let _ = size_of::<PortfolioReturnObservation>();
    let _ = size_of::<FeedbackComparisonArtifact>();
    let _ = size_of::<RomanoWolfStepdown>();

    accepts_execution_port(None);
}

#[test]
fn stepdown_controls_familywise_error() {
    let method = contract();
    let champion = observations(&vec![Decimal::ZERO; 500]);
    let first = observations(&vec![dec!(100); 500]);
    let tied = observations(&vec![dec!(100); 500]);
    let null = observations(&vec![Decimal::ZERO; 500]);
    let candidates = [
        RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(1),
            observations: &first,
        },
        RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(2),
            observations: &tied,
        },
        RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(3),
            observations: &null,
        },
    ];

    let outcome =
        RomanoWolfStepdown::evaluate(&method, &champion, &candidates).expect("comparison");
    let RomanoWolfOutcome::Compared { evidence } = outcome else {
        panic!("observation floor is met");
    };
    let plus_one =
        (Decimal::ONE / Decimal::from(u64::from(method.bootstrap_repetitions()) + 1)).round_dp(12);
    assert_eq!(evidence.observation_count, 500);
    assert_eq!(evidence.simultaneous_critical_value_bps, Bps::ZERO);
    assert_eq!(evidence.candidates[0].effect_bps, Bps::new(dec!(100)));
    assert_eq!(evidence.candidates[0].raw_p_value, plus_one);
    assert_eq!(evidence.candidates[0].adjusted_p_value, plus_one);
    assert_eq!(
        evidence.candidates[0].adjusted_p_value,
        evidence.candidates[1].adjusted_p_value
    );
    assert!(evidence.candidates[0].is_eligible());
    assert!(evidence.candidates[1].is_eligible());
    assert_eq!(evidence.candidates[2].raw_p_value, Decimal::ONE);
    assert_eq!(evidence.candidates[2].adjusted_p_value, Decimal::ONE);
    assert!(!evidence.candidates[2].is_eligible());

    let repeated =
        RomanoWolfStepdown::evaluate(&method, &champion, &candidates).expect("repeat comparison");
    assert_eq!(repeated, RomanoWolfOutcome::Compared { evidence });
}

#[test]
fn insufficient_has_no_evidence() {
    let method = contract();
    let champion = observations(&vec![Decimal::ZERO; 499]);
    let candidate = observations(&vec![dec!(100); 499]);
    let outcome = RomanoWolfStepdown::evaluate(
        &method,
        &champion,
        &[RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(1),
            observations: &candidate,
        }],
    )
    .expect("typed insufficient outcome");
    assert!(matches!(
        outcome,
        RomanoWolfOutcome::InsufficientObservations {
            observed: 499,
            required: 500,
            ..
        }
    ));
}

#[test]
fn window_mismatch_fails_closed() {
    let method = contract();
    let champion = observations(&vec![Decimal::ZERO; 500]);
    let mut candidate = observations(&vec![dec!(100); 500]);
    candidate[7].decision_at += Duration::seconds(1);
    let error = RomanoWolfStepdown::evaluate(
        &method,
        &champion,
        &[RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(1),
            observations: &candidate,
        }],
    )
    .expect_err("timestamp mismatch");
    assert!(matches!(error, FeedbackError::SameWindowMismatch { .. }));

    let mut candidate = observations(&vec![dec!(100); 500]);
    candidate[7].net_return_bps = Bps::new(dec!(99));
    let error = RomanoWolfStepdown::evaluate(
        &method,
        &champion,
        &[RomanoWolfCandidateInput {
            candidate_recipe_hash: hash(1),
            observations: &candidate,
        }],
    )
    .expect_err("return/PnL mismatch");
    assert!(matches!(
        error,
        FeedbackError::InvalidComparisonEvidence { .. }
    ));
}
