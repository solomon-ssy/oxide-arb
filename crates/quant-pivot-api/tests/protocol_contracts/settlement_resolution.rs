use quant_pivot_api::settlement::resolution::{
    FinalizedResolutionVector, ResolutionSourceReadError,
};
use quant_pivot_models::types::PayoutRatio;
use rust_decimal_macros::dec;

#[test]
fn finalized_resolution_vector_preserves_exact_binary_payouts() {
    let winner = FinalizedResolutionVector::try_from_decimal_parts("1", ["1", "0"])
        .expect("winner-take-all payout");
    assert_eq!(
        winner.payout_ratios(),
        [PayoutRatio::ONE, PayoutRatio::ZERO]
    );

    let split = FinalizedResolutionVector::try_from_decimal_parts("2", ["1", "1"])
        .expect("50/50 split payout");
    let half = PayoutRatio::try_new(dec!(0.5)).expect("half payout");
    assert_eq!(split.payout_ratios(), [half, half]);
}

#[test]
fn finalized_resolution_vector_rejects_unresolved_inexact_and_unbalanced_sources() {
    assert!(matches!(
        FinalizedResolutionVector::try_from_decimal_parts("0", ["0", "0"]),
        Err(ResolutionSourceReadError::ConditionNotResolved)
    ));
    assert!(matches!(
        FinalizedResolutionVector::try_from_decimal_parts("3", ["1", "2"]),
        Err(ResolutionSourceReadError::NonTerminatingPayout { .. })
    ));
    assert!(matches!(
        FinalizedResolutionVector::try_from_decimal_parts("2", ["1", "0"]),
        Err(ResolutionSourceReadError::InvalidPayoutVector { .. })
    ));
}
