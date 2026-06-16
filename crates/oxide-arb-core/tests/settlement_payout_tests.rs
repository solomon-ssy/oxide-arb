use oxide_arb_core::execution::settlement::payout::compute_settlement_economics;
use oxide_arb_models::types::{Shares, TokenId, Usd};
use rust_decimal_macros::dec;

#[test]
fn winning_token_pays_one_usd_per_share_net_of_cost_and_fees() {
    let token = TokenId::new("yes-token");
    let economics = compute_settlement_economics(
        Shares::new(dec!(100)),
        Usd::new(dec!(92)),
        Usd::new(dec!(0.4)),
        Usd::ZERO,
        &token,
        &token,
    );

    assert!(economics.won);
    assert_eq!(economics.payout_usd, Usd::new(dec!(100)));
    assert_eq!(economics.realized_pnl_usd, Usd::new(dec!(7.6)));
}

#[test]
fn losing_token_pays_zero_and_books_full_cost_plus_fee_loss() {
    let economics = compute_settlement_economics(
        Shares::new(dec!(100)),
        Usd::new(dec!(92)),
        Usd::new(dec!(0.4)),
        Usd::ZERO,
        &TokenId::new("yes-token"),
        &TokenId::new("no-token"),
    );

    assert!(!economics.won);
    assert_eq!(economics.payout_usd, Usd::ZERO);
    assert_eq!(economics.realized_pnl_usd, Usd::new(dec!(-92.4)));
}

#[test]
fn zero_share_settlement_is_zero_even_when_token_wins() {
    let token = TokenId::new("yes-token");
    let economics = compute_settlement_economics(
        Shares::ZERO,
        Usd::ZERO,
        Usd::ZERO,
        Usd::ZERO,
        &token,
        &token,
    );

    assert!(economics.won);
    assert_eq!(economics.payout_usd, Usd::ZERO);
    assert_eq!(economics.realized_pnl_usd, Usd::ZERO);
}

#[test]
fn payout_uses_token_identity_not_outcome_label_or_trade_side() {
    let economics = compute_settlement_economics(
        Shares::new(dec!(25)),
        Usd::new(dec!(10)),
        Usd::ZERO,
        Usd::ZERO,
        &TokenId::new("neg-risk-outcome-7"),
        &TokenId::new("neg-risk-outcome-7"),
    );

    assert!(economics.won);
    assert_eq!(economics.payout_usd, Usd::new(dec!(25)));
    assert_eq!(economics.realized_pnl_usd, Usd::new(dec!(15)));
}

#[test]
fn redeem_gas_reduces_realized_pnl_on_winning_position() {
    let token = TokenId::new("yes-token");
    let economics = compute_settlement_economics(
        Shares::new(dec!(100)),
        Usd::new(dec!(92)),
        Usd::new(dec!(0.4)),
        Usd::new(dec!(1.5)),
        &token,
        &token,
    );

    assert!(economics.won);
    assert_eq!(economics.realized_pnl_usd, Usd::new(dec!(6.1)));
}
