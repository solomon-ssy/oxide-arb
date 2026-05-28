use oxide_arb_models::{
    domain::settlement::SettlementEconomics,
    types::{Price, Shares, TokenId, Usd},
};

#[must_use]
pub fn compute_settlement_economics(
    shares: Shares,
    total_cost_usd: Usd,
    total_fees_usd: Usd,
    position_token_id: &TokenId,
    winning_token_id: &TokenId,
) -> SettlementEconomics {
    let won = position_token_id == winning_token_id;
    let payout_usd = if won { shares * Price::ONE } else { Usd::ZERO };
    let realized_pnl_usd = payout_usd - total_cost_usd - total_fees_usd;

    SettlementEconomics {
        won,
        payout_usd,
        realized_pnl_usd,
    }
}
