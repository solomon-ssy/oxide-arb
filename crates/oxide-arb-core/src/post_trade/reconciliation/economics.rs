//! Recompute fee and fill-time EV from proven CLOB economics.

use chrono::Utc;
use oxide_arb_api::{clob::ClobTrade, fees::FeeCalculator};
use oxide_arb_error::fee::FeeQuoteError;
use oxide_arb_models::{
    domain::{
        fee::FeeQuoteInput,
        trade::{TradeInfo, TradeObservation},
        trading::execution::fill_expected_net_profit,
    },
    enums::{
        common::{ExecutionMode, TradeState},
        fee::FeeLiquidityRole,
    },
    types::{Price, Shares, Usd},
};
use rust_decimal::Decimal;

/// Build a reconciled fill observation with Live-accurate fee quote and EV.
pub fn reconciled_fill_economics(
    trade: &TradeInfo,
    clob_trade: Option<&ClobTrade>,
    shares: Shares,
    price: Price,
    fee_calculator: &FeeCalculator,
    resolution_prob: Decimal,
    mode: ExecutionMode,
) -> Result<TradeObservation, FeeQuoteError> {
    let (shares, price) = clob_trade.map_or((shares, price), |clob| (clob.size, clob.price));
    let cost_usd = shares * price;
    let fee_input = FeeQuoteInput {
        market_id: trade.market_id.clone(),
        token_id: trade.token_id.clone(),
        category: trade.category,
        side: trade.side,
        liquidity_role: FeeLiquidityRole::Taker,
        shares,
        price,
        allow_category_fallback: mode != ExecutionMode::Live,
    };
    let fee_usd = fee_calculator
        .quote_for_mode(mode, fee_input)
        .map(|quote| quote.fee_usd)?;
    let net_profit_usd = Some(fill_expected_net_profit(
        resolution_prob,
        shares,
        cost_usd,
        fee_usd,
    ));
    Ok(TradeObservation {
        state: TradeState::FillObserved,
        shares,
        price,
        cost_usd,
        fee_usd,
        order_id: clob_trade
            .map(|c| c.order_id.clone())
            .or_else(|| trade.order_id.clone()),
        tx_hash: clob_trade
            .map(|c| c.tx_hash.clone())
            .or_else(|| trade.tx_hash.clone()),
        net_profit_usd,
        latency_ms: None,
        error_message: Some("reconciled fill with recomputed fee/EV".to_owned()),
        confirmed_at: Utc::now(),
    })
}

/// Resolve `resolution_prob` from the frozen scored snapshot; errors defer the trade.
pub fn resolution_prob_from_trade(trade: &TradeInfo) -> Result<Decimal, String> {
    let snapshot = trade
        .scored_opportunity_snapshot()
        .map_err(|error| error.to_string())?;
    Ok(snapshot.resolution_prob_decimal)
}

/// Resize reservation when actual fill cost is below the approved reservation.
pub fn reservation_amount_after_fill(cost_usd: Usd, fee_usd: Usd) -> Usd {
    cost_usd + fee_usd
}
