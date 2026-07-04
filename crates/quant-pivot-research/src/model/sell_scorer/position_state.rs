//! Lot position-state pseudo-factors — single source for runtime scoring and
//! offline Sell-scorer training (Phase 06.1).

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{factor::FactorFamily, quant::FactorDirection},
    types::{FactorDefinitionId, Probability},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorExplanation, FactorName, FactorValue, NormalizedFactor, names},
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Fraction of cost basis at which the unrealized-PnL pseudo-factor saturates
/// to a full `±1` signal (a `±20%` unrealized move).
fn unrealized_pnl_scale() -> Decimal {
    Decimal::new(2, 1)
}

/// Lot position-state inputs the Sell scorer weighs alongside market factors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionStateFeatures {
    /// Signed unrealized-PnL fraction of cost basis: `(mark − avg_price)/avg_price`.
    pub unrealized_pnl_pct: Decimal,
    /// Fraction of the scorer horizon elapsed since the lot opened, in `[0, 1]`.
    pub time_in_trade_ratio: Decimal,
    /// Drawdown of the current mark from the lot's peak mark, in `[0, 1]`.
    pub peak_mark_drawdown: Decimal,
}

/// Inputs to derive [`PositionStateFeatures`] at one decision instant.
#[derive(Debug, Clone, Copy)]
pub struct LotStateInput {
    /// Lot average entry price (cost basis per share).
    pub avg_price: Decimal,
    /// Live or historical mark at the decision instant.
    pub mark: Option<Decimal>,
    /// When the lot opened.
    pub opened_at: DateTime<Utc>,
    /// Decision time.
    pub now: DateTime<Utc>,
    /// Hold horizon for the `time_in_trade` feature (secs).
    pub max_hold_secs: u64,
    /// Peak mark observed for the lot before `now`.
    pub peak_mark: Option<Decimal>,
}

/// Derive position-state pseudo-features from ledger + mark inputs.
#[must_use]
pub fn position_state_features(input: LotStateInput) -> PositionStateFeatures {
    let avg = input.avg_price;
    let mark = input.mark;
    let unrealized_pnl_pct = match mark {
        Some(mark) if avg > Decimal::ZERO => (mark - avg) / avg,
        _ => Decimal::ZERO,
    };
    let horizon = input.max_hold_secs.max(1);
    let elapsed = (input.now - input.opened_at).num_seconds().max(0);
    let time_in_trade_ratio =
        (Decimal::from(elapsed) / Decimal::from(horizon)).clamp(Decimal::ZERO, Decimal::ONE);
    let peak_mark_drawdown = match (input.peak_mark, mark) {
        (Some(peak), Some(mark)) if peak > Decimal::ZERO => {
            ((peak - mark) / peak).clamp(Decimal::ZERO, Decimal::ONE)
        }
        _ => Decimal::ZERO,
    };
    PositionStateFeatures {
        unrealized_pnl_pct,
        time_in_trade_ratio,
        peak_mark_drawdown,
    }
}

/// Whether `name` is a position-state pseudo-factor consumed by the Sell scorer.
#[must_use]
pub fn is_position_state_factor(name: &FactorName) -> bool {
    matches!(
        name.as_str(),
        "position_unrealized_pnl_pct" | "position_time_in_trade" | "position_peak_drawdown"
    )
}

/// The signed `[-1, 1]` position-state contributions keyed by pseudo-factor name.
#[must_use]
pub fn position_state_signed(state: &PositionStateFeatures) -> Vec<(FactorName, Decimal)> {
    vec![
        (
            names::POSITION_UNREALIZED_PNL,
            clamp_signed(state.unrealized_pnl_pct / unrealized_pnl_scale()),
        ),
        (
            names::POSITION_TIME_IN_TRADE,
            clamp_signed(state.time_in_trade_ratio),
        ),
        (
            names::POSITION_PEAK_DRAWDOWN,
            clamp_signed(state.peak_mark_drawdown),
        ),
    ]
}

/// Signed contribution for one position-state pseudo-factor (training simplex fit).
#[must_use]
pub fn position_state_signed_contribution(
    state: &PositionStateFeatures,
    factor: &FactorName,
) -> Decimal {
    position_state_signed(state)
        .into_iter()
        .find(|(name, _)| name == factor)
        .map_or(Decimal::ZERO, |(_, signed)| signed)
}

/// Materialize position-state rows as [`FactorValue`]s for dataset export / audit.
#[must_use]
pub fn position_state_factor_values(state: &PositionStateFeatures) -> Vec<FactorValue> {
    position_state_signed(state)
        .into_iter()
        .map(|(name, signed)| FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Momentum,
            raw_value: Some(signed),
            normalization: NormalizedFactor::cross_section(Probability::new(signed.abs())),
            direction: if signed >= Decimal::ZERO {
                FactorDirection::Positive
            } else {
                FactorDirection::Negative
            },
            confidence: Probability::new(Decimal::ONE),
            explanation: FactorExplanation {
                headline: "position_state".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        })
        .collect()
}

fn clamp_signed(value: Decimal) -> Decimal {
    value
        .clamp(-Decimal::ONE, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use super::{LotStateInput, position_state_features, position_state_signed_contribution};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use crate::factors::names;

    #[test]
    fn position_state_matches_runtime_formula() {
        let opened = Utc.timestamp_opt(1_000, 0).single().expect("ts");
        let now = opened + chrono::Duration::seconds(43_200);
        let state = position_state_features(LotStateInput {
            avg_price: dec!(0.40),
            mark: Some(dec!(0.50)),
            opened_at: opened,
            now,
            max_hold_secs: 86_400,
            peak_mark: Some(dec!(0.55)),
        });
        assert_eq!(state.unrealized_pnl_pct, dec!(0.25));
        assert_eq!(state.time_in_trade_ratio, dec!(0.5));
        assert!(state.peak_mark_drawdown > dec!(0));
        let signed = position_state_signed_contribution(&state, &names::POSITION_UNREALIZED_PNL);
        assert!(signed > dec!(0));
    }
}
