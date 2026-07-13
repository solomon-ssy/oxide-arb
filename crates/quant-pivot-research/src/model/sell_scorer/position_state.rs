//! Lot position-state pseudo-factors — single source for runtime scoring and
//! offline Sell-scorer training (Phase 06.1).

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorName, names},
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
    pub unrealized_pnl_pct: Option<Decimal>,
    /// Fraction of the scorer horizon elapsed since the lot opened, in `[0, 1]`.
    pub time_in_trade_ratio: Decimal,
    /// Drawdown of the current mark from the lot's peak mark, in `[0, 1]`.
    pub peak_mark_drawdown: Option<Decimal>,
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
pub fn position_state_features(input: LotStateInput) -> QuantResult<PositionStateFeatures> {
    let avg = input.avg_price;
    if avg <= Decimal::ZERO {
        return Err(ResearchError::FactorComputation {
            detail: format!("position state: lot average price must be positive, got {avg}"),
        }
        .into());
    }
    if input.max_hold_secs == 0 {
        return Err(ResearchError::FactorComputation {
            detail: "position state: max_hold_secs must be positive".to_owned(),
        }
        .into());
    }
    let elapsed = (input.now - input.opened_at).num_seconds();
    if elapsed < 0 {
        return Err(ResearchError::FactorComputation {
            detail: format!(
                "position state: lot opened_at {} is after decision_at {}",
                input.opened_at, input.now
            ),
        }
        .into());
    }
    let mark = input.mark;
    if mark.is_some_and(|value| value <= Decimal::ZERO) {
        return Err(ResearchError::FactorComputation {
            detail: "position state: mark price must be positive when present".to_owned(),
        }
        .into());
    }
    if input.peak_mark.is_some_and(|value| value <= Decimal::ZERO) {
        return Err(ResearchError::FactorComputation {
            detail: "position state: peak mark price must be positive when present".to_owned(),
        }
        .into());
    }
    let unrealized_pnl_pct = mark
        .map(|mark| {
            checked_difference("unrealized PnL", mark, avg)
                .and_then(|difference| checked_ratio("unrealized PnL", difference, avg))
        })
        .transpose()?;
    let time_in_trade_ratio = checked_ratio(
        "time in trade",
        Decimal::from(elapsed),
        Decimal::from(input.max_hold_secs),
    )?
    .clamp(Decimal::ZERO, Decimal::ONE);
    let peak_mark_drawdown = input
        .peak_mark
        .zip(mark)
        .map(|(peak, mark)| {
            checked_difference("peak drawdown", peak, mark)
                .and_then(|difference| checked_ratio("peak drawdown", difference, peak))
        })
        .transpose()?
        .map(|value| value.clamp(Decimal::ZERO, Decimal::ONE));
    Ok(PositionStateFeatures {
        unrealized_pnl_pct,
        time_in_trade_ratio,
        peak_mark_drawdown,
    })
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
pub fn position_state_signed(state: &PositionStateFeatures) -> Vec<(FactorName, Option<Decimal>)> {
    vec![
        (
            names::POSITION_UNREALIZED_PNL,
            state
                .unrealized_pnl_pct
                .map(|value| clamp_signed(value / unrealized_pnl_scale())),
        ),
        (
            names::POSITION_TIME_IN_TRADE,
            Some(clamp_signed(state.time_in_trade_ratio)),
        ),
        (
            names::POSITION_PEAK_DRAWDOWN,
            state.peak_mark_drawdown.map(clamp_signed),
        ),
    ]
}

/// Signed contribution for one position-state pseudo-factor (training simplex fit).
#[must_use]
pub fn position_state_signed_contribution(
    state: &PositionStateFeatures,
    factor: &FactorName,
) -> Option<Decimal> {
    position_state_signed(state)
        .into_iter()
        .find(|(name, _)| name == factor)
        .and_then(|(_, signed)| signed)
}

fn clamp_signed(value: Decimal) -> Decimal {
    value
        .clamp(-Decimal::ONE, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

fn checked_ratio(label: &str, numerator: Decimal, denominator: Decimal) -> QuantResult<Decimal> {
    numerator
        .checked_div(denominator)
        .ok_or_else(|| ResearchError::FactorComputation {
            detail: format!("position state: {label} ratio is undefined or overflowed"),
        })
        .map_err(Into::into)
}

fn checked_difference(label: &str, left: Decimal, right: Decimal) -> QuantResult<Decimal> {
    left.checked_sub(right)
        .ok_or_else(|| ResearchError::FactorComputation {
            detail: format!("position state: {label} subtraction overflow"),
        })
        .map_err(Into::into)
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
        })
        .expect("position state");
        assert_eq!(state.unrealized_pnl_pct, Some(dec!(0.25)));
        assert_eq!(state.time_in_trade_ratio, dec!(0.5));
        assert!(
            state
                .peak_mark_drawdown
                .is_some_and(|value| value > dec!(0))
        );
        let signed = position_state_signed_contribution(&state, &names::POSITION_UNREALIZED_PNL);
        assert!(signed.is_some_and(|value| value > dec!(0)));
    }

    #[test]
    fn missing_mark_and_peak_remain_explicitly_missing() {
        let opened = Utc.timestamp_opt(1_000, 0).single().expect("ts");
        let state = position_state_features(LotStateInput {
            avg_price: dec!(0.40),
            mark: None,
            opened_at: opened,
            now: opened + chrono::Duration::seconds(60),
            max_hold_secs: 3_600,
            peak_mark: None,
        })
        .expect("position state");
        assert_eq!(state.unrealized_pnl_pct, None);
        assert_eq!(state.peak_mark_drawdown, None);
        assert_eq!(
            position_state_signed_contribution(&state, &names::POSITION_UNREALIZED_PNL),
            None
        );
    }
}
