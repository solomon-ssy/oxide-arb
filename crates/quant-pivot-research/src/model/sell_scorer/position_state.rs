//! Lot position-state pseudo-factors — single source for runtime scoring and
//! offline Sell-scorer training.

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

impl LotStateInput {
    /// Derive position-state pseudo-features from ledger + mark inputs.
    pub fn position_state_features(self) -> QuantResult<PositionStateFeatures> {
        let avg = self.avg_price;
        if avg <= Decimal::ZERO {
            return Err(ResearchError::FactorComputation {
                detail: format!("position state: lot average price must be positive, got {avg}"),
            }
            .into());
        }
        if self.max_hold_secs == 0 {
            return Err(ResearchError::FactorComputation {
                detail: "position state: max_hold_secs must be positive".to_owned(),
            }
            .into());
        }
        let elapsed = (self.now - self.opened_at).num_seconds();
        if elapsed < 0 {
            return Err(ResearchError::FactorComputation {
                detail: format!(
                    "position state: lot opened_at {} is after decision_at {}",
                    self.opened_at, self.now
                ),
            }
            .into());
        }
        let mark = self.mark;
        if mark.is_some_and(|value| value <= Decimal::ZERO) {
            return Err(ResearchError::FactorComputation {
                detail: "position state: mark price must be positive when present".to_owned(),
            }
            .into());
        }
        if self.peak_mark.is_some_and(|value| value <= Decimal::ZERO) {
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
            Decimal::from(self.max_hold_secs),
        )?
        .clamp(Decimal::ZERO, Decimal::ONE);
        let peak_mark_drawdown = self
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
}

/// Whether `name` is a position-state pseudo-factor consumed by the Sell scorer.
#[must_use]
pub fn is_position_state_factor(name: &FactorName) -> bool {
    matches!(
        name.as_str(),
        "position_take_profit_pressure"
            | "position_stop_loss_pressure"
            | "position_time_in_trade"
            | "position_peak_drawdown"
    )
}

impl PositionStateFeatures {
    /// Independent `[0, 1]` direct-exit evidence keyed by intrinsic input name.
    ///
    /// Profit and loss are deliberately separate coordinates: both may support
    /// an exit, while neither can cancel the other by sign convention.
    #[must_use]
    pub fn direct_exit_evidence(&self) -> Vec<(FactorName, Option<Decimal>)> {
        let take_profit = self
            .unrealized_pnl_pct
            .map(|value| clamp_unit(value.max(Decimal::ZERO) / unrealized_pnl_scale()));
        let stop_loss = self
            .unrealized_pnl_pct
            .map(|value| clamp_unit((-value).max(Decimal::ZERO) / unrealized_pnl_scale()));
        vec![
            (names::POSITION_TAKE_PROFIT_PRESSURE, take_profit),
            (names::POSITION_STOP_LOSS_PRESSURE, stop_loss),
            (
                names::POSITION_TIME_IN_TRADE,
                Some(clamp_unit(self.time_in_trade_ratio)),
            ),
            (
                names::POSITION_PEAK_DRAWDOWN,
                self.peak_mark_drawdown.map(clamp_unit),
            ),
        ]
    }
}

/// Direct-exit contribution for one position-state intrinsic input.
#[must_use]
pub fn position_state_exit_contribution(
    state: &PositionStateFeatures,
    factor: &FactorName,
) -> Option<Decimal> {
    (state)
        .direct_exit_evidence()
        .into_iter()
        .find(|(name, _)| name == factor)
        .and_then(|(_, evidence)| evidence)
}

fn clamp_unit(value: Decimal) -> Decimal {
    value
        .clamp(Decimal::ZERO, Decimal::ONE)
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
    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{LotStateInput, position_state_exit_contribution};
    use crate::factors::{
        names,
        names::{POSITION_STOP_LOSS_PRESSURE, POSITION_TAKE_PROFIT_PRESSURE},
    };

    #[test]
    fn position_state_matches_formula() {
        let opened = Utc.timestamp_opt(1_000, 0).single().expect("ts");
        let now = opened + Duration::seconds(43_200);
        let state = (LotStateInput {
            avg_price: dec!(0.40),
            mark: Some(dec!(0.50)),
            opened_at: opened,
            now,
            max_hold_secs: 86_400,
            peak_mark: Some(dec!(0.55)),
        })
        .position_state_features()
        .expect("position state");
        assert_eq!(state.unrealized_pnl_pct, Some(dec!(0.25)));
        assert_eq!(state.time_in_trade_ratio, dec!(0.5));
        assert!(
            state
                .peak_mark_drawdown
                .is_some_and(|value| value > dec!(0))
        );
        let take_profit = position_state_exit_contribution(&state, &POSITION_TAKE_PROFIT_PRESSURE);
        let stop_loss = position_state_exit_contribution(&state, &POSITION_STOP_LOSS_PRESSURE);
        assert!(take_profit.is_some_and(|value| value > dec!(0)));
        assert_eq!(stop_loss, Some(dec!(0)));
    }

    #[test]
    fn missing_mark_peak_missing() {
        let opened = Utc.timestamp_opt(1_000, 0).single().expect("ts");
        let state = (LotStateInput {
            avg_price: dec!(0.40),
            mark: None,
            opened_at: opened,
            now: opened + Duration::seconds(60),
            max_hold_secs: 3_600,
            peak_mark: None,
        })
        .position_state_features()
        .expect("position state");
        assert_eq!(state.unrealized_pnl_pct, None);
        assert_eq!(state.peak_mark_drawdown, None);
        assert_eq!(
            position_state_exit_contribution(&state, &names::POSITION_TAKE_PROFIT_PRESSURE),
            None
        );
    }

    #[test]
    fn pressures_support_exit() {
        let opened = Utc.timestamp_opt(1_000, 0).single().expect("ts");
        let state = |mark| {
            (LotStateInput {
                avg_price: dec!(0.50),
                mark: Some(mark),
                opened_at: opened,
                now: opened + Duration::seconds(60),
                max_hold_secs: 3_600,
                peak_mark: Some(mark),
            })
            .position_state_features()
            .expect("position state")
        };
        let profit = state(dec!(0.60));
        let loss = state(dec!(0.40));

        assert!(
            position_state_exit_contribution(&profit, &POSITION_TAKE_PROFIT_PRESSURE)
                .is_some_and(|value| value > Decimal::ZERO)
        );
        assert!(
            position_state_exit_contribution(&loss, &POSITION_STOP_LOSS_PRESSURE)
                .is_some_and(|value| value > Decimal::ZERO)
        );
    }
}
