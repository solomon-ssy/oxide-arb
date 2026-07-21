//! Lot-native return series for Sell Sharpe / DSR / PBO.
//!
//! Lot outcomes are **not** an equal-interval calendar process: lots open at
//! irregular times and may cluster. Bailey–López de Prado DSR/PSR need a
//! homogeneous *observation* process — **not** a wall-clock grid padded with
//! zeros. Padding empty calendar buckets with `0` silently deflates variance
//! and inflates Sharpe; that path is forbidden.
//!
//! This module therefore:
//!
//! 1. Orders lots by first-decision `as_of` (already the CPCV timeline order).
//! 2. Optionally bins *coincident* lots that share the same activity bucket
//!    (same epoch multiple of `period_secs`) by **summing** their returns —
//!    equal-weight lot contribution within a simultaneous open cluster.
//! 3. **Drops empty buckets** — only buckets that contain ≥ 1 lot become
//!    observations. The resulting series length equals the number of distinct
//!    active buckets (≤ lot count), never a zero-padded wall-clock span.
//!
//! Callers that need ≥ `S` CSCV blocks must require
//! `active_observation_count >= S` (lot-native), not
//! `calendar_span / period_secs`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::backtest::LotOutcome;

/// One observation in the lot-native (activity-only) return series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalendarReturn {
    /// Inclusive bucket start (epoch-aligned when `period_secs > 0`).
    pub period_start: DateTime<Utc>,
    /// Sum of lot `return_value`s whose `as_of` falls in this active bucket.
    pub return_value: Decimal,
}

/// Bin lot outcomes onto **activity-only** equal-period observations.
///
/// Lots whose `as_of` share an epoch-aligned bucket of width `period_secs`
/// have their returns summed. Buckets with no lots are **omitted** (never
/// filled with `0`). When `period_secs == 0`, each lot is its own observation
/// (identity series, sorted by `as_of`).
///
/// Returns an empty `Vec` when `outcomes` is empty (caller must fail closed
/// upstream when a Sell CPCV path has no lots).
#[must_use]
pub fn calendarize_lot_returns(outcomes: &[LotOutcome], period_secs: u64) -> Vec<CalendarReturn> {
    if outcomes.is_empty() {
        return Vec::new();
    }
    if period_secs == 0 {
        let mut rows: Vec<CalendarReturn> = outcomes
            .iter()
            .map(|outcome| CalendarReturn {
                period_start: outcome.decision_at,
                return_value: outcome.return_value,
            })
            .collect();
        rows.sort_by_key(|row| row.period_start);
        return rows;
    }
    let period_i64 = i64::try_from(period_secs).unwrap_or(i64::MAX).max(1);
    // (bucket_epoch, return_sum) — BTreeMap keeps ascending order.
    let mut buckets: BTreeMap<i64, Decimal> = BTreeMap::new();
    for outcome in outcomes {
        let epoch = (outcome.decision_at.timestamp() / period_i64) * period_i64;
        *buckets.entry(epoch).or_insert(Decimal::ZERO) += outcome.return_value;
    }
    buckets
        .into_iter()
        .map(|(epoch, return_value)| CalendarReturn {
            period_start: DateTime::from_timestamp(epoch, 0).unwrap_or(outcomes[0].decision_at),
            return_value,
        })
        .collect()
}

/// Mean return of an activity-only series (null-baseline comparison helper).
#[must_use]
pub fn mean_calendar_return(series: &[CalendarReturn]) -> Decimal {
    if series.is_empty() {
        return Decimal::ZERO;
    }
    let sum: Decimal = series.iter().map(|row| row.return_value).sum();
    sum / Decimal::from(series.len() as u64)
}

/// Number of distinct activity buckets (effective DSR/PBO observation count).
#[must_use]
pub fn active_observation_count(outcomes: &[LotOutcome], period_secs: u64) -> usize {
    calendarize_lot_returns(outcomes, period_secs).len()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::types::PositionId;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{active_observation_count, calendarize_lot_returns};
    use crate::backtest::LotOutcome;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn outcome(as_of: DateTime<Utc>, return_value: Decimal) -> LotOutcome {
        LotOutcome {
            position_id: PositionId::from_v7(),
            decision_at: as_of,
            return_value,
            cumulative_exit_pct: Decimal::ONE,
            rank_pairs: Vec::new(),
            path_diverged: false,
        }
    }

    #[test]
    fn calendarize_sums_lots_in_same_active_bucket() {
        let series = calendarize_lot_returns(
            &[
                outcome(ts(0), dec!(0.01)),
                outcome(ts(30), dec!(0.02)),
                outcome(ts(3600), dec!(0.03)),
            ],
            3600,
        );
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].return_value, dec!(0.03));
        assert_eq!(series[1].return_value, dec!(0.03));
    }

    #[test]
    fn calendarize_omits_empty_buckets_never_zero_pads() {
        let series = calendarize_lot_returns(
            &[outcome(ts(0), dec!(0.01)), outcome(ts(7200), dec!(0.02))],
            3600,
        );
        // Wall-clock would invent a middle zero bucket; activity-only must not.
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].return_value, dec!(0.01));
        assert_eq!(series[1].return_value, dec!(0.02));
        assert_eq!(
            active_observation_count(
                &[outcome(ts(0), dec!(0.01)), outcome(ts(7200), dec!(0.02))],
                3600,
            ),
            2
        );
    }

    #[test]
    fn period_zero_is_identity_per_lot() {
        let series = calendarize_lot_returns(
            &[outcome(ts(10), dec!(0.01)), outcome(ts(5), dec!(0.02))],
            0,
        );
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].period_start, ts(5));
        assert_eq!(series[0].return_value, dec!(0.02));
    }
}
