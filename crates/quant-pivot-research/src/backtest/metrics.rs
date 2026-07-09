//! Pure backtest metric aggregations over resolved [`SampleOutcome`]s.

use std::collections::BTreeMap;

use quant_pivot_models::{enums::common::MarketCategory, types::Probability};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    backtest::{CategoryMetric, ExpectedVsRealized, PnlCurvePoint, SampleOutcome},
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
};

/// Clamp a decimal into `[0, 1]` and wrap as a [`Probability`].
fn probability(value: Decimal) -> Probability {
    Probability::new(value.clamp(Decimal::ZERO, Decimal::ONE))
}

/// Spearman rank IC between composite score and realized return.
#[must_use]
pub fn rank_ic(samples: &[SampleOutcome]) -> Decimal {
    let scores: Vec<Decimal> = samples.iter().map(|s| s.composite_score.inner()).collect();
    let realized: Vec<Decimal> = samples.iter().map(|s| s.realized_return_bps).collect();
    stats::spearman(&scores, &realized).round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Fraction of samples with a strictly positive realized return.
#[must_use]
pub fn hit_rate(samples: &[SampleOutcome]) -> Probability {
    if samples.is_empty() {
        return Probability::ZERO;
    }
    let wins = samples
        .iter()
        .filter(|s| s.realized_return_bps > Decimal::ZERO)
        .count();
    probability(Decimal::from(wins as u64) / Decimal::from(samples.len() as u64))
}

/// Expected-vs-realized agreement summary.
#[must_use]
pub fn expected_vs_realized(samples: &[SampleOutcome]) -> ExpectedVsRealized {
    let expected: Vec<Decimal> = samples.iter().map(|s| s.expected_return_bps).collect();
    let realized: Vec<Decimal> = samples.iter().map(|s| s.realized_return_bps).collect();
    let mean_expected = stats::mean(&expected);
    let mean_realized = stats::mean(&realized);
    ExpectedVsRealized {
        mean_expected_bps: mean_expected.round_dp(RESEARCH_DECIMAL_SCALE),
        mean_realized_bps: mean_realized.round_dp(RESEARCH_DECIMAL_SCALE),
        correlation: stats::pearson(&expected, &realized).round_dp(RESEARCH_DECIMAL_SCALE),
        bias_bps: (mean_expected - mean_realized).round_dp(RESEARCH_DECIMAL_SCALE),
    }
}

/// Per-category breakdown, ordered by category wire name.
#[must_use]
pub fn category_breakdown(samples: &[SampleOutcome]) -> Vec<CategoryMetric> {
    let mut by_category: BTreeMap<MarketCategory, Vec<SampleOutcome>> = BTreeMap::new();
    for sample in samples {
        by_category
            .entry(sample.category)
            .or_default()
            .push(sample.clone());
    }
    by_category
        .into_iter()
        .map(|(category, group)| {
            let realized: Vec<Decimal> = group.iter().map(|s| s.realized_return_bps).collect();
            CategoryMetric {
                category,
                sample_count: group.len() as u64,
                rank_ic: rank_ic(&group),
                hit_rate: hit_rate(&group),
                mean_realized_bps: stats::mean(&realized).round_dp(RESEARCH_DECIMAL_SCALE),
            }
        })
        .collect()
}

/// Conditional mean of the worst-decile realized returns (tail loss, bps).
///
/// `quantile` is the lower tail fraction (e.g. `0.10`); the result is the mean
/// realized return of the worst `ceil(n · quantile)` samples (≤ 0 for losses).
#[must_use]
pub fn tail_loss(samples: &[SampleOutcome], quantile: Decimal) -> Decimal {
    if samples.is_empty() {
        return Decimal::ZERO;
    }
    let mut realized: Vec<Decimal> = samples.iter().map(|s| s.realized_return_bps).collect();
    realized.sort();
    let n = realized.len();
    let q = quantile.clamp(Decimal::ZERO, Decimal::ONE);
    // ceil(n * q), at least one sample.
    let raw = (Decimal::from(n as u64) * q).ceil();
    let take = raw.to_usize().unwrap_or(1).max(1).min(n);
    let tail = &realized[..take];
    stats::mean(tail).round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Maximum cumulative-`PnL` drawdown as a fraction of `budget` (or of the peak
/// `PnL` when the budget is non-positive). Returns a non-negative ratio.
#[must_use]
pub fn max_drawdown(pnl_curve: &[PnlCurvePoint], budget: Decimal) -> Decimal {
    let mut peak = Decimal::ZERO;
    let mut max_dd = Decimal::ZERO;
    for point in pnl_curve {
        peak = peak.max(point.cumulative_realized_pnl_usd);
        let dd = peak - point.cumulative_realized_pnl_usd;
        max_dd = max_dd.max(dd);
    }
    let denom = if budget > Decimal::ZERO {
        budget
    } else if peak > Decimal::ZERO {
        peak
    } else {
        return Decimal::ZERO;
    };
    (max_dd / denom)
        .clamp(Decimal::ZERO, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Mean per-tick portfolio turnover: the average L1 change in per-market
/// allocation weights between consecutive ticks, halved (one-sided turnover).
#[must_use]
pub fn turnover(tick_weights: &[BTreeMap<String, Decimal>]) -> Decimal {
    if tick_weights.len() < 2 {
        return Decimal::ZERO;
    }
    let mut total = Decimal::ZERO;
    for pair in tick_weights.windows(2) {
        let [prev, curr] = pair else { continue };
        let mut keys: Vec<&String> = prev.keys().chain(curr.keys()).collect();
        keys.sort();
        keys.dedup();
        let mut l1 = Decimal::ZERO;
        for key in keys {
            let a = prev.get(key).copied().unwrap_or(Decimal::ZERO);
            let b = curr.get(key).copied().unwrap_or(Decimal::ZERO);
            l1 += (a - b).abs();
        }
        total += l1 / Decimal::from(2);
    }
    (total / Decimal::from((tick_weights.len() - 1) as u64)).round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Sharpe ratio of a per-period return series: `mean/stddev · sqrt(periods_per_year)`.
///
/// `0` for fewer than two periods or a zero-variance series (never a
/// divide-by-zero panic or an unbounded value). Pass `periods_per_year = 1`
/// for an unannualized (raw per-period) Sharpe — callers comparing across
/// different sampling cadences must annualize.
#[must_use]
pub fn sharpe_ratio(returns: &[Decimal], periods_per_year: Decimal) -> Decimal {
    if returns.len() < 2 {
        return Decimal::ZERO;
    }
    let mean = stats::mean(returns);
    let sigma = stats::stddev(returns);
    if sigma.is_zero() {
        return Decimal::ZERO;
    }
    (mean / sigma * stats::sqrt(periods_per_year.max(Decimal::ZERO)))
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Fraction of samples whose allocation respected the liquidity-usage cap.
#[must_use]
pub fn liquidity_feasibility(samples: &[SampleOutcome]) -> Probability {
    if samples.is_empty() {
        return Probability::ONE;
    }
    let feasible = samples.iter().filter(|s| s.liquidity_feasible).count();
    probability(Decimal::from(feasible as u64) / Decimal::from(samples.len() as u64))
}
