//! Deflated / Probabilistic Sharpe Ratio + Minimum Track Record Length
//! (Phase 11.5 §4, Bailey & López de Prado).
//!
//! A raw Sharpe ratio estimated from one backtest overstates significance
//! whenever (a) the underlying return series is non-normal (skewed / fat
//! tailed) and (b) the strategy was selected from `N` independent trials —
//! the researcher implicitly "kept" whichever trial happened to look best.
//! Both corrections compose into the **Deflated Sharpe Ratio**:
//!
//! - **PSR** (Probabilistic Sharpe Ratio, Bailey & López de Prado 2012, *The
//!   Sharpe Ratio Efficient Frontier*): the probability that the *true*
//!   Sharpe ratio exceeds a benchmark `SR*`, correcting for the estimator's
//!   own skew/kurtosis-driven variance.
//! - **DSR** (Deflated Sharpe Ratio, Bailey & López de Prado 2014, *The
//!   Deflated Sharpe Ratio*): `PSR` evaluated at `SR* =` the analytically
//!   expected maximum Sharpe ratio achievable by chance across `N`
//!   independent trials with Sharpe variance `V` — i.e. "is the observed
//!   Sharpe still significant after accounting for how many configurations
//!   were tried before this one was reported?"
//! - **`MinTRL`** (Minimum Track Record Length, same 2012 paper): the fewest
//!   return observations needed for the observed Sharpe to be significant at
//!   a target confidence, given the same skew/kurtosis correction.

use chrono::Duration;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{precision::RESEARCH_DECIMAL_SCALE, stats};

/// Euler–Mascheroni constant, used by the expected-maximum-Sharpe benchmark.
const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;

/// Inputs to a Deflated Sharpe Ratio / `MinTRL` evaluation.
///
/// All pre-computed from a single representative return series (Phase 11.5
/// §3.4 uses the CPCV φ-path whose Sharpe is the distribution's median) plus
/// the governed trial grid's Sharpe distribution (Phase 11.5 §3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DsrInput {
    /// The observed (estimated) Sharpe ratio `SR_hat` of the representative path.
    pub observed_sharpe: Decimal,
    /// Number of return observations (`T`) the representative path's Sharpe was
    /// estimated over.
    pub returns_period_count: u64,
    /// The wall-clock length of one return period (for converting `MinTRL`'s
    /// period count back into a duration).
    pub period_length: Duration,
    /// Population skewness (`γ3`) of the representative path's per-period returns.
    pub skewness: Decimal,
    /// Population (non-excess) kurtosis (`γ4`) of the representative path's
    /// per-period returns (a normal distribution has `γ4 = 3`).
    pub kurtosis: Decimal,
    /// Number of independent trials (`N`) in the governed hyperparameter grid
    /// (Phase 11.5 §3.5) — the multiple-testing correction's sample size.
    pub trial_count: u32,
    /// Variance of the Sharpe ratio across those `N` trials (`V`) — the
    /// multiple-testing correction's dispersion estimate. This is
    /// deliberately **not** the CPCV path-to-path Sharpe variance (that is a
    /// data-split uncertainty, a different source of dispersion than
    /// trial-selection uncertainty; see Phase 11.5 plan §3.4).
    pub trial_sharpe_variance: Decimal,
}

/// The result of a Deflated Sharpe Ratio evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DsrReport {
    /// The observed Sharpe ratio (`SR_hat`, echoed from the input).
    pub observed_sharpe: Decimal,
    /// The expected-maximum-Sharpe benchmark (`SR*`) the observed Sharpe is
    /// deflated against.
    pub benchmark_sharpe: Decimal,
    /// `PSR(SR*)` — the probability, in `[0, 1]`, that the strategy's true
    /// Sharpe ratio exceeds `SR*` once selection bias and non-normality are
    /// both corrected for. This **is** the Deflated Sharpe Ratio.
    pub deflated_sharpe: Decimal,
}

/// The analytically expected maximum Sharpe ratio across `N` independent
/// trials with per-trial Sharpe variance `V` (Bailey & López de Prado 2014
/// eq. 8): `SR* ≈ sqrt(V) · [(1-γ)·Φ⁻¹(1-1/N) + γ·Φ⁻¹(1-1/(N·e))]`. A single
/// trial needs no multiple-testing correction (`SR* = 0`).
fn expected_max_sharpe(trial_count: u32, trial_sharpe_variance: Decimal) -> f64 {
    if trial_count <= 1 || trial_sharpe_variance <= Decimal::ZERO {
        return 0.0;
    }
    let n = f64::from(trial_count);
    let v = trial_sharpe_variance.to_f64().unwrap_or(0.0).max(0.0);
    let term_a = (1.0 - EULER_MASCHERONI) * stats::normal_inverse_cdf(1.0 - 1.0 / n);
    let term_b =
        EULER_MASCHERONI * stats::normal_inverse_cdf(1.0 - 1.0 / (n * std::f64::consts::E));
    v.sqrt() * (term_a + term_b)
}

/// The PSR denominator `sqrt(1 - γ3·SR_hat + (γ4-1)/4·SR_hat²)`, guarding the
/// degenerate case where extreme skew/kurtosis make the radicand
/// non-positive (the moment-based variance correction breaks down). Returns
/// `None` in that case — callers must fail closed (never divide by an
/// imaginary/zero denominator or silently substitute the normal-distribution
/// formula).
fn psr_denominator(skewness: Decimal, kurtosis: Decimal, observed_sharpe: Decimal) -> Option<f64> {
    let gamma3 = skewness.to_f64().unwrap_or(0.0);
    let gamma4 = kurtosis.to_f64().unwrap_or(3.0);
    let sr = observed_sharpe.to_f64().unwrap_or(0.0);
    let radicand = ((gamma4 - 1.0) / 4.0 * sr).mul_add(sr, 1.0 - gamma3 * sr);
    if radicand <= 0.0 {
        return None;
    }
    Some(radicand.sqrt())
}

/// Evaluate the Deflated Sharpe Ratio for `input`.
///
/// Fails closed to `deflated_sharpe = 0` (never significant) when the
/// moment-based variance correction is degenerate (see [`psr_denominator`])
/// or when fewer than two return periods are available (a Sharpe ratio is
/// not estimable from a single observation).
#[must_use]
pub fn deflated_sharpe_ratio(input: &DsrInput) -> DsrReport {
    let benchmark = expected_max_sharpe(input.trial_count, input.trial_sharpe_variance);
    let benchmark_sharpe = Decimal::from_f64(benchmark)
        .unwrap_or(Decimal::ZERO)
        .round_dp(RESEARCH_DECIMAL_SCALE);

    if input.returns_period_count < 2 {
        return DsrReport {
            observed_sharpe: input.observed_sharpe,
            benchmark_sharpe,
            deflated_sharpe: Decimal::ZERO,
        };
    }
    let Some(denominator) = psr_denominator(input.skewness, input.kurtosis, input.observed_sharpe)
    else {
        return DsrReport {
            observed_sharpe: input.observed_sharpe,
            benchmark_sharpe,
            deflated_sharpe: Decimal::ZERO,
        };
    };
    let sr_hat = input.observed_sharpe.to_f64().unwrap_or(0.0);
    let t_minus_one =
        f64::from(u32::try_from(input.returns_period_count.saturating_sub(1)).unwrap_or(u32::MAX));
    let z = (sr_hat - benchmark) * t_minus_one.sqrt() / denominator;
    let deflated = Decimal::from_f64(stats::normal_cdf(z))
        .unwrap_or(Decimal::ZERO)
        .clamp(Decimal::ZERO, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE);

    DsrReport {
        observed_sharpe: input.observed_sharpe,
        benchmark_sharpe,
        deflated_sharpe: deflated,
    }
}

/// The minimum number of return periods needed for `input.observed_sharpe` to
/// be significant at `target_significance` (e.g. `0.05` for 95% confidence).
///
/// Against a zero benchmark, per Bailey & López de Prado (2012) eq. 10:
/// `MinTRL = 1 + [(1 - γ3·SR_hat + (γ4-1)/4·SR_hat²) · Z_{1-α}²] / SR_hat²`.
///
/// Returns `None` when `observed_sharpe` is non-positive (no finite track
/// record makes a non-positive Sharpe ratio significant against zero) or the
/// moment correction is degenerate — an informational/soft signal, never a
/// hard failure.
#[must_use]
pub fn min_track_record_length(input: &DsrInput, target_significance: Decimal) -> Option<Duration> {
    if input.observed_sharpe <= Decimal::ZERO {
        return None;
    }
    let _denominator = psr_denominator(input.skewness, input.kurtosis, input.observed_sharpe)?;
    let gamma3 = input.skewness.to_f64().unwrap_or(0.0);
    let gamma4 = input.kurtosis.to_f64().unwrap_or(3.0);
    let sr = input.observed_sharpe.to_f64().unwrap_or(0.0);
    let alpha = target_significance
        .to_f64()
        .unwrap_or(0.05)
        .clamp(1e-6, 0.5);
    let z = stats::normal_inverse_cdf(1.0 - alpha);
    let moment_term = ((gamma4 - 1.0) / 4.0 * sr).mul_add(sr, 1.0 - gamma3 * sr);
    if moment_term <= 0.0 {
        return None;
    }
    let periods = 1.0 + moment_term * z * z / (sr * sr);
    let periods = periods.ceil().max(1.0);
    let period_count = i64::from_f64(periods).unwrap_or(i64::MAX);
    Some(input.period_length * i32::try_from(period_count).unwrap_or(i32::MAX))
}

#[cfg(test)]
mod tests {
    use super::{DsrInput, deflated_sharpe_ratio, min_track_record_length};
    use chrono::Duration;
    use rust_decimal_macros::dec;

    fn normal_input(observed_sharpe: rust_decimal::Decimal, trial_count: u32) -> DsrInput {
        DsrInput {
            observed_sharpe,
            returns_period_count: 252,
            period_length: Duration::days(1),
            skewness: dec!(0),
            kurtosis: dec!(3),
            trial_count,
            trial_sharpe_variance: dec!(0.04),
        }
    }

    #[test]
    fn deflated_sharpe_below_naive_sharpe_under_multiple_trials() {
        // A short, modest-Sharpe track record (20 periods, SR_hat=0.3) keeps
        // both z-scores away from the CDF's saturated tail, so the
        // multiple-testing correction's effect is actually observable (a
        // 252-period, SR_hat=1.5 series is so overwhelmingly significant
        // that both the 1-trial and 50-trial benchmarks round to a deflated
        // Sharpe of exactly 1.0, masking the comparison).
        let short_track_record = |trial_count: u32| DsrInput {
            observed_sharpe: dec!(0.3),
            returns_period_count: 20,
            trial_count,
            ..normal_input(dec!(0.3), trial_count)
        };
        let one_trial = deflated_sharpe_ratio(&short_track_record(1));
        let many_trials = deflated_sharpe_ratio(&short_track_record(50));
        assert_eq!(one_trial.benchmark_sharpe, rust_decimal::Decimal::ZERO);
        assert!(
            many_trials.benchmark_sharpe > one_trial.benchmark_sharpe,
            "more trials must raise the by-chance benchmark"
        );
        assert!(
            many_trials.deflated_sharpe < one_trial.deflated_sharpe,
            "the same observed Sharpe must be judged less significant under a wider search: \
             one_trial={one_trial:?} many_trials={many_trials:?}"
        );
    }

    #[test]
    fn deflated_sharpe_increases_with_trial_count_pushed_further() {
        let fifty = deflated_sharpe_ratio(&normal_input(dec!(2.0), 50));
        let five_hundred = deflated_sharpe_ratio(&normal_input(dec!(2.0), 500));
        assert!(five_hundred.benchmark_sharpe > fifty.benchmark_sharpe);
        assert!(five_hundred.deflated_sharpe <= fifty.deflated_sharpe);
    }

    #[test]
    fn deflated_sharpe_is_bounded_probability() {
        let report = deflated_sharpe_ratio(&normal_input(dec!(3.0), 1));
        assert!(report.deflated_sharpe >= rust_decimal::Decimal::ZERO);
        assert!(report.deflated_sharpe <= rust_decimal::Decimal::ONE);
    }

    #[test]
    fn non_positive_sharpe_has_no_min_track_record_length() {
        assert!(min_track_record_length(&normal_input(dec!(-0.5), 1), dec!(0.05)).is_none());
        assert!(min_track_record_length(&normal_input(dec!(0), 1), dec!(0.05)).is_none());
    }

    #[test]
    fn higher_sharpe_needs_shorter_min_track_record_length() {
        let low = min_track_record_length(&normal_input(dec!(0.5), 1), dec!(0.05)).expect("some");
        let high = min_track_record_length(&normal_input(dec!(2.0), 1), dec!(0.05)).expect("some");
        assert!(
            high < low,
            "a stronger Sharpe needs fewer periods to prove significant"
        );
    }
}
