//! Deflated / Probabilistic Sharpe Ratio + Minimum Track Record Length,
//! following Bailey and López de Prado.
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

use std::f64::consts::E;

use chrono::Duration;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{precision::RESEARCH_DECIMAL_SCALE, stats};

/// Euler–Mascheroni constant, used by the expected-maximum-Sharpe benchmark.
const EULER_MASCHERONI: f64 = 0.577_215_664_901_532_9;

/// Inputs to a Deflated Sharpe Ratio / `MinTRL` evaluation.
///
/// All pre-computed from a single representative return series (the CPCV
/// φ-path whose Sharpe is the distribution's median) plus
/// the governed trial grid's Sharpe distribution.
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
    /// — the multiple-testing correction's sample size.
    pub trial_count: u32,
    /// Variance of the Sharpe ratio across those `N` trials (`V`) — the
    /// multiple-testing correction's dispersion estimate. This is
    /// deliberately **not** the CPCV path-to-path Sharpe variance: data-split
    /// uncertainty and trial-selection uncertainty are different sources of
    /// dispersion.
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
fn expected_max_sharpe(trial_count: u32, trial_sharpe_variance: Decimal) -> QuantResult<f64> {
    if trial_count <= 1 {
        return Ok(0.0);
    }
    let n = f64::from(trial_count);
    // When every trial Sharpe is identical, V=0 would erase the multiple-testing
    // correction. Floor V at a tiny positive value so SR* still rises with N.
    let variance = decimal_to_f64(trial_sharpe_variance, "trial_sharpe_variance")?;
    if variance < 0.0 {
        return Err(methodology(
            "trial_sharpe_variance must be non-negative".to_owned(),
        ));
    }
    let variance = variance.max(1e-12);
    let term_a = (1.0 - EULER_MASCHERONI) * stats::normal_inverse_cdf(1.0 - 1.0 / n);
    let term_b = EULER_MASCHERONI * stats::normal_inverse_cdf(1.0 - 1.0 / (n * E));
    let benchmark = variance.sqrt() * (term_a + term_b);
    if !benchmark.is_finite() {
        return Err(methodology(
            "expected maximum Sharpe calculation produced a non-finite result".to_owned(),
        ));
    }
    Ok(benchmark)
}

/// The PSR denominator `sqrt(1 - γ3·SR_hat + (γ4-1)/4·SR_hat²)`, guarding the
/// degenerate case where extreme skew/kurtosis make the radicand
/// non-positive (the moment-based variance correction breaks down). Returns
/// `None` in that case — callers must fail closed (never divide by an
/// imaginary/zero denominator or silently substitute the normal-distribution
/// formula).
fn psr_denominator(
    skewness: Decimal,
    kurtosis: Decimal,
    observed_sharpe: Decimal,
) -> QuantResult<Option<f64>> {
    let gamma3 = decimal_to_f64(skewness, "skewness")?;
    // Degenerate series report kurtosis=0 from `stats::kurtosis`; treat that as
    // the normal-distribution default γ4=3 so DSR does not silently under-correct.
    let converted_kurtosis = decimal_to_f64(kurtosis, "kurtosis")?;
    let gamma4 = if converted_kurtosis > 0.0 {
        converted_kurtosis
    } else {
        3.0
    };
    let sr = decimal_to_f64(observed_sharpe, "observed_sharpe")?;
    let normal_term = gamma3.mul_add(-sr, 1.0);
    let radicand = ((gamma4 - 1.0) / 4.0 * sr).mul_add(sr, normal_term);
    if !radicand.is_finite() {
        return Err(methodology(
            "PSR denominator calculation produced a non-finite radicand".to_owned(),
        ));
    }
    Ok((radicand > 0.0).then(|| radicand.sqrt()))
}

impl DsrInput {
    /// Evaluate the Deflated Sharpe Ratio for `input`.
    ///
    /// Fails closed to `deflated_sharpe = 0` (never significant) when the
    /// moment-based variance correction is degenerate (see `psr_denominator`)
    /// or when fewer than two return periods are available (a Sharpe ratio is
    /// not estimable from a single observation).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] when a numeric input or
    /// derived statistic cannot be represented without a non-finite/saturated
    /// value, or when the trial Sharpe variance is negative.
    pub fn deflated_sharpe_ratio(&self) -> QuantResult<DsrReport> {
        let benchmark = expected_max_sharpe(self.trial_count, self.trial_sharpe_variance)?;
        let benchmark_sharpe =
            decimal_from_f64(benchmark, "benchmark_sharpe")?.round_dp(RESEARCH_DECIMAL_SCALE);

        if self.returns_period_count < 2 {
            return Ok(DsrReport {
                observed_sharpe: self.observed_sharpe,
                benchmark_sharpe,
                deflated_sharpe: Decimal::ZERO,
            });
        }
        let Some(denominator) =
            psr_denominator(self.skewness, self.kurtosis, self.observed_sharpe)?
        else {
            return Ok(DsrReport {
                observed_sharpe: self.observed_sharpe,
                benchmark_sharpe,
                deflated_sharpe: Decimal::ZERO,
            });
        };
        let sr_hat = decimal_to_f64(self.observed_sharpe, "observed_sharpe")?;
        let t_minus_one = exact_u64_to_f64(
            self.returns_period_count.checked_sub(1).ok_or_else(|| {
                methodology("returns_period_count underflowed while computing DSR".to_owned())
            })?,
            "returns_period_count - 1",
        )?;
        let z = (sr_hat - benchmark) * t_minus_one.sqrt() / denominator;
        if !z.is_finite() {
            return Err(methodology(
                "deflated Sharpe z-score is non-finite".to_owned(),
            ));
        }
        let deflated = decimal_from_f64(stats::normal_cdf(z), "deflated_sharpe")?
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE);

        Ok(DsrReport {
            observed_sharpe: self.observed_sharpe,
            benchmark_sharpe,
            deflated_sharpe: deflated,
        })
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
///
/// # Errors
///
/// Returns [`ResearchError::ValidationMethodology`] when significance is
/// outside `(0, 0.5]`, a conversion is non-finite/out of range, or the final
/// duration exceeds `chrono::Duration`'s supported range.
pub fn min_track_record_length(
    input: &DsrInput,
    target_significance: Decimal,
) -> QuantResult<Option<Duration>> {
    if input.observed_sharpe <= Decimal::ZERO {
        return Ok(None);
    }
    if psr_denominator(input.skewness, input.kurtosis, input.observed_sharpe)?.is_none() {
        return Ok(None);
    }
    let gamma3 = decimal_to_f64(input.skewness, "skewness")?;
    let converted_kurtosis = decimal_to_f64(input.kurtosis, "kurtosis")?;
    let gamma4 = if converted_kurtosis > 0.0 {
        converted_kurtosis
    } else {
        3.0
    };
    let sr = decimal_to_f64(input.observed_sharpe, "observed_sharpe")?;
    let alpha = decimal_to_f64(target_significance, "target_significance")?;
    if !(0.0..=0.5).contains(&alpha) || alpha == 0.0 {
        return Err(methodology(format!(
            "target_significance must be in (0, 0.5], got {target_significance}"
        )));
    }
    let z = stats::normal_inverse_cdf(1.0 - alpha);
    let normal_term = gamma3.mul_add(-sr, 1.0);
    let moment_term = ((gamma4 - 1.0) / 4.0 * sr).mul_add(sr, normal_term);
    if !moment_term.is_finite() {
        return Err(methodology(
            "minimum track-record calculation produced a non-finite moment term".to_owned(),
        ));
    }
    if moment_term <= 0.0 {
        return Ok(None);
    }
    let periods = 1.0 + moment_term * z * z / (sr * sr);
    if !periods.is_finite() || periods < 1.0 {
        return Err(methodology(
            "minimum track-record period count is non-finite or below one".to_owned(),
        ));
    }
    let period_count = periods.ceil().to_i32().ok_or_else(|| {
        methodology(format!(
            "minimum track-record period count {periods} exceeds the supported i32 range"
        ))
    })?;
    input
        .period_length
        .checked_mul(period_count)
        .map(Some)
        .ok_or_else(|| {
            methodology(format!(
                "minimum track-record duration overflows for {period_count} periods"
            ))
        })
}

fn decimal_to_f64(value: Decimal, field: &'static str) -> QuantResult<f64> {
    value
        .to_f64()
        .filter(|converted| converted.is_finite())
        .ok_or_else(|| {
            methodology(format!(
                "{field}={value} cannot be represented as finite f64"
            ))
        })
}

fn decimal_from_f64(value: f64, field: &'static str) -> QuantResult<Decimal> {
    if !value.is_finite() {
        return Err(methodology(format!("{field} is non-finite")));
    }
    Decimal::from_f64(value)
        .ok_or_else(|| methodology(format!("{field}={value} cannot be represented as Decimal")))
}

fn exact_u64_to_f64(value: u64, field: &'static str) -> QuantResult<f64> {
    const MAX_EXACT_F64_INTEGER: u64 = 1_u64 << f64::MANTISSA_DIGITS;
    if value > MAX_EXACT_F64_INTEGER {
        return Err(methodology(format!(
            "{field}={value} exceeds the exact integer range of f64"
        )));
    }
    value
        .to_f64()
        .ok_or_else(|| methodology(format!("{field}={value} cannot be represented as f64")))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{DsrInput, min_track_record_length};

    fn normal_input(observed_sharpe: Decimal, trial_count: u32) -> DsrInput {
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
    fn deflated_sharpe_below_trials() {
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
        let one_trial = short_track_record(1).deflated_sharpe_ratio().expect("DSR");
        let many_trials = short_track_record(50).deflated_sharpe_ratio().expect("DSR");
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
    fn deflated_sharpe_increases_further() {
        let fifty = normal_input(dec!(2.0), 50)
            .deflated_sharpe_ratio()
            .expect("DSR");
        let five_hundred = normal_input(dec!(2.0), 500)
            .deflated_sharpe_ratio()
            .expect("DSR");
        assert!(five_hundred.benchmark_sharpe > fifty.benchmark_sharpe);
        assert!(five_hundred.deflated_sharpe <= fifty.deflated_sharpe);
    }

    #[test]
    fn deflated_sharpe_bounded_probability() {
        let report = normal_input(dec!(3.0), 1)
            .deflated_sharpe_ratio()
            .expect("DSR");
        assert!(report.deflated_sharpe >= rust_decimal::Decimal::ZERO);
        assert!(report.deflated_sharpe <= rust_decimal::Decimal::ONE);
    }

    #[test]
    fn non_positive_no_length() {
        assert!(
            min_track_record_length(&normal_input(dec!(-0.5), 1), dec!(0.05))
                .expect("MinTRL")
                .is_none()
        );
        assert!(
            min_track_record_length(&normal_input(dec!(0), 1), dec!(0.05))
                .expect("MinTRL")
                .is_none()
        );
    }

    #[test]
    fn higher_sharpe_needs_length() {
        let low = min_track_record_length(&normal_input(dec!(0.5), 1), dec!(0.05))
            .expect("MinTRL")
            .expect("some");
        let high = min_track_record_length(&normal_input(dec!(2.0), 1), dec!(0.05))
            .expect("MinTRL")
            .expect("some");
        assert!(
            high < low,
            "a stronger Sharpe needs fewer periods to prove significant"
        );
    }

    #[test]
    fn negative_trial_rejected_floored() {
        let input = DsrInput {
            trial_sharpe_variance: dec!(-0.01),
            ..normal_input(dec!(1), 10)
        };
        assert!(input.deflated_sharpe_ratio().is_err());
    }

    #[test]
    fn period_count_beyond_rejected() {
        let input = DsrInput {
            returns_period_count: (1_u64 << f64::MANTISSA_DIGITS) + 2,
            ..normal_input(dec!(1), 1)
        };
        assert!(input.deflated_sharpe_ratio().is_err());
    }

    #[test]
    fn invalid_significance_rejected_clamped() {
        assert!(min_track_record_length(&normal_input(dec!(1), 1), dec!(0)).is_err());
        assert!(min_track_record_length(&normal_input(dec!(1), 1), dec!(0.6)).is_err());
    }
}
