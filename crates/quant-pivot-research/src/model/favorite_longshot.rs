//! Favorite-longshot bias table (Phase 11.2.1 — an 11.3 calibration advance).
//!
//! A content-addressed calibration artifact mapping a market's implied price
//! (mid) to the **empirical** settlement-as-YES frequency, conditioned on
//! `(category, time_to_resolution_bucket, price_bucket)`. It is the direct
//! measurement of the favorite-longshot bias (low-probability tokens
//! systematically over-priced, high-probability tokens under-priced) — and is
//! consumed by the `struct.favorite_longshot` factor.
//!
//! The favorite-longshot bias is **conditioned on time-to-resolution** as well
//! as category: the empirical mispricing at a given implied price differs across
//! a market's life (a 0.10 token a week out is not the same bet as a 0.10 token
//! an hour out), so a single-lead measurement mis-states the correction served
//! at an arbitrary decision time. The fit therefore samples the entry mid across
//! the market lifecycle (see the core fit orchestration) and buckets by residual
//! time to resolution.
//!
//! Fail-closed on thin / insignificant data (greenfield):
//! - a price bin below `min_bin_samples`, or whose empirical frequency's Wilson
//!   interval **contains** the implied mid (bias not statistically distinct from
//!   the price), carries no bias;
//! - a `(category, ttr_bucket)` curve below `min_curve_samples` or with no
//!   retained bin is dropped;
//! - a `(category, ttr_bucket)` curve whose bias-vs-residual IC is not
//!   significant (Student-t on the correlation) is marked `ic_significant=false`
//!   and gated off at read time;
//! - a fit with no qualifying category produces **no artifact at all** (never an
//!   empty table masquerading as a fit).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{CalibrationArtifactInfo, NewCalibrationArtifact, query::TimeWindow},
    enums::{common::MarketCategory, quant::CalibrationKind},
    types::{CalibrationArtifactId, ContentHash, MarketId, Price, Probability},
};
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use statrs::distribution::{ContinuousCDF, StudentsT};

use crate::{
    hashing::ResearchHasher,
    stats::{count_f64, wilson_interval, wilson_z},
};

/// Decimal scale for every `f64`-derived statistic entering the artifact (Wilson
/// CI, IC), matching the crate-wide `STAT_SCALE` so hashes are platform-stable.
const STAT_SCALE: u32 = 12;

/// One `(price_bucket)` empirical-bias record within a `(category, ttr_bucket)` curve.
///
/// A retained bin has enough samples **and** a bias whose Wilson interval excludes
/// the implied mid (statistically distinct mispricing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceBiasBin {
    /// Inclusive lower price edge of the bin.
    pub price_lo: Price,
    /// Exclusive upper price edge (inclusive only for the top bin, `price_hi==1`).
    pub price_hi: Price,
    /// Bin mid (the representative implied probability).
    pub implied_mid: Price,
    /// Empirical settle-as-YES frequency across the bin's samples.
    pub realized_frequency: Probability,
    /// Signed bias `realized_frequency − implied_mid` (the correction).
    pub bias: Decimal,
    /// Wilson score interval `(lo, hi)` for `realized_frequency`.
    pub bias_ci: (Decimal, Decimal),
    /// Number of samples in the bin.
    pub sample_count: u64,
}

impl PriceBiasBin {
    /// Whether `mid` falls in this bin under the shared half-open convention
    /// `[lo, hi)`, with the top bin (`price_hi == 1`) closed on the right.
    #[must_use]
    fn contains(&self, mid: Decimal) -> bool {
        let lo = self.price_lo.inner();
        let hi = self.price_hi.inner();
        mid >= lo && (mid < hi || (hi == Decimal::ONE && mid <= hi))
    }
}

/// A per-`(category, ttr_bucket)` empirical-bias curve over price buckets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtrBucketCurve {
    /// Inclusive lower time-to-resolution edge, in seconds.
    pub ttr_lo_secs: u64,
    /// Exclusive upper time-to-resolution edge, in seconds (`None` = unbounded
    /// top bucket).
    pub ttr_hi_secs: Option<u64>,
    /// Price-ordered bins (only bins clearing `min_bin_samples` and the Wilson
    /// significance gate are retained).
    pub bins: Vec<PriceBiasBin>,
    /// In-sample information coefficient of the bias signal for this curve.
    pub ic: Decimal,
    /// Whether the curve's IC cleared the significance gate (Student-t).
    pub ic_significant: bool,
    /// Total samples used to fit the curve.
    pub sample_count: u64,
}

impl TtrBucketCurve {
    /// Whether `ttr_secs` falls in this bucket under `[lo, hi)` (top unbounded).
    #[must_use]
    fn contains_ttr(&self, ttr_secs: u64) -> bool {
        ttr_secs >= self.ttr_lo_secs && self.ttr_hi_secs.is_none_or(|hi| ttr_secs < hi)
    }

    /// The bias for a mid price, or `None` when no retained bin contains it.
    #[must_use]
    fn bias_for(&self, mid: Price) -> Option<Decimal> {
        let mid = mid.inner();
        self.bins
            .iter()
            .find(|bin| bin.contains(mid))
            .map(|bin| bin.bias)
    }
}

/// A per-category empirical-bias curve, conditioned on time to resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryBiasCurve {
    /// Time-to-resolution-ordered bucket curves (ascending `ttr_lo_secs`).
    pub by_ttr: Vec<TtrBucketCurve>,
    /// Total samples across all retained ttr buckets.
    pub sample_count: u64,
}

impl CategoryBiasCurve {
    /// The bias for `(ttr_secs, mid)`, honoring the IC gate.
    #[must_use]
    fn bias_for(&self, ttr_secs: u64, mid: Price, ic_gate: bool) -> Option<Decimal> {
        let curve = self.by_ttr.iter().find(|c| c.contains_ttr(ttr_secs))?;
        if ic_gate && !curve.ic_significant {
            return None;
        }
        curve.bias_for(mid)
    }
}

/// A content-addressed favorite-longshot bias table — the `MarketPriceBias`
/// payload of the unified `CalibrationArtifact` family (Phase 11.3 §3.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FavoriteLongshotBiasTable {
    /// Surrogate artifact id (shared `CalibrationArtifactId` space).
    pub table_id: CalibrationArtifactId,
    /// `blake3:` canonical content hash (excludes the surrogate id).
    pub content_hash: ContentHash,
    /// The sample window the table was fit over.
    pub fit_window: TimeWindow,
    /// Hash of the independent calibration split (leakage guard; 11.5-aligned).
    pub calibration_split_hash: ContentHash,
    /// Per-category empirical-bias curves (each conditioned on ttr).
    pub by_category: BTreeMap<MarketCategory, CategoryBiasCurve>,
}

impl FavoriteLongshotBiasTable {
    /// The signed bias correction for `(category, ttr_secs, mid)`, honoring the
    /// IC gate.
    ///
    /// Returns `None` — the factor then stays inert (never a fabricated value) —
    /// when the category is absent, no ttr bucket contains `ttr_secs`, the
    /// bucket curve is IC-gated off, or no retained price bin contains `mid`.
    #[must_use]
    pub fn bias_for(
        &self,
        category: MarketCategory,
        ttr_secs: u64,
        mid: Price,
        ic_gate: bool,
    ) -> Option<Decimal> {
        self.by_category
            .get(&category)?
            .bias_for(ttr_secs, mid, ic_gate)
    }

    /// Rehydrate a persisted `MarketPriceBias` artifact into the compute-domain
    /// table, verifying the recomputed canonical content hash matches the
    /// stored hash.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::DatasetBuild`](quant_pivot_error::research::ResearchError::DatasetBuild)
    /// when `info.kind` is not [`CalibrationKind::MarketPriceBias`], the
    /// payload cannot be deserialized, or the recomputed hash does not match
    /// the persisted `content_hash` (fail-closed: a tampered / corrupt / wrong-
    /// kind artifact never binds to the factor plane).
    pub fn from_persisted(info: &CalibrationArtifactInfo) -> QuantResult<Self> {
        use quant_pivot_error::{QuantError, research::ResearchError};

        if info.kind != CalibrationKind::MarketPriceBias {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "calibration artifact `{}` is kind `{}`, expected `market_price_bias`",
                    info.artifact_id,
                    info.kind.as_str()
                ),
            }));
        }
        let by_category: BTreeMap<MarketCategory, CategoryBiasCurve> =
            serde_json::from_value(info.payload_json.clone()).map_err(|error| {
                QuantError::from(ResearchError::DatasetBuild {
                    detail: format!("bias-table payload deserialization failed: {error}"),
                })
            })?;
        let fit_window = TimeWindow::new(info.fit_window_start, info.fit_window_end);
        let recomputed = ResearchHasher::canonical(&BiasTableCanonical {
            fit_window: &fit_window,
            calibration_split_hash: &info.calibration_split_hash,
            by_category: &by_category,
        })?;
        if recomputed != info.content_hash {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "bias-table content hash mismatch: stored {} recomputed {}",
                    info.content_hash, recomputed
                ),
            }));
        }
        Ok(Self {
            table_id: info.artifact_id.clone(),
            content_hash: info.content_hash.clone(),
            fit_window,
            calibration_split_hash: info.calibration_split_hash.clone(),
            by_category,
        })
    }
}

impl From<FavoriteLongshotBiasTable> for NewCalibrationArtifact {
    fn from(table: FavoriteLongshotBiasTable) -> Self {
        Self {
            artifact_id: table.table_id,
            kind: CalibrationKind::MarketPriceBias,
            content_hash: table.content_hash,
            calibration_split_hash: table.calibration_split_hash,
            fit_window_start: table.fit_window.from,
            fit_window_end: table.fit_window.to,
            sample_count: i64::try_from(
                table
                    .by_category
                    .values()
                    .map(|c| c.sample_count)
                    .sum::<u64>(),
            )
            .unwrap_or(i64::MAX),
            payload_json: serde_json::to_value(&table.by_category).unwrap_or_default(),
            active: false,
        }
    }
}

/// One fit observation: an entry mid, its residual time to resolution, and the
/// realized settlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BiasSample {
    /// The market the sample belongs to (provenance / split-hash key).
    pub market_id: MarketId,
    /// Decision instant the entry mid was resolved at (split-hash key).
    pub sampled_at: DateTime<Utc>,
    /// The market's category.
    pub category: MarketCategory,
    /// PIT entry mid (the market-implied probability at the sample instant).
    pub entry_mid: Price,
    /// Residual seconds to resolution at the sample instant (drives ttr bucket).
    pub ttr_secs: u64,
    /// Whether the market settled YES (the truth key).
    pub settled_yes: bool,
}

/// Fit parameters for the bias table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BiasFitConfig {
    /// Number of equal-width price buckets over `(0, 1)`.
    pub bins: u32,
    /// Ascending time-to-resolution bucket boundaries in seconds. `n` bounds
    /// define `n + 1` buckets: `[0, b0), [b0, b1), …, [b_{n-1}, ∞)`.
    pub ttr_bucket_bounds_secs: Vec<u64>,
    /// Minimum samples for a price bin to carry a bias.
    pub min_bin_samples: u64,
    /// Minimum samples for a `(category, ttr_bucket)` curve to be retained.
    pub min_curve_samples: u64,
    /// Two-sided confidence level for the Wilson interval and the IC
    /// significance test (e.g. `0.95`).
    pub ci_confidence: Decimal,
    /// Absolute `|IC|` floor a curve must additionally clear to be significant.
    pub ic_significance_min: Decimal,
}

impl FavoriteLongshotBiasTable {
    /// Fit a bias table from PIT `(category, entry_mid, ttr_secs, settled_yes)`
    /// samples.
    ///
    /// Returns `Ok(None)` when **no** `(category, ttr_bucket)` curve clears its
    /// gates — the fit produced no usable curve, so no artifact is minted
    /// (fail-closed).
    ///
    /// # Errors
    ///
    /// Propagates canonical-hash serialization failures.
    pub fn fit(
        samples: &[BiasSample],
        fit_window: TimeWindow,
        calibration_split_hash: ContentHash,
        config: &BiasFitConfig,
    ) -> QuantResult<Option<Self>> {
        let z = wilson_z(config.ci_confidence);
        let ranges = ttr_ranges(&config.ttr_bucket_bounds_secs);
        let mut by_category: BTreeMap<MarketCategory, CategoryBiasCurve> = BTreeMap::new();
        for category in MarketCategory::ALL_VARIANTS {
            let category_samples: Vec<&BiasSample> =
                samples.iter().filter(|s| s.category == category).collect();
            let mut by_ttr = Vec::new();
            for &(lo, hi) in &ranges {
                let bucket_samples: Vec<&BiasSample> = category_samples
                    .iter()
                    .copied()
                    .filter(|s| s.ttr_secs >= lo && hi.is_none_or(|hi| s.ttr_secs < hi))
                    .collect();
                let total = bucket_samples.len() as u64;
                if total < config.min_curve_samples {
                    continue;
                }
                let bins = fit_bins(&bucket_samples, config, z);
                if bins.is_empty() {
                    continue;
                }
                let ic = curve_ic(&bucket_samples, &bins);
                let ic_significant =
                    ic_is_significant(ic, total, config.ci_confidence, config.ic_significance_min);
                by_ttr.push(TtrBucketCurve {
                    ttr_lo_secs: lo,
                    ttr_hi_secs: hi,
                    bins,
                    ic,
                    ic_significant,
                    sample_count: total,
                });
            }
            if by_ttr.is_empty() {
                continue;
            }
            let sample_count = by_ttr.iter().map(|c| c.sample_count).sum();
            by_category.insert(
                category,
                CategoryBiasCurve {
                    by_ttr,
                    sample_count,
                },
            );
        }
        if by_category.is_empty() {
            return Ok(None);
        }
        let content_hash = ResearchHasher::canonical(&BiasTableCanonical {
            fit_window: &fit_window,
            calibration_split_hash: &calibration_split_hash,
            by_category: &by_category,
        })?;
        Ok(Some(Self {
            table_id: CalibrationArtifactId::from_v7(),
            content_hash,
            fit_window,
            calibration_split_hash,
            by_category,
        }))
    }
}

/// Canonical projection for the content hash (excludes the surrogate id).
#[derive(Serialize)]
struct BiasTableCanonical<'a> {
    fit_window: &'a TimeWindow,
    calibration_split_hash: &'a ContentHash,
    by_category: &'a BTreeMap<MarketCategory, CategoryBiasCurve>,
}

/// Expand ascending ttr bucket boundaries into `[lo, hi)` ranges (top unbounded).
fn ttr_ranges(bounds: &[u64]) -> Vec<(u64, Option<u64>)> {
    let mut ranges = Vec::with_capacity(bounds.len() + 1);
    let mut lo = 0_u64;
    for &bound in bounds {
        // Skip degenerate / non-monotone bounds defensively (config validation
        // rejects these up front).
        if bound <= lo {
            continue;
        }
        ranges.push((lo, Some(bound)));
        lo = bound;
    }
    ranges.push((lo, None));
    ranges
}

/// Fit the retained price-bucket bins for one `(category, ttr)` curve.
///
/// A bin is retained only when it clears `min_bin_samples` **and** its Wilson
/// interval for the realized frequency excludes the implied mid — i.e. the bias
/// is statistically distinct from the market price at `ci_confidence`.
fn fit_bins(samples: &[&BiasSample], config: &BiasFitConfig, z: f64) -> Vec<PriceBiasBin> {
    let bin_count = config.bins.max(1);
    let width = Decimal::ONE / Decimal::from(bin_count);
    let mut bins = Vec::new();
    for index in 0..bin_count {
        let lo = width * Decimal::from(index);
        let top = index + 1 == bin_count;
        let hi = if top {
            Decimal::ONE
        } else {
            width * Decimal::from(index + 1)
        };
        let in_bin: Vec<&&BiasSample> = samples
            .iter()
            .filter(|s| {
                let mid = s.entry_mid.inner();
                mid >= lo && (mid < hi || (top && mid <= hi))
            })
            .collect();
        let n = u64::try_from(in_bin.len()).unwrap_or(u64::MAX);
        if n < config.min_bin_samples {
            continue;
        }
        let yes = u64::try_from(in_bin.iter().filter(|s| s.settled_yes).count()).unwrap_or(0);
        let p_hat = count_f64(yes) / count_f64(n);
        let implied_mid = (lo + hi) / Decimal::from(2);
        let realized = Decimal::from_f64(p_hat)
            .unwrap_or(Decimal::ZERO)
            .round_dp(STAT_SCALE);
        let (ci_lo, ci_hi) = wilson_interval(p_hat, n, z, STAT_SCALE);
        // Wilson significance gate: retain the bias only when the implied mid
        // falls outside the empirical-frequency confidence interval.
        let implied = implied_mid.round_dp(STAT_SCALE);
        if implied >= ci_lo && implied <= ci_hi {
            continue;
        }
        let signed_bias = (realized - implied_mid).round_dp(STAT_SCALE);
        bins.push(PriceBiasBin {
            price_lo: Price::new(lo),
            price_hi: Price::new(hi),
            implied_mid: Price::new(implied_mid),
            realized_frequency: Probability::new(realized),
            bias: signed_bias,
            bias_ci: (ci_lo, ci_hi),
            sample_count: n,
        });
    }
    bins
}

/// Per-curve information coefficient: the Pearson correlation between the
/// bin-implied bias signal and the realized excess `settled − entry_mid`.
///
/// A high, sign-consistent correlation means the empirical bias genuinely
/// predicts the settlement residual (not a statistical artifact).
fn curve_ic(samples: &[&BiasSample], curve: &[PriceBiasBin]) -> Decimal {
    let mut xs: Vec<f64> = Vec::new();
    let mut ys: Vec<f64> = Vec::new();
    for sample in samples {
        let mid = sample.entry_mid.inner();
        let Some(bias) = curve
            .iter()
            .find(|bucket| bucket.contains(mid))
            .map(|bucket| bucket.bias)
        else {
            continue;
        };
        let residual = if sample.settled_yes {
            Decimal::ONE - mid
        } else {
            -mid
        };
        let (Some(x), Some(y)) = (bias.to_f64(), residual.to_f64()) else {
            continue;
        };
        xs.push(x);
        ys.push(y);
    }
    Decimal::from_f64(pearson(&xs, &ys))
        .unwrap_or(Decimal::ZERO)
        .round_dp(STAT_SCALE)
}

/// Whether a correlation `ic` over `n` paired samples is significant: it must
/// clear the absolute `|IC|` floor **and** a two-sided Student-t test at
/// `ci_confidence` (`t = IC·√((n−2)/(1−IC²))`, df = `n − 2`).
fn ic_is_significant(ic: Decimal, n: u64, ci_confidence: Decimal, ic_floor: Decimal) -> bool {
    if ic.abs() < ic_floor {
        return false;
    }
    if n < 3 {
        return false;
    }
    let Some(r) = ic.to_f64() else {
        return false;
    };
    let nn = count_f64(n);
    let denom = r.mul_add(-r, 1.0).max(1e-12);
    let t = r * ((nn - 2.0) / denom).sqrt();
    let level = ci_confidence.to_f64().unwrap_or(0.95).clamp(0.5, 0.999_999);
    let upper = 1.0 - (1.0 - level) / 2.0;
    let df = nn - 2.0;
    let t_crit = StudentsT::new(0.0, 1.0, df).map_or(1.96, |dist| dist.inverse_cdf(upper));
    t.abs() >= t_crit
}

/// Pearson correlation (0.0 for degenerate/empty inputs).
fn pearson(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len();
    if n < 2 || n != ys.len() {
        return 0.0;
    }
    let count = count_f64(u64::try_from(n).unwrap_or(u64::MAX));
    let mean_x = xs.iter().sum::<f64>() / count;
    let mean_y = ys.iter().sum::<f64>() / count;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        let dx = x - mean_x;
        let dy = y - mean_y;
        cov = dx.mul_add(dy, cov);
        var_x = dx.mul_add(dx, var_x);
        var_y = dy.mul_add(dy, var_y);
    }
    if var_x <= 0.0 || var_y <= 0.0 {
        return 0.0;
    }
    let corr = cov / (var_x.sqrt() * var_y.sqrt());
    if corr.is_finite() { corr } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::{BiasFitConfig, BiasSample, FavoriteLongshotBiasTable};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::query::TimeWindow,
        enums::common::MarketCategory,
        types::{ContentHash, MarketId, Price},
    };
    use rust_decimal::Decimal;

    fn window() -> TimeWindow {
        TimeWindow::new(
            Utc.timestamp_opt(0, 0).unwrap(),
            Utc.timestamp_opt(1_000, 0).unwrap(),
        )
    }

    fn split_hash() -> ContentHash {
        crate::hashing::ResearchHasher::canonical(&"calibration-split").unwrap()
    }

    fn config() -> BiasFitConfig {
        BiasFitConfig {
            bins: 4,
            // Single unbounded ttr bucket keeps these unit tests focused on the
            // price-bucket + significance logic.
            ttr_bucket_bounds_secs: Vec::new(),
            min_bin_samples: 10,
            min_curve_samples: 20,
            ci_confidence: Decimal::new(95, 2),
            ic_significance_min: Decimal::new(1, 2),
        }
    }

    fn sample(
        index: u64,
        category: MarketCategory,
        entry_mid: Price,
        ttr_secs: u64,
        settled_yes: bool,
    ) -> BiasSample {
        BiasSample {
            market_id: MarketId::new(format!("m-{index}")),
            sampled_at: Utc
                .timestamp_opt(i64::try_from(index).unwrap_or(i64::MAX), 0)
                .unwrap(),
            category,
            entry_mid,
            ttr_secs,
            settled_yes,
        }
    }

    /// Below the curve sample gate → no artifact (fail-closed greenfield).
    #[test]
    fn fit_fail_closed_when_insufficient_samples() {
        let samples = (0..5)
            .map(|i| {
                sample(
                    i,
                    MarketCategory::Crypto,
                    Price::new(Decimal::new(1, 1)),
                    3_600,
                    true,
                )
            })
            .collect::<Vec<_>>();
        let table =
            FavoriteLongshotBiasTable::fit(&samples, window(), split_hash(), &config()).unwrap();
        assert!(table.is_none(), "thin data must produce no artifact");
    }

    /// A favorite-longshot skew (low-price tokens over-priced) yields a signed,
    /// price-varying bias — never a constant.
    #[test]
    fn fit_produces_non_constant_signed_bias() {
        let mut samples = Vec::new();
        // Low-price bucket: implied ~0.125 but settles YES far less often (skew).
        for i in 0..200 {
            samples.push(sample(
                i,
                MarketCategory::Crypto,
                Price::new(Decimal::new(1, 1)),
                3_600,
                i % 20 == 0,
            ));
        }
        // High-price bucket: implied ~0.875 but settles YES more often.
        for i in 200..400 {
            samples.push(sample(
                i,
                MarketCategory::Crypto,
                Price::new(Decimal::new(9, 1)),
                3_600,
                i % 20 != 0,
            ));
        }
        let table = FavoriteLongshotBiasTable::fit(&samples, window(), split_hash(), &config())
            .unwrap()
            .expect("qualifying category yields a table");
        let low = table
            .bias_for(
                MarketCategory::Crypto,
                3_600,
                Price::new(Decimal::new(1, 1)),
                false,
            )
            .expect("low bucket bias");
        let high = table
            .bias_for(
                MarketCategory::Crypto,
                3_600,
                Price::new(Decimal::new(9, 1)),
                false,
            )
            .expect("high bucket bias");
        assert!(low < Decimal::ZERO, "low-price token over-priced: {low}");
        assert!(
            high > Decimal::ZERO,
            "high-price token under-priced: {high}"
        );
        assert_ne!(low, high, "bias must vary by price bucket (not constant)");
    }

    /// An unmapped category / uncovered bucket returns no bias (inert factor).
    #[test]
    fn bias_absent_for_unfit_category() {
        let samples = (0..50)
            .map(|i| {
                sample(
                    i,
                    MarketCategory::Crypto,
                    Price::new(Decimal::new(5, 1)),
                    3_600,
                    true,
                )
            })
            .collect::<Vec<_>>();
        let table =
            FavoriteLongshotBiasTable::fit(&samples, window(), split_hash(), &config()).unwrap();
        // A single price bucket with a 50/50-ish skew at implied 0.5 may or may
        // not clear the Wilson gate; either way, an unfit category has no bias.
        if let Some(table) = table {
            assert!(
                table
                    .bias_for(
                        MarketCategory::Sports,
                        3_600,
                        Price::new(Decimal::new(5, 1)),
                        false
                    )
                    .is_none(),
                "unfit category has no bias"
            );
        }
    }
}
