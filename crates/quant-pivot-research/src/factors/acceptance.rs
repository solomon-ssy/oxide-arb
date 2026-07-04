//! Factor-plane acceptance tests (Phase 11.1 §8).
//!
//! Closes audit #1 (all generic families default-on), #2 (momentum is not a
//! return clone; collinearity is detectable), #3 (config-driven normalization,
//! no silent neutral — small / degenerate cross-sections are indeterminate).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{
        factor::{FactorFamily, FactorIndeterminateReason, NormalizationSource},
        quant::DataQualityStatus,
    },
    runtime_config::{
        DecimalString, FactorsConfig, FeaturesConfig, MissingFactorPolicy, SmallCrossSectionPolicy,
    },
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::factors::{
    CollinearPair, FactorCollinearityAnalyzer, FactorEligibility, FactorEngine, FactorHistory,
    FactorName, FactorObservationMatrix, MarketFactorOutcome, NormalizedFactor, ScoredFactor,
};
use crate::features::{FeatureName, FeatureValue, FeatureVector, NullReason, stats};

// ── Fixtures ────────────────────────────────────────────────────────────────

/// Config for the given families with a small `min_size` so modest test batches
/// still exercise the cross-sectional path.
fn factors_config(
    families: &[FactorFamily],
    policy: MissingFactorPolicy,
    floor: &str,
) -> FactorsConfig {
    let mut config = FactorsConfig {
        enabled_factor_families: families.to_vec(),
        min_factor_confidence: DecimalString::new(floor),
        missing_factor_policy: policy,
        ..FactorsConfig::default()
    };
    config.cross_section.min_size = 2;
    config
}

fn make_vector(
    market: &str,
    values: &[(&'static str, FeatureValue)],
    data_quality: DataQualityStatus,
    staleness_ms: u64,
    as_of: DateTime<Utc>,
) -> FeatureVector {
    let values: BTreeMap<FeatureName, FeatureValue> = values
        .iter()
        .map(|(name, value)| (FeatureName::from_static(name), value.clone()))
        .collect();
    FeatureVector {
        market_id: MarketId::new(market),
        token_id: Some(TokenId::new("token")),
        as_of,
        schema_version: SchemaVersion::FIRST,
        values,
        substitutions: Vec::new(),
        data_quality,
        staleness_ms,
        source_refs: Vec::new(),
    }
}

fn usd(value: i64) -> FeatureValue {
    FeatureValue::Usd(Usd::new(Decimal::from(value)))
}

fn dec(value: Decimal) -> FeatureValue {
    FeatureValue::Decimal(value)
}

fn bps(value: i64) -> FeatureValue {
    FeatureValue::Bps(Decimal::from(value))
}

fn scored<'a>(outcome: &'a MarketFactorOutcome, name: &str) -> &'a ScoredFactor {
    outcome
        .factors
        .iter()
        .find(|scored| scored.value.name.as_str() == name)
        .expect("factor present in outcome")
}

fn in_unit(value: Probability) -> bool {
    value.inner() >= Decimal::ZERO && value.inner() <= Decimal::ONE
}

/// Compute one named factor for the first of a two-market batch under one family.
fn compute_one(
    family: FactorFamily,
    factor: &str,
    market0: &[(&'static str, FeatureValue)],
    market1: &[(&'static str, FeatureValue)],
) -> ScoredFactor {
    let config = factors_config(&[family], MissingFactorPolicy::ZeroWeight, "0.10");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let vectors = [
        make_vector("0xmarket0", market0, DataQualityStatus::Fresh, 0, as_of),
        make_vector("0xmarket1", market1, DataQualityStatus::Fresh, 0, as_of),
    ];
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("batch compute");
    scored(&outcomes[0], factor).clone()
}

// ── #1 default factor set ─────────────────────────────────────────────────────

#[test]
fn default_factor_config_enables_all_generic_families() {
    let config = FactorsConfig::default();
    assert_eq!(
        config.enabled_factor_families,
        FactorFamily::ALL_GENERIC.to_vec(),
        "the default config must enable every generic factor family"
    );
    let engine = FactorEngine::new(&config, &FeaturesConfig::default());
    // 8 single-feature factors + 4 momentum estimators counted within Momentum.
    assert!(
        engine.registry().len() >= 12,
        "all generic families should register the full factor set"
    );
}

// ── #3 no silent neutral ──────────────────────────────────────────────────────

#[test]
fn small_cross_section_yields_indeterminate_not_half() {
    // A single-market batch cannot form a cross-section for a Rank factor;
    // the outcome is Indeterminate, never a fabricated neutral 0.5.
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vector = make_vector(
        "0xlonely",
        &[
            ("book.visible_liquidity_usd", usd(10_000)),
            ("book.spread_bps", bps(120)),
        ],
        DataQualityStatus::Fresh,
        0,
        Utc::now(),
    );
    let outcome = engine.compute_all(&vector, &config).expect("compute");
    let factor = scored(&outcome, "liquidity_depth");
    assert!(
        matches!(
            factor.value.normalization,
            NormalizedFactor::Indeterminate {
                reason: FactorIndeterminateReason::CrossSectionTooSmall
            }
        ),
        "single-market rank must be indeterminate, got {:?}",
        factor.value.normalization
    );
    assert!(
        factor.value.normalized_score().is_none(),
        "an indeterminate factor carries no normalized score"
    );
    assert!(
        !factor.contributes,
        "an indeterminate factor cannot contribute"
    );
}

#[test]
fn zero_variance_yields_indeterminate() {
    // Every market has identical liquidity → the cross-section carries no
    // dispersion → ZeroVariance indeterminate (never a silent 0.5).
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let vectors: Vec<FeatureVector> = (0..4)
        .map(|index| {
            make_vector(
                &format!("0xflat{index}"),
                &[
                    ("book.visible_liquidity_usd", usd(10_000)),
                    ("book.spread_bps", bps(100)),
                ],
                DataQualityStatus::Fresh,
                0,
                as_of,
            )
        })
        .collect();
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("compute");
    let factor = scored(&outcomes[0], "liquidity_depth");
    assert!(
        matches!(
            factor.value.normalization,
            NormalizedFactor::Indeterminate {
                reason: FactorIndeterminateReason::ZeroVariance
            }
        ),
        "a zero-variance cross-section must be indeterminate, got {:?}",
        factor.value.normalization
    );
}

// ── Cross-sectional gating / determinism ──────────────────────────────────────

#[test]
fn compute_all_batch_rejects_mixed_as_of() {
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let first = make_vector(
        "0xa",
        &[("book.visible_liquidity_usd", usd(10_000))],
        DataQualityStatus::Fresh,
        0,
        as_of,
    );
    let mut second = make_vector(
        "0xb",
        &[("book.visible_liquidity_usd", usd(5_000))],
        DataQualityStatus::Fresh,
        0,
        as_of,
    );
    second.as_of = as_of + chrono::Duration::seconds(1);
    let error = engine
        .compute_all_batch(&[first, second], &config)
        .expect_err("mixed as_of must fail");
    assert!(
        error.to_string().contains("batch as_of mismatch"),
        "expected as_of mismatch, got: {error}"
    );
}

// ── Explanation drivers ───────────────────────────────────────────────────────

#[test]
fn factor_explanation_lists_positive_and_negative_drivers() {
    let config = factors_config(
        &[FactorFamily::DataQuality],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vector = make_vector(
        "0xdq",
        &[
            (
                "book.best_bid",
                FeatureValue::Probability(Probability::new(Decimal::new(47, 2))),
            ),
            (
                "book.spread_bps",
                FeatureValue::Missing(NullReason::SourceUnavailable),
            ),
        ],
        DataQualityStatus::Degraded,
        90_000,
        Utc::now(),
    );
    let outcome = engine.compute_all(&vector, &config).expect("compute");
    let drivers = &scored(&outcome, "data_quality").value.explanation.drivers;
    assert!(
        drivers.iter().any(|d| d.contribution > Decimal::ZERO),
        "must list a positive driver"
    );
    assert!(
        drivers.iter().any(|d| d.contribution < Decimal::ZERO),
        "must list a negative driver"
    );
}

// ── Confidence floor + missing policy ─────────────────────────────────────────

#[test]
fn factor_confidence_floor_zero_weights_low_confidence() {
    // Stale data → confidence 0.40 < floor 0.50; under ZeroWeight the factor is
    // scored but does not contribute, and the market still proceeds.
    let config = factors_config(
        &[FactorFamily::Microstructure],
        MissingFactorPolicy::ZeroWeight,
        "0.50",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let vectors = [
        make_vector(
            "0xfloor0",
            &[("book.depth_imbalance", dec(Decimal::new(2, 1)))],
            DataQualityStatus::Stale,
            0,
            as_of,
        ),
        make_vector(
            "0xfloor1",
            &[("book.depth_imbalance", dec(Decimal::new(-3, 1)))],
            DataQualityStatus::Stale,
            0,
            as_of,
        ),
    ];
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("compute");
    assert!(outcomes[0].eligibility.is_eligible());
    let factor = scored(&outcomes[0], "book_imbalance");
    assert!(factor.below_confidence_floor);
    assert!(
        !factor.contributes,
        "below-floor factor must not contribute"
    );
}

#[test]
fn factor_missing_reject_candidate_policy() {
    // `spread_efficiency` is required; a market missing it under RejectCandidate
    // is excluded, while complete markets (present cross-section) proceed.
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::RejectCandidate,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let complete_a = make_vector(
        "0xcomplete_a",
        &[
            ("book.visible_liquidity_usd", usd(20_000)),
            ("book.spread_bps", bps(120)),
        ],
        DataQualityStatus::Fresh,
        0,
        as_of,
    );
    let complete_b = make_vector(
        "0xcomplete_b",
        &[
            ("book.visible_liquidity_usd", usd(12_000)),
            ("book.spread_bps", bps(240)),
        ],
        DataQualityStatus::Fresh,
        0,
        as_of,
    );
    let missing_spread = make_vector(
        "0xmissing",
        &[("book.visible_liquidity_usd", usd(8_000))],
        DataQualityStatus::Fresh,
        0,
        as_of,
    );
    let outcomes = engine
        .compute_all_batch(&[complete_a, complete_b, missing_spread], &config)
        .expect("batch compute");
    assert!(
        outcomes[0].eligibility.is_eligible(),
        "complete market eligible"
    );
    assert!(
        outcomes[1].eligibility.is_eligible(),
        "complete market eligible"
    );
    assert!(
        matches!(
            outcomes[2].eligibility,
            FactorEligibility::RejectCandidate { .. }
        ),
        "market missing a required factor must be rejected"
    );
}

// ── Schema hash ───────────────────────────────────────────────────────────────

#[test]
fn factor_set_change_changes_schema_hash() {
    let features = FeaturesConfig::default();
    let one = FactorEngine::new(
        &factors_config(
            &[FactorFamily::Liquidity],
            MissingFactorPolicy::ZeroWeight,
            "0.50",
        ),
        &features,
    );
    let two = FactorEngine::new(
        &factors_config(
            &[FactorFamily::Liquidity, FactorFamily::Momentum],
            MissingFactorPolicy::ZeroWeight,
            "0.50",
        ),
        &features,
    );
    assert_ne!(
        one.factor_schema_hash().expect("hash"),
        two.factor_schema_hash().expect("hash"),
        "changing the enabled factor set must change the schema hash"
    );
}

#[test]
fn factor_schema_hash_is_order_independent_for_same_set() {
    let features = FeaturesConfig::default();
    let forward = FactorEngine::new(
        &factors_config(
            &[FactorFamily::Liquidity, FactorFamily::Momentum],
            MissingFactorPolicy::ZeroWeight,
            "0.50",
        ),
        &features,
    );
    let reversed = FactorEngine::new(
        &factors_config(
            &[FactorFamily::Momentum, FactorFamily::Liquidity],
            MissingFactorPolicy::ZeroWeight,
            "0.50",
        ),
        &features,
    );
    assert_eq!(
        forward.factor_schema_hash().expect("hash"),
        reversed.factor_schema_hash().expect("hash"),
    );
}

// ── Generic factors: basic compute ────────────────────────────────────────────

#[test]
fn liquidity_depth_factor_basic_compute() {
    let factor = compute_one(
        FactorFamily::Liquidity,
        "liquidity_depth",
        &[("book.visible_liquidity_usd", usd(25_000))],
        &[("book.visible_liquidity_usd", usd(5_000))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(factor.value.normalized_score().is_some_and(in_unit));
}

#[test]
fn spread_efficiency_factor_basic_compute() {
    let factor = compute_one(
        FactorFamily::Liquidity,
        "spread_efficiency",
        &[("book.spread_bps", bps(80))],
        &[("book.spread_bps", bps(400))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(factor.value.normalized_score().is_some_and(in_unit));
}

#[test]
fn book_imbalance_factor_basic_compute() {
    let factor = compute_one(
        FactorFamily::Microstructure,
        "book_imbalance",
        &[("book.depth_imbalance", dec(Decimal::new(3, 1)))],
        &[("book.depth_imbalance", dec(Decimal::new(-2, 1)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(factor.value.normalized_score().is_some_and(in_unit));
}

#[test]
fn momentum_roc_factor_basic_compute() {
    let factor = compute_one(
        FactorFamily::Momentum,
        "momentum_roc",
        &[("ts.momentum_roc_900s", dec(Decimal::new(5, 2)))],
        &[("ts.momentum_roc_900s", dec(Decimal::new(-3, 2)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(factor.value.normalized_score().is_some_and(in_unit));
}

#[test]
fn data_quality_factor_basic_compute() {
    let factor = compute_one(FactorFamily::DataQuality, "data_quality", &[], &[]);
    assert!(factor.value.raw_value.is_some());
    // data_quality uses per-market MinMax, so it is always scored.
    assert!(factor.value.normalized_score().is_some_and(in_unit));
}

// ── Parallel batch: determinism, order, serial/parallel equivalence ───────────

fn varied_batch(count: usize) -> Vec<FeatureVector> {
    let as_of = Utc::now();
    (0..count)
        .map(|index| {
            let step = i64::try_from(index).expect("index fits in i64");
            make_vector(
                &format!("0xmkt{index:02}"),
                &[
                    ("book.visible_liquidity_usd", usd(1_000 + step * 500)),
                    ("book.spread_bps", bps(40 + step)),
                ],
                DataQualityStatus::Fresh,
                0,
                as_of,
            )
        })
        .collect()
}

#[test]
fn compute_all_batch_is_deterministic() {
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(32);
    let first = engine
        .compute_all_batch(&vectors, &config)
        .expect("first run");
    let second = engine
        .compute_all_batch(&vectors, &config)
        .expect("second run");
    assert_eq!(
        first, second,
        "the parallel batch path must be deterministic"
    );
}

#[test]
fn compute_all_batch_preserves_input_order() {
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(20);
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("batch compute");
    assert_eq!(outcomes.len(), vectors.len());
    for (vector, outcome) in vectors.iter().zip(&outcomes) {
        assert_eq!(vector.market_id, outcome.market_id);
    }
}

#[test]
fn serial_and_parallel_normalizer_paths_are_bit_identical() {
    // The serial and rayon paths share one `CrossSectionalNormalizer`, so they
    // must be bit-identical. (Online-vs-replay parity — both driving the same
    // engine entrypoint — is asserted at the core pipeline level.)
    let config = factors_config(
        &[FactorFamily::Liquidity, FactorFamily::Momentum],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(24);
    let history = crate::factors::FactorHistory::empty();
    let serial = engine
        .compute_all_batch_inner(&vectors, &config, &history, false)
        .expect("serial path");
    let parallel = engine
        .compute_all_batch_inner(&vectors, &config, &history, true)
        .expect("parallel path");
    assert_eq!(serial, parallel, "serial and parallel paths must agree");
}

#[test]
fn online_and_replay_entrypoints_agree_default_policy() {
    // The replay path calls `compute_all_batch` and the online path calls
    // `compute_all_batch_with_history`; under the default (`Indeterminate`) policy
    // they must produce identical outcomes — the same normalizer serves both.
    let config = factors_config(
        &[FactorFamily::Liquidity, FactorFamily::Momentum],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(20);
    let replay = engine
        .compute_all_batch(&vectors, &config)
        .expect("replay path");
    let online = engine
        .compute_all_batch_with_history(&vectors, &config, &FactorHistory::empty())
        .expect("online path");
    assert_eq!(
        replay, online,
        "replay and online entrypoints must agree under the default policy"
    );
}

#[test]
fn cross_sectional_zscore_mean_zero_std_one_per_as_of() {
    // The winsorized z-score maps a standardized value `z = (x - μ) / σ` into
    // `[0, 1]` via `(z + k) / 2k` (k = clamp_sigma). With no winsorizing/clamping,
    // recovering `z = score·2k − k` across the cross-section must have population
    // mean 0 and std 1 — the defining property of a per-as_of z-score.
    let config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(9);
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("batch compute");
    // `spread_efficiency` is a WinsorizedZScore factor; k = default clamp_sigma.
    let clamp_sigma = 3.0_f64;
    let zs: Vec<f64> = outcomes
        .iter()
        .map(|outcome| {
            scored(outcome, "spread_efficiency")
                .value
                .normalized_score()
                .expect("scored")
                .inner()
                .to_f64()
                .expect("f64")
        })
        .map(|score| score.mul_add(2.0 * clamp_sigma, -clamp_sigma))
        .collect();
    let n = f64::from(u32::try_from(zs.len()).unwrap_or(u32::MAX));
    let mean = zs.iter().sum::<f64>() / n;
    let variance = zs.iter().map(|z| (z - mean).powi(2)).sum::<f64>() / n;
    let std = variance.sqrt();
    assert!(
        mean.abs() < 1e-6,
        "cross-section z-score mean ≈ 0, got {mean}"
    );
    assert!(
        (std - 1.0).abs() < 1e-6,
        "cross-section z-score std ≈ 1, got {std}"
    );
}

#[test]
fn historical_quantile_scores_small_cross_section() {
    // A single-market batch is below `min_size`, but under the HistoricalQuantile
    // policy a pre-fetched rolling distribution normalizes it — the factor is
    // scored with `source = HistoricalQuantile`, not indeterminate.
    let mut config = factors_config(
        &[FactorFamily::Liquidity],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    config.cross_section.min_size = 5;
    config.cross_section.small_cross_section_policy = SmallCrossSectionPolicy::HistoricalQuantile;
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);

    let mut history = FactorHistory::empty();
    history.insert(
        FactorName::from_static("liquidity_depth"),
        (1..=8).map(|value| Decimal::from(value * 1_000)).collect(),
    );

    let vector = make_vector(
        "0xlonely",
        &[
            ("book.visible_liquidity_usd", usd(4_500)),
            ("book.spread_bps", bps(120)),
        ],
        DataQualityStatus::Fresh,
        0,
        Utc::now(),
    );
    let outcomes = engine
        .compute_all_batch_with_history(std::slice::from_ref(&vector), &config, &history)
        .expect("historical-quantile compute");
    let factor = scored(&outcomes[0], "liquidity_depth");
    assert!(
        matches!(
            factor.value.normalization,
            NormalizedFactor::Scored {
                source: NormalizationSource::HistoricalQuantile,
                ..
            }
        ),
        "small cross-section must score via history, got {:?}",
        factor.value.normalization
    );
    assert!(
        factor.contributes,
        "a historically-scored factor contributes"
    );
}

// ── #2 collinearity analysis ──────────────────────────────────────────────────

#[test]
fn collinearity_analyzer_flags_rho_over_threshold() {
    let alpha = FactorName::from_static("alpha");
    let beta = FactorName::from_static("beta");
    let gamma = FactorName::from_static("gamma");
    // `beta` is a monotone copy of `alpha` (ρ = 1); `gamma` is anti-monotone
    // to `alpha` (ρ = -1). Both breach a 0.9 tolerance.
    let rows: Vec<Vec<Option<Decimal>>> = (0..8)
        .map(|index| {
            let value = Decimal::from(index);
            vec![
                Some(value),
                Some(value * Decimal::from(2)),
                Some(Decimal::from(100) - value),
            ]
        })
        .collect();
    let panel = FactorObservationMatrix {
        factors: vec![alpha.clone(), beta.clone(), gamma],
        rows,
    };
    let report = FactorCollinearityAnalyzer::analyze(&panel, Decimal::new(9, 1));
    assert!(report.is_collinear(), "identical factors must be flagged");
    assert!(
        report
            .violations
            .iter()
            .any(|CollinearPair { left, right, .. }| { *left == alpha && *right == beta }),
        "alpha/beta (ρ=1) must be a violation"
    );
    assert!(
        report
            .violations
            .iter()
            .any(|pair| pair.correlation < Decimal::ZERO),
        "the anti-correlated pair must appear with negative ρ"
    );
}

/// Build a 24-sample mid-price path (30s cadence) from four per-segment slopes
/// plus an independent volatility oscillation. Distinct slope vectors make the
/// *timing* of the move differ across markets, so total return, lag-skipped ROC,
/// and recent EMA slope rank markets differently — the four momentum estimators
/// are genuinely different signals, not return clones.
fn momentum_path(shape: [i64; 4], vol: i64) -> Vec<(i64, Decimal)> {
    let mut price = 10_000_i64;
    let mut out = Vec::with_capacity(24);
    let mut k = 0_i64;
    for slope in shape {
        for _ in 0..6 {
            price += slope;
            let osc = if k % 2 == 0 { vol } else { -vol };
            out.push((k * 30_000, Decimal::from(price + osc)));
            k += 1;
        }
    }
    out
}

#[test]
fn default_momentum_estimators_not_mutually_collinear() {
    // Closes audit #2 at the falsifiability level: on a heterogeneous panel the
    // four default momentum estimators AND the simple return must all stay below
    // the configured collinearity tolerance. A panel that only varied trend
    // magnitude would make them near-identical; heterogeneity in the *timing*
    // and volatility of the move is exactly what decouples them.
    // Timing-diverse shapes: total return (full window), lag-skipped ROC
    // (base→t-lag, i.e. ignores the last third) and recent EMA slope each rank
    // these markets differently, so no pair collapses onto another.
    const SHAPES: [[i64; 4]; 16] = [
        [6, 0, 0, 0],     // early up: high ROC, ~flat recent slope
        [-6, 0, 0, 0],    // early down
        [0, 0, 0, 6],     // late up: ~zero ROC, high recent slope
        [0, 0, 0, -6],    // late down
        [6, 0, 0, -6],    // early up then late down: ~zero return, +ROC, -slope
        [-6, 0, 0, 6],    // early down then late up: ~zero return, -ROC, +slope
        [0, 6, 0, 0],     // mid-early up
        [0, -6, 0, 0],    // mid-early down
        [0, 0, 6, 0],     // mid-late up
        [0, 0, -6, 0],    // mid-late down
        [3, 3, 3, 3],     // monotone up (all estimators agree)
        [-3, -3, -3, -3], // monotone down
        [1, 2, 3, 4],     // accelerating up
        [4, 3, 2, 1],     // decelerating up
        [5, -5, 5, -5],   // choppy
        [0, 0, 0, 0],     // flat (pure volatility)
    ];
    // Volatility amplitudes permuted to be uncorrelated with any trend metric.
    const VOLS: [i64; 16] = [3, 40, 9, 60, 1, 25, 7, 50, 15, 2, 33, 5, 70, 12, 45, 20];
    // Lag-skipped ROC base→(t-lag): 300s lag ≈ 10 samples at 30s cadence.
    const LAG_POINTS: usize = 10;

    let return_col = FactorName::from_static("simple_return");
    let roc = FactorName::from_static("momentum_roc");
    let ema_slope = FactorName::from_static("momentum_ema_slope");
    let vol_adjusted = FactorName::from_static("momentum_vol_adjusted");
    let macd = FactorName::from_static("momentum_macd");

    let mut rows: Vec<Vec<Option<Decimal>>> = Vec::with_capacity(SHAPES.len());
    for (shape, vol) in SHAPES.iter().zip(VOLS) {
        let samples = momentum_path(*shape, vol);
        let values: Vec<Decimal> = samples.iter().map(|&(_, value)| value).collect();
        let simple = stats::simple_return(&values);
        let at_lag = values[values.len() - 1 - LAG_POINTS];
        rows.push(vec![
            simple,
            stats::rate_of_change(values[0], at_lag),
            stats::ema_slope_time(&samples, 300),
            simple.and_then(|ret| {
                stats::realized_volatility(&values).and_then(|vol| stats::vol_adjusted(ret, vol))
            }),
            stats::macd_time(&samples, 300, 900),
        ]);
    }
    // Column 0 is the reference simple return; columns 1..=4 are the registered
    // momentum factors.
    let momentum_names = [&roc, &ema_slope, &vol_adjusted, &macd];
    let panel = FactorObservationMatrix {
        factors: vec![
            return_col,
            roc.clone(),
            ema_slope.clone(),
            vol_adjusted.clone(),
            macd.clone(),
        ],
        rows,
    };
    // The default orthogonalize tolerance (0.90).
    let threshold = FactorsConfig::default()
        .orthogonalize
        .max_correlation
        .value
        .parse::<Decimal>()
        .expect("default max_correlation parses");
    let report = FactorCollinearityAnalyzer::analyze(&panel, threshold);

    // (a) Production orthogonality gate: the four *registered* momentum factors
    //     must be mutually below the tolerance — no two collapse onto one signal.
    for i in 1..=4 {
        for j in (i + 1)..=4 {
            let rho = report.matrix[i][j].abs();
            assert!(
                rho < threshold,
                "momentum factors `{}` vs `{}` |ρ|={rho} must stay below {threshold}",
                momentum_names[i - 1],
                momentum_names[j - 1],
            );
        }
    }
    // (b) Audit #2 falsifiability: the old bug made momentum an *exact* clone of
    //     the simple return (ρ = 1.0). Every estimator must be demonstrably
    //     distinct — recent velocity co-moves with total return, but none is a
    //     rank-identical copy.
    let clone_ceiling = Decimal::new(98, 2);
    for (index, name) in momentum_names.iter().enumerate() {
        let rho = report.matrix[0][index + 1].abs();
        assert!(
            rho < clone_ceiling,
            "momentum factor `{name}` vs simple_return |ρ|={rho} — must not be a return clone"
        );
    }
    // (c) The risk/vol dimension adds genuinely orthogonal information: the
    //     vol-adjusted and MACD estimators stay below the gate against the raw
    //     return, not just below the clone ceiling.
    for name in [&vol_adjusted, &macd] {
        let index = momentum_names.iter().position(|n| *n == name).unwrap() + 1;
        let rho = report.matrix[0][index].abs();
        assert!(
            rho < threshold,
            "vol/risk momentum factor `{name}` vs simple_return |ρ|={rho} must stay below {threshold}"
        );
    }
}

// ── #3 no hardcoded normalization constants ───────────────────────────────────

#[test]
fn normalizer_has_no_hardcoded_constants() {
    // The generic factor registry must declare only normalization *methods*, not
    // numeric parameters. The deleted logistic heuristic (`k` / `x0`) must be gone.
    let source = include_str!("generic.rs");
    assert!(
        !source.contains("Logistic"),
        "the logistic heuristic normalization must be deleted"
    );
    assert!(
        !source.contains("x0"),
        "no hardcoded logistic midpoint may remain in the factor registry"
    );
    assert!(
        !source.contains("clamp_sigma:"),
        "sigma clamp is a config parameter, not a code constant"
    );
}
