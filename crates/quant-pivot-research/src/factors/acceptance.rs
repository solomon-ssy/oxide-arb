//! Factor-plane acceptance tests (Phase 11.1 §8).
//!
//! Closes audit #1 (all generic families default-on), #2 (momentum is not a
//! return clone; collinearity is detectable), #3 (config-driven normalization,
//! no silent neutral — small / degenerate cross-sections are indeterminate).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{
        factor::{FactorFamily, FactorIndeterminateReason},
        quant::DataQualityStatus,
    },
    runtime_config::{DecimalString, FactorsConfig, FeaturesConfig, MissingFactorPolicy},
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::Decimal;

use crate::factors::{
    CollinearPair, FactorCollinearityAnalyzer, FactorEligibility, FactorEngine, FactorName,
    FactorObservationMatrix, MarketFactorOutcome, NormalizedFactor, ScoredFactor,
};
use crate::features::{FeatureName, FeatureValue, FeatureVector, NullReason};

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
fn online_and_batch_use_same_normalizer() {
    // The serial and rayon paths share one `CrossSectionalNormalizer`, so they
    // must be bit-identical — the guarantee that online and backtest normalize
    // through the same code (Phase 11.6 parity seam).
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
