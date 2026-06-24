//! Factor-plane acceptance tests (03.3 §8).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::quant::DataQualityStatus,
    runtime_config::{DecimalString, FactorsConfig, FeaturesConfig, MissingFactorPolicy},
    types::{MarketId, Probability, SchemaVersion, TokenId, Usd},
};
use rust_decimal::Decimal;

use crate::{
    factors::{FactorEligibility, FactorEngine, MarketFactorOutcome, ScoredFactor},
    features::{FeatureName, FeatureValue, FeatureVector, NullReason},
};

// ── Fixtures ────────────────────────────────────────────────────────────────

fn factors_config(families: &[&str], policy: MissingFactorPolicy, floor: &str) -> FactorsConfig {
    FactorsConfig {
        enabled_factor_families: families.iter().map(|f| (*f).to_owned()).collect(),
        min_factor_confidence: DecimalString::new(floor),
        missing_factor_policy: policy,
        ..FactorsConfig::default()
    }
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

fn count(value: u64) -> FeatureValue {
    FeatureValue::Count(value)
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
    family: &str,
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

// ── Normalization / clamp ─────────────────────────────────────────────────────

#[test]
fn factor_normalization_clamps_into_probability() {
    // `book.depth_imbalance` is bounded to [-1, 1] by MinMax; an out-of-range raw
    // is clamped to the bound *and* the clamp is recorded — never silently eaten.
    let config = factors_config(&["microstructure"], MissingFactorPolicy::ZeroWeight, "0.10");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vector = make_vector(
        "0xclamp",
        &[("book.depth_imbalance", dec(Decimal::from(5)))],
        DataQualityStatus::Fresh,
        0,
        Utc::now(),
    );
    let outcome = engine.compute_all(&vector, &config).expect("compute");
    let factor = scored(&outcome, "book_imbalance");
    assert_eq!(factor.value.normalized_score, Probability::ONE);
    assert!(
        factor.value.explanation.clamp.is_some(),
        "out-of-range raw must record a clamp audit"
    );
}

// ── Cross-sectional gating ────────────────────────────────────────────────────

#[test]
fn compute_all_batch_rejects_mixed_as_of() {
    let config = factors_config(&["liquidity"], MissingFactorPolicy::ZeroWeight, "0.10");
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

#[test]
fn cross_sectional_rank_requires_batch() {
    // `liquidity_depth` is Rank (cross-sectional); the single-market path must
    // refuse it rather than fabricate a pseudo cross-section.
    let config = factors_config(&["liquidity"], MissingFactorPolicy::ZeroWeight, "0.10");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vector = make_vector(
        "0xrank",
        &[("book.visible_liquidity_usd", usd(10_000))],
        DataQualityStatus::Fresh,
        0,
        Utc::now(),
    );
    let error = engine
        .compute_all(&vector, &config)
        .expect_err("must require batch");
    assert!(
        error.to_string().contains("requires the batch"),
        "expected a RequiresBatch error, got: {error}"
    );
}

// ── Explanation drivers ───────────────────────────────────────────────────────

#[test]
fn factor_explanation_lists_positive_and_negative_drivers() {
    // The data-quality factor blends a positive base with negative penalties.
    let config = factors_config(&["data_quality"], MissingFactorPolicy::ZeroWeight, "0.10");
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
    // present but does not contribute, and the market still proceeds.
    let config = factors_config(&["microstructure"], MissingFactorPolicy::ZeroWeight, "0.50");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vector = make_vector(
        "0xfloor",
        &[("book.depth_imbalance", dec(Decimal::new(2, 1)))],
        DataQualityStatus::Stale,
        0,
        Utc::now(),
    );
    let outcome = engine.compute_all(&vector, &config).expect("compute");
    assert!(outcome.eligibility.is_eligible());
    let factor = scored(&outcome, "book_imbalance");
    assert!(factor.below_confidence_floor);
    assert!(
        !factor.contributes,
        "below-floor factor must not contribute"
    );
}

#[test]
fn factor_missing_reject_candidate_policy() {
    // `spread_efficiency` is required (quality-gated); a market missing it under
    // RejectCandidate is excluded, while a complete market proceeds.
    let config = factors_config(&["liquidity"], MissingFactorPolicy::RejectCandidate, "0.50");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let as_of = Utc::now();
    let complete = make_vector(
        "0xcomplete",
        &[
            ("book.visible_liquidity_usd", usd(20_000)),
            ("book.spread_bps", bps(120)),
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
        .compute_all_batch(&[complete, missing_spread], &config)
        .expect("batch compute");
    assert!(
        outcomes[0].eligibility.is_eligible(),
        "complete market eligible"
    );
    assert!(
        matches!(
            outcomes[1].eligibility,
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
        &factors_config(&["liquidity"], MissingFactorPolicy::ZeroWeight, "0.50"),
        &features,
    );
    let two = FactorEngine::new(
        &factors_config(
            &["liquidity", "momentum"],
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
    // The same enabled set hashes identically regardless of config list order.
    let features = FeaturesConfig::default();
    let forward = FactorEngine::new(
        &factors_config(
            &["liquidity", "momentum"],
            MissingFactorPolicy::ZeroWeight,
            "0.50",
        ),
        &features,
    );
    let reversed = FactorEngine::new(
        &factors_config(
            &["momentum", "liquidity"],
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

// ── Nine generic factors: basic compute ───────────────────────────────────────

#[test]
fn liquidity_depth_factor_basic_compute() {
    let factor = compute_one(
        "liquidity",
        "liquidity_depth",
        &[("book.visible_liquidity_usd", usd(25_000))],
        &[("book.visible_liquidity_usd", usd(5_000))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn spread_efficiency_factor_basic_compute() {
    let factor = compute_one(
        "liquidity",
        "spread_efficiency",
        &[("book.spread_bps", bps(80))],
        &[("book.spread_bps", bps(400))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn book_imbalance_factor_basic_compute() {
    let factor = compute_one(
        "microstructure",
        "book_imbalance",
        &[("book.depth_imbalance", dec(Decimal::new(3, 1)))],
        &[("book.depth_imbalance", dec(Decimal::new(-2, 1)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn momentum_factor_basic_compute() {
    let factor = compute_one(
        "momentum",
        "momentum",
        &[("ts.momentum_300s", dec(Decimal::new(5, 2)))],
        &[("ts.momentum_300s", dec(Decimal::new(-3, 2)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn mean_reversion_factor_basic_compute() {
    let factor = compute_one(
        "mean_reversion",
        "mean_reversion",
        &[("ts.price_reversal", dec(Decimal::new(4, 2)))],
        &[("ts.price_reversal", dec(Decimal::new(-1, 2)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn volatility_regime_factor_basic_compute() {
    let factor = compute_one(
        "volatility",
        "volatility_regime",
        &[("ts.realized_vol_900s", dec(Decimal::new(8, 2)))],
        &[("ts.realized_vol_900s", dec(Decimal::new(2, 2)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn market_activity_factor_basic_compute() {
    let factor = compute_one(
        "activity",
        "market_activity",
        &[("micro.quote_update_rate", dec(Decimal::from(3)))],
        &[("micro.quote_update_rate", dec(Decimal::new(5, 1)))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn time_to_resolution_factor_basic_compute() {
    let factor = compute_one(
        "resolution",
        "time_to_resolution",
        &[("market.time_to_resolution_secs", count(172_800))],
        &[("market.time_to_resolution_secs", count(3_600))],
    );
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

#[test]
fn data_quality_factor_basic_compute() {
    let factor = compute_one("data_quality", "data_quality", &[], &[]);
    assert!(factor.value.raw_value.is_some());
    assert!(in_unit(factor.value.normalized_score));
}

// ── Parallel batch: determinism, order, serial/parallel equivalence ───────────

/// A batch of distinct markets with varied liquidity and momentum, sized past
/// `PARALLEL_MIN_MARKETS` so [`FactorEngine::compute_all_batch`] takes the rayon
/// path and the cross-section (`Rank` / `ZScore`) has real spread.
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
                    ("ts.momentum_300s", dec(Decimal::new(step - 12, 2))),
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
    // The parallel path must produce a byte-identical result run to run: pure
    // computers + quantized normalization mean scheduling cannot perturb output.
    let config = factors_config(&["liquidity"], MissingFactorPolicy::ZeroWeight, "0.10");
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
    // `outcomes[i]` must describe `features[i]` so downstream id alignment (the
    // feature-vector foreign key) holds under parallel evaluation.
    let config = factors_config(&["liquidity"], MissingFactorPolicy::ZeroWeight, "0.10");
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(20);
    let outcomes = engine
        .compute_all_batch(&vectors, &config)
        .expect("batch compute");
    assert_eq!(outcomes.len(), vectors.len());
    for (vector, outcome) in vectors.iter().zip(&outcomes) {
        assert_eq!(
            vector.market_id, outcome.market_id,
            "outcome order must match input order"
        );
    }
}

#[test]
fn compute_all_batch_serial_and_parallel_agree() {
    // Same inputs, both code paths: the rayon path must be bit-identical to the
    // serial path, including the cross-sectional Rank / ZScore columns.
    let config = factors_config(
        &["liquidity", "momentum"],
        MissingFactorPolicy::ZeroWeight,
        "0.10",
    );
    let features = FeaturesConfig::default();
    let engine = FactorEngine::new(&config, &features);
    let vectors = varied_batch(24);
    let serial = engine
        .compute_all_batch_inner(&vectors, &config, false)
        .expect("serial path");
    let parallel = engine
        .compute_all_batch_inner(&vectors, &config, true)
        .expect("parallel path");
    assert_eq!(serial, parallel, "serial and parallel paths must agree");
}
