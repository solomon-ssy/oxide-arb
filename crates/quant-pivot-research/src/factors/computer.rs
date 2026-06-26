//! The [`FactorComputer`] contract and the [`FactorEngine`] that drives it.
//!
//! A [`FactorComputer`] is a pure, per-market function producing a [`RawFactor`]
//! (no normalization, no cross-section). The [`FactorEngine`] owns the governed
//! registry, applies the (possibly cross-sectional) [`NormalizationSpec`], and
//! resolves the runtime confidence floor / missing-factor policy into a
//! [`MarketFactorOutcome`]. Cross-sectional normalization is only valid through
//! the batch interface — the single-market path refuses to fabricate one.

use std::{str::FromStr, sync::Arc};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::{FactorsConfig, FeaturesConfig, MissingFactorPolicy},
    types::{ContentHash, FactorDefinitionId, Probability},
};
use rust_decimal::Decimal;

use crate::{
    factors::{
        normalize::{Normalized, normalize_column},
        registry::FactorRegistry,
        value::{
            FactorDefinitionSpec, FactorEligibility, FactorExplanation, FactorSet, FactorValue,
            MarketFactorOutcome, RawFactor, ScoredFactor,
        },
    },
    features::FeatureVector,
    hashing::ResearchHasher,
    parallel::{par_map_with_index, par_try_map},
};

/// Batch size at or above which the engine spreads its three phases across the
/// `rayon` pool. Below it the serial path wins: parallel scheduling adds fixed
/// overhead a tiny batch never amortizes. Tunable via the factor benchmarks.
const PARALLEL_MIN_MARKETS: usize = 16;

/// One enabled factor: its governed spec paired with the computer that produces
/// it. Built once per round by [`FactorRegistry`].
type FactorEntry = (FactorDefinitionSpec, Arc<dyn FactorComputer>);

/// Computes one factor's raw value from a feature vector.
///
/// `compute_raw` is synchronous and side-effect free: factors are pure functions
/// of their inputs. Normalization (including cross-sectional `ZScore` / `Rank`)
/// is the [`FactorEngine`]'s responsibility, never the computer's.
pub trait FactorComputer: Send + Sync {
    /// Governing factor-definition id.
    fn definition_id(&self) -> FactorDefinitionId;

    /// The governed specification this computer implements.
    fn spec(&self) -> &FactorDefinitionSpec;

    /// Compute the raw (un-normalized) factor for a single feature vector.
    ///
    /// # Errors
    ///
    /// Returns an error only on an irrecoverable computation failure; a missing
    /// input is modeled as [`RawFactor`] with `raw_value = None`, not an error.
    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor>;
}

/// The factor engine: a governed registry plus the normalization + policy logic
/// that turns raw factors into explainable [`MarketFactorOutcome`]s.
pub struct FactorEngine {
    registry: FactorRegistry,
}

impl FactorEngine {
    /// Build the engine from frozen factor + feature config.
    ///
    /// `factors` selects the enabled families; `features` resolves windowed
    /// factor inputs (e.g. the momentum / volatility window) against the active
    /// feature schema, so a factor's declared input always matches a real feature
    /// name.
    #[must_use]
    pub fn new(factors: &FactorsConfig, features: &FeaturesConfig) -> Self {
        Self {
            registry: FactorRegistry::build(factors, features),
        }
    }

    /// The governed registry backing this engine.
    #[must_use]
    pub const fn registry(&self) -> &FactorRegistry {
        &self.registry
    }

    /// The governed factor set (for `factor_schema_hash` binding).
    #[must_use]
    pub fn factor_set(&self) -> FactorSet {
        self.registry.factor_set()
    }

    /// The canonical `factor_schema_hash` of the enabled factor set.
    ///
    /// # Errors
    ///
    /// Propagates canonical-hash serialization failures.
    pub fn factor_schema_hash(&self) -> QuantResult<ContentHash> {
        ResearchHasher::factor_schema(&self.factor_set())
    }

    /// Compute every enabled factor for a single market.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::FactorRequiresBatch`] if any enabled factor uses
    /// a cross-sectional normalization (those require [`Self::compute_all_batch`]);
    /// otherwise propagates raw-computation / config-parse failures.
    pub fn compute_all(
        &self,
        features: &FeatureVector,
        config: &FactorsConfig,
    ) -> QuantResult<MarketFactorOutcome> {
        if let Some((spec, _)) = self
            .registry
            .factors()
            .iter()
            .find(|(spec, _)| spec.normalization.is_cross_sectional())
        {
            return Err(ResearchError::FactorRequiresBatch {
                factor: spec.name.as_str().to_owned(),
            }
            .into());
        }
        let mut outcomes = self.compute_all_batch(std::slice::from_ref(features), config)?;
        outcomes.pop().ok_or_else(|| {
            ResearchError::FactorComputation {
                detail: "factor engine produced no outcome".into(),
            }
            .into()
        })
    }

    /// Compute every enabled factor for a batch of markets at one decision time.
    ///
    /// Cross-sectional normalization (`ZScore` / `Rank`) is resolved across the
    /// batch column. The confidence floor and `missing_factor_policy` are applied
    /// per market to derive each [`ScoredFactor`]'s transient contribution flag
    /// and the market-level [`FactorEligibility`].
    ///
    /// # Errors
    ///
    /// Propagates raw-computation failures, an invalid `min_factor_confidence`, a
    /// non-uniform batch `as_of`, or an empty enabled factor registry.
    pub fn compute_all_batch(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        if self.registry.is_empty() {
            return Err(ResearchError::FactorComputation {
                detail: "no factors enabled in the governed registry".to_owned(),
            }
            .into());
        }
        Self::validate_batch_invariants(features)?;
        let parallel = features.len() >= PARALLEL_MIN_MARKETS;
        self.compute_all_batch_inner(features, config, parallel)
    }

    /// Validate batch preconditions shared by the pipeline and the engine.
    ///
    /// Cross-sectional normalization requires every vector in the batch to share
    /// the same decision timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::FactorComputation`] when `as_of` values differ.
    pub fn validate_batch_invariants(features: &[FeatureVector]) -> QuantResult<()> {
        if features.len() <= 1 {
            return Ok(());
        }
        let expected = features[0].as_of;
        for vector in features.iter().skip(1) {
            if vector.as_of != expected {
                return Err(ResearchError::FactorComputation {
                    detail: format!(
                        "batch as_of mismatch: expected {expected}, got {} for market {}",
                        vector.as_of, vector.market_id
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    /// The three-phase batch core with an explicit parallel toggle.
    ///
    /// Production always routes through [`Self::compute_all_batch`], which picks
    /// the path by batch size; this seam exists so the factor test suite can
    /// assert the serial and `rayon` paths are bit-identical on the same inputs.
    ///
    /// Determinism: every [`FactorComputer::compute_raw`] is a pure function of
    /// one vector and normalization is quantized, so parallel scheduling only
    /// reorders evaluation — the output stays index-aligned with `features`
    /// (`outcomes[i]` describes `features[i]`). Phase B always normalizes a full
    /// `M`-length column, so cross-sectional `ZScore` / `Rank` see the same
    /// selection regardless of the toggle.
    pub(in crate::factors) fn compute_all_batch_inner(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
        parallel: bool,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        let floor = parse_floor(config)?;
        let policy = config.missing_factor_policy;
        let factors = self.registry.factors();

        let raw_by_market = build_raw_by_market(factors, features, parallel)?;
        let norm_grid = build_norm_grid(factors, &raw_by_market, parallel);
        Ok(assemble_outcomes(
            features,
            factors,
            &raw_by_market,
            &norm_grid,
            floor,
            policy,
            parallel,
        ))
    }
}

/// Phase A — the raw factor grid, market-major (`raw[market][factor]`).
///
/// Each [`FactorComputer::compute_raw`] is a pure function of one vector, so the
/// parallel path (over markets) yields the same ordered grid as the serial path
/// and fails fast on the first computation error.
fn build_raw_by_market(
    factors: &[FactorEntry],
    features: &[FeatureVector],
    parallel: bool,
) -> QuantResult<Vec<Vec<RawFactor>>> {
    let compute_row = |vector: &FeatureVector| -> QuantResult<Vec<RawFactor>> {
        factors
            .iter()
            .map(|(_, computer)| computer.compute_raw(vector))
            .collect()
    };
    if parallel {
        par_try_map(features, compute_row)
    } else {
        features.iter().map(compute_row).collect()
    }
}

/// Phase B — normalize each factor's full column, factor-major
/// (`norm[factor][market]`).
///
/// Columns are normalized independently, so factors parallelize cleanly; each
/// column stays whole, preserving cross-sectional `ZScore` / `Rank`.
fn build_norm_grid(
    factors: &[FactorEntry],
    raw_by_market: &[Vec<RawFactor>],
    parallel: bool,
) -> Vec<Vec<Option<Normalized>>> {
    let normalize_factor = |index: usize| -> Vec<Option<Normalized>> {
        let raws: Vec<Option<Decimal>> = raw_by_market
            .iter()
            .map(|row| row[index].raw_value)
            .collect();
        normalize_column(&factors[index].0.normalization, &raws)
    };
    if parallel {
        par_map_with_index(factors, |index, _| normalize_factor(index))
    } else {
        (0..factors.len()).map(normalize_factor).collect()
    }
}

/// Phase C — assemble one [`MarketFactorOutcome`] per market, preserving input
/// order.
///
/// Markets are independent, so the parallel path (over markets) matches the
/// serial path element-for-element.
fn assemble_outcomes(
    features: &[FeatureVector],
    factors: &[FactorEntry],
    raw_by_market: &[Vec<RawFactor>],
    norm_grid: &[Vec<Option<Normalized>>],
    floor: Decimal,
    policy: MissingFactorPolicy,
    parallel: bool,
) -> Vec<MarketFactorOutcome> {
    let assemble_one = |market: usize, vector: &FeatureVector| -> MarketFactorOutcome {
        assemble_market(
            vector,
            factors,
            &raw_by_market[market],
            norm_grid,
            market,
            floor,
            policy,
        )
    };
    if parallel {
        par_map_with_index(features, assemble_one)
    } else {
        features
            .iter()
            .enumerate()
            .map(|(market, vector)| assemble_one(market, vector))
            .collect()
    }
}

/// Assemble one market's outcome from its raw row and the normalized columns,
/// deriving each factor's transient scoring eligibility and the market-level
/// verdict (the first required factor that is missing or below the floor rejects
/// the market under `RejectCandidate`).
fn assemble_market(
    vector: &FeatureVector,
    factors: &[FactorEntry],
    raw_row: &[RawFactor],
    norm_grid: &[Vec<Option<Normalized>>],
    market: usize,
    floor: Decimal,
    policy: MissingFactorPolicy,
) -> MarketFactorOutcome {
    let mut scored = Vec::with_capacity(factors.len());
    let mut reject: Option<String> = None;
    for (index, (spec, _)) in factors.iter().enumerate() {
        let raw = &raw_row[index];
        let normalized = norm_grid[index][market].as_ref();
        let scored_factor = assemble(raw, normalized, floor);
        if reject.is_none()
            && spec.is_required()
            && policy == MissingFactorPolicy::RejectCandidate
            && (raw.raw_value.is_none() || scored_factor.below_confidence_floor)
        {
            reject = Some(reject_reason(raw, floor));
        }
        scored.push(scored_factor);
    }
    let eligibility = reject.map_or(FactorEligibility::Eligible, |reason| {
        FactorEligibility::RejectCandidate { reason }
    });
    MarketFactorOutcome {
        market_id: vector.market_id.clone(),
        as_of: vector.as_of,
        eligibility,
        factors: scored,
    }
}

/// Build a [`ScoredFactor`] from a raw factor and its (optional) normalization.
///
/// A missing factor (`raw_value = None`, hence no normalization) carries a
/// neutral placeholder score with `confidence = 0`, so it can never contribute
/// downstream (`weight × score × confidence = 0`) yet remains a complete,
/// auditable row.
fn assemble(raw: &RawFactor, normalized: Option<&Normalized>, floor: Decimal) -> ScoredFactor {
    let present = raw.raw_value.is_some();
    let below_confidence_floor = raw.confidence.inner() < floor;
    let (normalized_score, clamp) = normalized.map_or_else(
        || (Probability::new(Decimal::new(5, 1)), None),
        |value| (value.score, value.clamp.clone()),
    );
    let value = FactorValue {
        definition_id: raw.definition_id.clone(),
        name: raw.name.clone(),
        family: raw.family,
        raw_value: raw.raw_value,
        normalized_score,
        direction: raw.direction,
        confidence: raw.confidence,
        explanation: FactorExplanation {
            headline: raw.headline.clone(),
            drivers: raw.drivers.clone(),
            clamp,
        },
        input_feature_refs: raw.input_feature_refs.clone(),
    };
    ScoredFactor {
        value,
        contributes: present && !below_confidence_floor,
        below_confidence_floor,
    }
}

/// The market-rejection reason for a required factor that is missing or below
/// the confidence floor under `RejectCandidate`.
fn reject_reason(raw: &RawFactor, floor: Decimal) -> String {
    if raw.raw_value.is_none() {
        format!("required factor `{}` missing", raw.name)
    } else {
        format!(
            "required factor `{}` confidence {} below floor {floor}",
            raw.name,
            raw.confidence.inner()
        )
    }
}

/// Parse the runtime confidence floor, failing closed on a malformed value.
fn parse_floor(config: &FactorsConfig) -> QuantResult<Decimal> {
    let raw = config.min_factor_confidence.value.trim();
    Decimal::from_str(raw)
        .map_err(|err| QuantError::config(format!("invalid min_factor_confidence `{raw}`: {err}")))
}
