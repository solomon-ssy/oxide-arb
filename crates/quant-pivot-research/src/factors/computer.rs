//! The [`FactorComputer`] contract and the [`FactorEngine`] that drives it.
//!
//! A [`FactorComputer`] is a pure, per-market function producing a [`RawFactor`]
//! (no normalization, no cross-section). The [`FactorEngine`] owns the governed
//! registry and runs two stages:
//!
//! 1. **raw stage** — every computer runs per market → a raw factor grid;
//! 2. **cross-section stage** — each factor column is normalized against the
//!    same-`as_of` cross-section (or, for a small cross-section, the configured
//!    [`SmallCrossSectionPolicy`]).
//!
//! There is no single-market pseudo cross-section and no silent neutral: a
//! too-small or degenerate cross-section yields
//! [`NormalizedFactor::Indeterminate`](crate::factors::normalize::NormalizedFactor).

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::factor::{FactorIndeterminateReason, NormalizationSource},
    runtime_config::{
        FactorCrossSectionConfig, FactorsConfig, FeaturesConfig, MissingFactorPolicy,
        SmallCrossSectionPolicy,
    },
    types::{ContentHash, FactorDefinitionId, Probability},
};
use rust_decimal::Decimal;

use crate::{
    factors::{
        normalize::{
            CrossSectionalNormalizer, NormalizedFactor, RawFactorColumn, indeterminate_present,
            resolve_normalizer,
        },
        registry::FactorRegistry,
        value::{
            FactorDefinitionSpec, FactorEligibility, FactorExplanation, FactorName, FactorSet,
            FactorValue, MarketFactorOutcome, RawFactor, ScoredFactor,
        },
    },
    features::FeatureVector,
    hashing::ResearchHasher,
    parallel::{par_map_with_index, par_try_map},
};

/// Batch size at or above which the engine spreads its stages across the `rayon`
/// pool. Below it the serial path wins: parallel scheduling adds fixed overhead
/// a tiny batch never amortizes. Tunable via the factor benchmarks.
const PARALLEL_MIN_MARKETS: usize = 16;

/// One enabled factor: its governed spec paired with the computer that produces
/// it. Built once per round by [`FactorRegistry`].
type FactorEntry = (FactorDefinitionSpec, Arc<dyn FactorComputer>);

/// Historical raw factor values, keyed by factor name.
///
/// Backs the [`SmallCrossSectionPolicy::HistoricalQuantile`] fallback; empty by
/// default (the [`SmallCrossSectionPolicy::Indeterminate`] policy needs none).
#[derive(Debug, Clone, Default)]
pub struct FactorHistory {
    by_factor: BTreeMap<FactorName, Vec<Decimal>>,
}

impl FactorHistory {
    /// An empty history (the default `Indeterminate` policy path).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Record a factor's historical raw values (rolling lookback distribution).
    pub fn insert(&mut self, factor: FactorName, values: Vec<Decimal>) {
        self.by_factor.insert(factor, values);
    }

    /// The historical raw values for a factor, if any were pre-fetched.
    #[must_use]
    fn values_for(&self, factor: &FactorName) -> Option<&[Decimal]> {
        self.by_factor.get(factor).map(Vec::as_slice)
    }
}

/// Computes one factor's raw value from a feature vector.
///
/// `compute_raw` is synchronous and side-effect free: factors are pure functions
/// of their inputs. Normalization (including the cross-section) is the
/// [`FactorEngine`]'s responsibility, never the computer's.
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
    /// factor inputs (e.g. the momentum / volatility windows) against the active
    /// feature schema, so a factor's declared input always matches a real feature
    /// name. Normalizers are resolved per compute call from
    /// `factors.normalization` so parameter tuning never re-keys the registry.
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
    /// Cross-sectional factors see a cross-section of one, so — unless the
    /// `HistoricalQuantile` policy supplies a distribution — they resolve to
    /// [`NormalizedFactor::Indeterminate`], never a fabricated neutral.
    ///
    /// # Errors
    ///
    /// Propagates raw-computation / config-parse failures.
    pub fn compute_all(
        &self,
        features: &FeatureVector,
        config: &FactorsConfig,
    ) -> QuantResult<MarketFactorOutcome> {
        let mut outcomes = self.compute_all_batch(std::slice::from_ref(features), config)?;
        outcomes.pop().ok_or_else(|| {
            ResearchError::FactorComputation {
                detail: "factor engine produced no outcome".into(),
            }
            .into()
        })
    }

    /// Compute every enabled factor for a batch of markets at one decision time,
    /// with the default (empty) history — the [`SmallCrossSectionPolicy::Indeterminate`]
    /// path.
    ///
    /// # Errors
    ///
    /// See [`Self::compute_all_batch_with_history`].
    pub fn compute_all_batch(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        self.compute_all_batch_with_history(features, config, &FactorHistory::empty())
    }

    /// Compute every enabled factor for a batch of markets, using `history` for
    /// the [`SmallCrossSectionPolicy::HistoricalQuantile`] fallback.
    ///
    /// Cross-sectional normalization is resolved across the batch column; the
    /// confidence floor and `missing_factor_policy` are applied per market to
    /// derive each [`ScoredFactor`]'s contribution flag and the market-level
    /// [`FactorEligibility`].
    ///
    /// # Errors
    ///
    /// Propagates raw-computation failures, an invalid `min_factor_confidence`,
    /// an unresolvable normalization config, a non-uniform batch `as_of`, or an
    /// empty enabled factor registry.
    pub fn compute_all_batch_with_history(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
        history: &FactorHistory,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        if self.registry.is_empty() {
            return Err(ResearchError::FactorComputation {
                detail: "no factors enabled in the governed registry".to_owned(),
            }
            .into());
        }
        Self::validate_batch_invariants(features)?;
        let parallel = features.len() >= PARALLEL_MIN_MARKETS;
        self.compute_all_batch_inner(features, config, history, parallel)
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

    /// The two-stage batch core with an explicit parallel toggle.
    ///
    /// Production always routes through [`Self::compute_all_batch_with_history`],
    /// which picks the path by batch size; this seam exists so the factor test
    /// suite can assert the serial and `rayon` paths are bit-identical.
    pub(in crate::factors) fn compute_all_batch_inner(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
        history: &FactorHistory,
        parallel: bool,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        let floor = parse_floor(config)?;
        let policy = config.missing_factor_policy;
        let factors = self.registry.factors();
        let normalizers = resolve_normalizers(factors, config)?;

        let raw_by_market = build_raw_by_market(factors, features, parallel)?;
        let norm_grid = build_norm_grid(
            factors,
            &normalizers,
            &raw_by_market,
            &config.cross_section,
            history,
            parallel,
        );
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

/// Resolve one normalizer per factor from the runtime normalization config.
fn resolve_normalizers(
    factors: &[FactorEntry],
    config: &FactorsConfig,
) -> QuantResult<Vec<Box<dyn CrossSectionalNormalizer>>> {
    factors
        .iter()
        .map(|(spec, _)| {
            resolve_normalizer(
                spec.name.as_str(),
                spec.normalization,
                &config.normalization,
            )
        })
        .collect()
}

/// Phase A — the raw factor grid, market-major (`raw[market][factor]`).
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
/// (`norm[factor][market]`), applying the small-cross-section policy per factor.
fn build_norm_grid(
    factors: &[FactorEntry],
    normalizers: &[Box<dyn CrossSectionalNormalizer>],
    raw_by_market: &[Vec<RawFactor>],
    cross_section: &FactorCrossSectionConfig,
    history: &FactorHistory,
    parallel: bool,
) -> Vec<Vec<NormalizedFactor>> {
    let normalize_factor = |index: usize| -> Vec<NormalizedFactor> {
        let column = RawFactorColumn {
            factor: factors[index].0.name.clone(),
            values: raw_by_market
                .iter()
                .map(|row| row[index].raw_value)
                .collect(),
        };
        normalize_one_factor(normalizers[index].as_ref(), &column, cross_section, history)
    };
    if parallel {
        par_map_with_index(factors, |index, _| normalize_factor(index))
    } else {
        (0..factors.len()).map(normalize_factor).collect()
    }
}

/// Normalize one factor's column, honoring the small-cross-section policy for
/// cross-sectional methods (per-market methods always apply directly).
fn normalize_one_factor(
    normalizer: &dyn CrossSectionalNormalizer,
    column: &RawFactorColumn,
    cross_section: &FactorCrossSectionConfig,
    history: &FactorHistory,
) -> Vec<NormalizedFactor> {
    if !normalizer.is_cross_sectional() {
        let stats = normalizer.fit(column);
        return normalizer.apply(column, &stats, NormalizationSource::PerMarket);
    }
    let min_size = usize::try_from(cross_section.min_size).unwrap_or(usize::MAX);
    if column.present_count() >= min_size {
        let stats = normalizer.fit(column);
        return normalizer.apply(column, &stats, NormalizationSource::CrossSection);
    }
    match cross_section.small_cross_section_policy {
        SmallCrossSectionPolicy::Indeterminate => {
            indeterminate_present(column, FactorIndeterminateReason::CrossSectionTooSmall)
        }
        SmallCrossSectionPolicy::HistoricalQuantile => match history.values_for(&column.factor) {
            Some(values) if values.len() >= min_size => {
                let historical = RawFactorColumn {
                    factor: column.factor.clone(),
                    values: values.iter().map(|value| Some(*value)).collect(),
                };
                let stats = normalizer.fit(&historical);
                normalizer.apply(column, &stats, NormalizationSource::HistoricalQuantile)
            }
            _ => indeterminate_present(column, FactorIndeterminateReason::NoHistory),
        },
    }
}

/// Phase C — assemble one [`MarketFactorOutcome`] per market, preserving order.
fn assemble_outcomes(
    features: &[FeatureVector],
    factors: &[FactorEntry],
    raw_by_market: &[Vec<RawFactor>],
    norm_grid: &[Vec<NormalizedFactor>],
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
/// verdict (the first required factor that is missing / indeterminate / below
/// the floor rejects the market under `RejectCandidate`).
fn assemble_market(
    vector: &FeatureVector,
    factors: &[FactorEntry],
    raw_row: &[RawFactor],
    norm_grid: &[Vec<NormalizedFactor>],
    market: usize,
    floor: Decimal,
    policy: MissingFactorPolicy,
) -> MarketFactorOutcome {
    let mut scored = Vec::with_capacity(factors.len());
    let mut reject: Option<String> = None;
    for (index, (spec, _)) in factors.iter().enumerate() {
        let raw = &raw_row[index];
        let normalized = norm_grid[index][market].clone();
        let scored_factor = assemble(raw, normalized, floor);
        if reject.is_none()
            && spec.is_required()
            && policy == MissingFactorPolicy::RejectCandidate
            && !scored_factor.contributes
        {
            reject = Some(reject_reason(&scored_factor, floor));
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

/// Build a [`ScoredFactor`] from a raw factor and its normalization outcome.
///
/// A factor contributes only when it carries a usable normalized score and its
/// confidence meets the floor. Missing inputs (`confidence = 0`) and
/// indeterminate cross-sections never contribute — and are never coerced to a
/// neutral placeholder score.
fn assemble(raw: &RawFactor, normalized: NormalizedFactor, floor: Decimal) -> ScoredFactor {
    let scored = matches!(normalized, NormalizedFactor::Scored { .. });
    // A factor with no usable normalized score reports zero confidence: a missing
    // input or an indeterminate cross-section is not "confident" about anything,
    // and a non-zero confidence sitting next to an indeterminate reason misleads
    // operators (and the persisted fact / report breakdown). Only a scored factor
    // carries its raw confidence forward.
    let confidence = if scored {
        raw.confidence
    } else {
        Probability::ZERO
    };
    let below_confidence_floor = confidence.inner() < floor;
    let value = FactorValue {
        definition_id: raw.definition_id.clone(),
        name: raw.name.clone(),
        family: raw.family,
        raw_value: raw.raw_value,
        normalization: normalized,
        direction: raw.direction,
        confidence,
        explanation: FactorExplanation {
            headline: raw.headline.clone(),
            drivers: raw.drivers.clone(),
        },
        input_feature_refs: raw.input_feature_refs.clone(),
    };
    ScoredFactor {
        value,
        contributes: scored && !below_confidence_floor,
        below_confidence_floor,
    }
}

/// The market-rejection reason for a required factor that did not contribute.
fn reject_reason(scored: &ScoredFactor, floor: Decimal) -> String {
    let value = &scored.value;
    match &value.normalization {
        NormalizedFactor::MissingInput => format!("required factor `{}` missing", value.name),
        NormalizedFactor::Indeterminate { reason } => {
            format!("required factor `{}` indeterminate ({reason})", value.name)
        }
        NormalizedFactor::Scored { .. } => format!(
            "required factor `{}` confidence {} below floor {floor}",
            value.name,
            value.confidence.inner()
        ),
    }
}

/// Parse the runtime confidence floor, failing closed on a malformed value.
fn parse_floor(config: &FactorsConfig) -> QuantResult<Decimal> {
    let raw = config.min_factor_confidence.value.trim();
    Decimal::from_str(raw)
        .map_err(|err| QuantError::config(format!("invalid min_factor_confidence `{raw}`: {err}")))
}
