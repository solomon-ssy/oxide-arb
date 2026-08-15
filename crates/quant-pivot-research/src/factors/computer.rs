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

use std::{slice, sync::Arc};

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::FactorDirection,
    enums::{
        common::MarketCategory,
        factor::{FactorIndeterminateReason, NormalizationSource},
    },
    runtime_config::{
        DomainConfig, FactorCrossSectionConfig, FactorsConfig, FeaturesConfig,
        SmallCrossSectionPolicy,
    },
    types::{
        ContentHash, FactorDefinitionId, Probability, ResearchFeatureContract,
        factor::{FACTOR_VALUE_OUTPUT_SCHEMA_VERSION, FactorDefinitionRef, FactorServingPlane},
    },
};
use rust_decimal::Decimal;

use crate::{
    factors::{
        normalize::{
            CrossSectionalNormalizer, NormalizedFactor, RawFactorColumn, indeterminate_present,
            resolve_normalizer,
        },
        reference::FrozenReferenceQuantiles,
        registry::FactorRegistry,
        value::{
            FactorDefinitionDocument, FactorEligibility, FactorExplanation, FactorName,
            FactorValue, MarketFactorOutcome, RawFactor, RawFactorEligibility, ScoredFactor,
        },
    },
    features::{ExecutableFeatureSchema, FeatureVector},
    hashing::ResearchHasher,
    model::FavoriteLongshotBiasTable,
    parallel::{par_map_with_index, par_try_map, par_try_map_index},
};

/// Batch size at or above which the engine spreads its stages across the `rayon`
/// pool. Below it the serial path wins: parallel scheduling adds fixed overhead
/// a tiny batch never amortizes. Tunable via the factor benchmarks.
const PARALLEL_MIN_MARKETS: usize = 16;

/// One enabled factor: its governed spec paired with the computer that produces
/// it. Built once per round by [`FactorRegistry`].
type FactorEntry = (FactorDefinitionDocument, Arc<dyn FactorComputer>);

/// Computes one factor's raw value from a feature vector.
///
/// `compute_raw` is synchronous and side-effect free: factors are pure functions
/// of their inputs. Normalization (including the cross-section) is the
/// [`FactorEngine`]'s responsibility, never the computer's.
pub trait FactorComputer: Send + Sync {
    /// The governed specification this computer implements.
    fn spec(&self) -> &FactorDefinitionDocument;

    /// Compute the raw (un-normalized) factor for a single feature vector.
    ///
    /// # Errors
    ///
    /// Returns an error only on an irrecoverable computation failure; a missing
    /// input is modeled as [`RawFactor`] with `raw_value = None`, not an error.
    fn compute_raw(
        &self,
        definition_id: FactorDefinitionId,
        features: &FeatureVector,
    ) -> QuantResult<RawFactor>;
}

struct FactorEngineContract {
    feature_contract_hash: ContentHash,
    serving_plane: FactorServingPlane,
}

/// The factor engine: a governed registry plus the normalization + policy logic
/// that turns raw factors into explainable [`MarketFactorOutcome`]s.
pub struct FactorEngine {
    registry: FactorRegistry,
    contract: Result<FactorEngineContract, String>,
}

impl FactorEngine {
    /// Build the engine from frozen factor + feature + domain config.
    ///
    /// `factors` selects the enabled families; `features` resolves windowed
    /// factor inputs (e.g. the momentum / volatility windows) against the active
    /// feature schema, so a factor's declared input always matches a real feature
    /// name; `domain` gates the category-routed vertical factors.
    /// Normalizers are resolved per compute call from `factors.normalization`
    /// so parameter tuning never re-keys the registry.
    ///
    /// `bias_table` binds the favorite-longshot factor; `None` keeps it inert.
    /// The table is runtime data whose artifact commitment is owned by the
    /// model-serving contract rather than the factor plane.
    #[must_use]
    pub fn new(
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> Self {
        Self::from_registry(
            FactorRegistry::build(factors, features, domain, bias_table),
            features,
            ResearchFeatureContract::FullL2,
        )
    }

    /// Build the exact factor engine for one immutable `ResearchProfile` scope.
    ///
    /// `None` is an explicit pooled scope and excludes all domain factors.
    /// Domain-mapped categories include only their own vertical's revisions.
    #[must_use]
    pub fn for_model_scope(
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        feature_contract: ResearchFeatureContract,
        category_scope: Option<MarketCategory>,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> Self {
        Self::from_registry(
            FactorRegistry::for_model_scope(
                factors,
                features,
                domain,
                feature_contract,
                category_scope,
                bias_table,
            ),
            features,
            feature_contract,
        )
    }

    fn from_registry(
        registry: FactorRegistry,
        features: &FeaturesConfig,
        feature_contract: ResearchFeatureContract,
    ) -> Self {
        let contract = (|| {
            let feature_schema = ExecutableFeatureSchema::build(features, feature_contract)?;
            let feature_contract_hash = ResearchHasher::feature_schema(&feature_schema)?;
            let definitions = registry
                .factors()
                .iter()
                .map(|(spec, _)| {
                    FactorDefinitionRef::try_seal(
                        spec.clone(),
                        feature_contract_hash,
                        feature_schema.version(),
                        FACTOR_VALUE_OUTPUT_SCHEMA_VERSION,
                    )
                    .map_err(|error| {
                        ResearchError::FactorComputation {
                            detail: format!(
                                "seal factor definition revision `{}`: {error}",
                                spec.name
                            ),
                        }
                        .into()
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?;
            let serving_plane = FactorServingPlane::try_seal(definitions).map_err(|error| {
                ResearchError::FactorComputation {
                    detail: format!("seal factor serving plane: {error}"),
                }
            })?;
            QuantResult::Ok(FactorEngineContract {
                feature_contract_hash,
                serving_plane,
            })
        })()
        .map_err(|error| error.to_string());
        Self { registry, contract }
    }

    /// The governed registry backing this engine.
    #[must_use]
    pub const fn registry(&self) -> &FactorRegistry {
        &self.registry
    }

    /// Canonical feature contract bound into every enabled factor revision.
    ///
    /// # Errors
    ///
    /// Returns the schema/hash construction failure captured at engine build.
    pub fn feature_contract_hash(&self) -> QuantResult<ContentHash> {
        self.contract
            .as_ref()
            .map(|contract| contract.feature_contract_hash)
            .map_err(|detail| {
                ResearchError::FactorComputation {
                    detail: format!("factor serving contract construction failed: {detail}"),
                }
                .into()
            })
    }

    /// Exact canonical factor plane frozen by this engine.
    ///
    /// # Errors
    ///
    /// Returns the revision/plane construction failure captured at engine build.
    pub fn serving_plane(&self) -> QuantResult<&FactorServingPlane> {
        self.contract
            .as_ref()
            .map(|contract| &contract.serving_plane)
            .map_err(|detail| {
                ResearchError::FactorComputation {
                    detail: format!("factor serving contract construction failed: {detail}"),
                }
                .into()
            })
    }

    /// Exact content-addressed revision for one enabled logical factor.
    ///
    /// # Errors
    ///
    /// Returns a contract-construction error or an unknown factor error.
    pub fn definition_ref(&self, factor: &FactorName) -> QuantResult<&FactorDefinitionRef> {
        self.serving_plane()?
            .definitions()
            .iter()
            .find(|definition| definition.factor_name() == factor)
            .ok_or_else(|| {
                ResearchError::FactorComputation {
                    detail: format!("factor `{factor}` is absent from the governed registry"),
                }
                .into()
            })
    }

    /// Compute every enabled factor for a single market.
    ///
    /// Cross-sectional factors see a cross-section of one, so — unless the
    /// `FrozenReferenceQuantile` policy supplies a training CDF — they resolve to
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
        let mut outcomes = self.compute_all_batch(slice::from_ref(features), config)?;
        outcomes.pop().ok_or_else(|| {
            ResearchError::FactorComputation {
                detail: "factor engine produced no outcome".into(),
            }
            .into()
        })
    }

    /// Compute every enabled factor for a batch of markets at one decision time,
    /// with no artifact reference — the [`SmallCrossSectionPolicy::Indeterminate`]
    /// path. Configuring `FrozenReferenceQuantile` without calling
    /// [`Self::compute_batch_with_refs`] fails closed per factor.
    ///
    /// # Errors
    ///
    /// See [`Self::compute_batch_with_refs`].
    pub fn compute_all_batch(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        self.compute_batch_with_refs(features, config, &FrozenReferenceQuantiles::empty())
    }

    /// Compute every enabled factor for a batch of markets, using the loaded
    /// model artifact's reference CDFs for the
    /// [`SmallCrossSectionPolicy::FrozenReferenceQuantile`] fallback.
    ///
    /// Cross-sectional normalization is resolved across the batch column; the
    /// confidence floor and each frozen factor definition's requiredness are
    /// applied per market to derive each [`ScoredFactor`]'s contribution flag
    /// and the market-level [`FactorEligibility`].
    ///
    /// # Errors
    ///
    /// Propagates raw-computation failures, an invalid `min_factor_confidence`,
    /// an unresolvable normalization config, a non-uniform batch `as_of`, or an
    /// empty enabled factor registry.
    pub fn compute_batch_with_refs(
        &self,
        features: &[FeatureVector],
        config: &FactorsConfig,
        references: &FrozenReferenceQuantiles,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        if self.registry.is_empty() {
            return Err(ResearchError::FactorComputation {
                detail: "no factors enabled in the governed registry".to_owned(),
            }
            .into());
        }
        Self::validate_batch_invariants(features)?;
        let parallel = features.len() >= PARALLEL_MIN_MARKETS;
        self.compute_all_batch_inner(features, config, references, parallel)
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
        let expected = features[0].decision_at;
        for vector in features.iter().skip(1) {
            if vector.decision_at != expected {
                return Err(ResearchError::FactorComputation {
                    detail: format!(
                        "batch as_of mismatch: expected {expected}, got {} for market {}",
                        vector.decision_at, vector.market_id
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
        references: &FrozenReferenceQuantiles,
        parallel: bool,
    ) -> QuantResult<Vec<MarketFactorOutcome>> {
        let floor = config.min_factor_confidence.value;
        let factors = self.registry.factors();
        let definitions: Vec<&FactorDefinitionRef> = factors
            .iter()
            .map(|(spec, _)| self.definition_ref(&spec.name))
            .collect::<QuantResult<_>>()?;
        let normalizers = resolve_normalizers(factors, config)?;
        let min_cross_section_size =
            usize::try_from(config.cross_section.min_size).map_err(|error| {
                ResearchError::FactorComputation {
                    detail: format!("factor cross-section min_size conversion failed: {error}"),
                }
            })?;

        let raw_by_market = build_raw_by_market(factors, &definitions, features, parallel)?;
        let norm_grid = build_norm_grid(
            factors,
            &normalizers,
            &raw_by_market,
            &config.cross_section,
            references,
            min_cross_section_size,
            parallel,
        )?;
        Ok(assemble_outcomes(
            features,
            &definitions,
            &raw_by_market,
            &norm_grid,
            floor,
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

/// Stage A — the raw factor grid, market-major (`raw[market][factor]`).
fn build_raw_by_market(
    factors: &[FactorEntry],
    definitions: &[&FactorDefinitionRef],
    features: &[FeatureVector],
    parallel: bool,
) -> QuantResult<Vec<Vec<RawFactor>>> {
    let compute_row = |vector: &FeatureVector| -> QuantResult<Vec<RawFactor>> {
        factors
            .iter()
            .zip(definitions)
            .map(|((_, computer), definition)| {
                let mut raw = computer.compute_raw(definition.factor_definition_id(), vector)?;
                raw.canonicalize_against(definition)?;
                Ok(raw)
            })
            .collect()
    };
    if parallel {
        par_try_map(features, compute_row)
    } else {
        features.iter().map(compute_row).collect()
    }
}

/// Stage B — normalize each factor's full column, factor-major
/// (`norm[factor][market]`), applying the small-cross-section policy per factor.
fn build_norm_grid(
    factors: &[FactorEntry],
    normalizers: &[Box<dyn CrossSectionalNormalizer>],
    raw_by_market: &[Vec<RawFactor>],
    cross_section: &FactorCrossSectionConfig,
    references: &FrozenReferenceQuantiles,
    min_size: usize,
    parallel: bool,
) -> QuantResult<Vec<Vec<NormalizedFactor>>> {
    let normalize_factor = |index: usize| -> QuantResult<Vec<NormalizedFactor>> {
        let definition = &factors[index].0;
        let column = RawFactorColumn {
            factor: definition.name.clone(),
            values: raw_by_market
                .iter()
                .map(|row| {
                    row[index]
                        .raw_value
                        .map(|raw| {
                            definition.normalization_input(raw).ok_or_else(|| {
                                ResearchError::FactorComputation {
                                    detail: format!(
                                        "factor `{}` emitted negative raw value {raw} for unsigned output",
                                        definition.name
                                    ),
                                }
                                .into()
                            })
                        })
                        .transpose()
                })
                .collect::<QuantResult<Vec<_>>>()?,
        };
        normalize_one_factor(
            normalizers[index].as_ref(),
            &column,
            cross_section,
            references,
            min_size,
        )
    };
    if parallel {
        par_try_map_index(factors, |index, _| normalize_factor(index))
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
    references: &FrozenReferenceQuantiles,
    min_size: usize,
) -> QuantResult<Vec<NormalizedFactor>> {
    if !normalizer.is_cross_sectional() {
        let stats = normalizer.fit(column)?;
        return Ok(normalizer.apply(column, &stats, NormalizationSource::PerMarket));
    }
    if column.present_count() >= min_size {
        let stats = normalizer.fit(column)?;
        return Ok(normalizer.apply(column, &stats, NormalizationSource::CrossSection));
    }
    Ok(match cross_section.small_cross_section_policy {
        SmallCrossSectionPolicy::Indeterminate => {
            indeterminate_present(column, FactorIndeterminateReason::CrossSectionTooSmall)
        }
        SmallCrossSectionPolicy::FrozenReferenceQuantile => {
            let Some(reference) = references
                .get(&column.factor)
                .filter(|reference| reference.sample_count() >= min_size)
            else {
                return Ok(indeterminate_present(
                    column,
                    FactorIndeterminateReason::NoFrozenReference,
                ));
            };
            column
                .values
                .iter()
                .map(|value| match value {
                    None => Ok(NormalizedFactor::MissingInput),
                    Some(raw) => Ok(NormalizedFactor::Scored {
                        score: reference.percentile(*raw)?,
                        source: NormalizationSource::FrozenReferenceQuantile,
                        clamp: None,
                    }),
                })
                .collect::<QuantResult<Vec<_>>>()?
        }
    })
}

/// Stage C — assemble one [`MarketFactorOutcome`] per market, preserving order.
fn assemble_outcomes(
    features: &[FeatureVector],
    definitions: &[&FactorDefinitionRef],
    raw_by_market: &[Vec<RawFactor>],
    norm_grid: &[Vec<NormalizedFactor>],
    floor: Decimal,
    parallel: bool,
) -> Vec<MarketFactorOutcome> {
    let assemble_one = |market: usize, vector: &FeatureVector| -> MarketFactorOutcome {
        assemble_market(
            vector,
            definitions,
            &raw_by_market[market],
            norm_grid,
            market,
            floor,
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
/// verdict. The first required factor that is missing, indeterminate, or below
/// the confidence floor rejects the market before model inference.
fn assemble_market(
    vector: &FeatureVector,
    definitions: &[&FactorDefinitionRef],
    raw_row: &[RawFactor],
    norm_grid: &[Vec<NormalizedFactor>],
    market: usize,
    floor: Decimal,
) -> MarketFactorOutcome {
    let mut scored = Vec::with_capacity(definitions.len());
    let mut reject: Option<FactorEligibility> = None;
    for (index, revision) in definitions.iter().enumerate() {
        let spec = revision.definition();
        let raw = &raw_row[index];
        let normalized = norm_grid[index][market].clone();
        let scored_factor = assemble(revision, raw, normalized, floor);
        if reject.is_none() && spec.is_required() && !scored_factor.contributes {
            reject = Some(reject_verdict(&scored_factor, floor));
        }
        scored.push(scored_factor);
    }
    let eligibility = reject.unwrap_or(FactorEligibility::Eligible);
    MarketFactorOutcome {
        market_id: vector.market_id.clone(),
        decision_at: vector.decision_at,
        eligibility,
        factors: scored,
    }
}

/// Build a [`ScoredFactor`] from a raw factor and its normalization outcome.
///
/// A factor contributes only when it carries a usable normalized score and its
/// confidence meets the floor. Missing inputs (`confidence = 0`), scores below
/// the floor (also zeroed), and indeterminate cross-sections never contribute —
/// and are never coerced to a neutral placeholder score.
fn assemble(
    revision: &FactorDefinitionRef,
    raw: &RawFactor,
    normalized: NormalizedFactor,
    floor: Decimal,
) -> ScoredFactor {
    let definition = revision.definition();
    // A factor computer can short-circuit normalization for a cell that is
    // structurally not applicable (binary market) or that should have computed
    // but had a structurally-absent input (missing neg-risk leg book). Neither
    // ever flows through the cross-section — the raw value for such a cell is
    // `None`, so it never pollutes the column stats.
    let normalized = match raw.eligibility {
        RawFactorEligibility::Normalizable => normalized,
        RawFactorEligibility::NotApplicable => NormalizedFactor::NotApplicable,
        RawFactorEligibility::Indeterminate(reason) => NormalizedFactor::Indeterminate { reason },
    };
    let normalized = match (&normalized, raw.raw_value) {
        (NormalizedFactor::Scored { source, clamp, .. }, Some(value))
            if definition.is_outcome_alpha() && value.is_zero() =>
        {
            NormalizedFactor::Scored {
                score: Probability::ZERO,
                source: *source,
                clamp: clamp.clone(),
            }
        }
        _ => normalized,
    };
    let scored = matches!(normalized, NormalizedFactor::Scored { .. });
    // A factor with no usable normalized score reports zero confidence: a missing
    // input or an indeterminate cross-section is not "confident" about anything,
    // and a non-zero confidence sitting next to an indeterminate reason misleads
    // operators (and the persisted fact / report breakdown). Only a scored factor
    // carries its raw confidence forward — unless it falls below the floor, in
    // which case confidence is zeroed so downstream scorers cannot partially weight it.
    let is_suppressed_alpha =
        definition.is_outcome_alpha() && raw.raw_value.is_some_and(|value| value.is_zero());
    let mut confidence = if scored && !is_suppressed_alpha {
        raw.confidence
    } else {
        Probability::ZERO
    };
    let below_confidence_floor = scored && confidence.inner() < floor;
    if below_confidence_floor {
        confidence = Probability::ZERO;
    }
    let value = FactorValue {
        definition_id: revision.factor_definition_id(),
        name: definition.name.clone(),
        family: definition.family,
        raw_value: raw.raw_value,
        normalization: normalized,
        direction: raw
            .raw_value
            .and_then(|value| definition.contribution_direction(value))
            .unwrap_or(FactorDirection::Neutral),
        confidence,
        explanation: FactorExplanation {
            headline: raw.headline.clone(),
            drivers: raw.drivers.clone(),
        },
        input_feature_refs: definition.input_features.clone(),
    };
    ScoredFactor {
        value,
        contributes: scored && !below_confidence_floor,
        below_confidence_floor,
    }
}

/// The market-level verdict for a required factor that did not contribute.
///
/// A structurally not-applicable required factor excludes the market as
/// [`FactorEligibility::NotApplicable`] (the signal cannot exist for this market's
/// structure); every other non-contributing reason is a quality
/// [`FactorEligibility::RejectCandidate`].
fn reject_verdict(scored: &ScoredFactor, floor: Decimal) -> FactorEligibility {
    let value = &scored.value;
    match &value.normalization {
        NormalizedFactor::NotApplicable => FactorEligibility::NotApplicable {
            reason: format!("required factor `{}` not applicable", value.name),
        },
        NormalizedFactor::MissingInput => FactorEligibility::RejectCandidate {
            reason: format!("required factor `{}` missing", value.name),
        },
        NormalizedFactor::Indeterminate { reason } => FactorEligibility::RejectCandidate {
            reason: format!("required factor `{}` indeterminate ({reason})", value.name),
        },
        NormalizedFactor::Scored { .. } => FactorEligibility::RejectCandidate {
            reason: format!(
                "required factor `{}` confidence below floor {floor}",
                value.name
            ),
        },
    }
}
