//! The generic factor computers.
//!
//! Each generic factor is a pure, per-market function over a
//! [`FeatureVector`](crate::features::FeatureVector). Single-feature factors are
//! backed by [`FeatureBackedFactor`]; the [`DataQualityFactor`] reads the
//! vector's aggregate quality metadata. Raw values are never silently zero — a
//! missing input yields `raw_value = None` with `confidence = 0`.
//!
//! Each factor declares only its normalization **method** (a semantic choice).
//! The distributional parameters (winsorize percentile, sigma clamp, `MinMax`
//! bounds) are resolved from runtime config by the
//! [`FactorEngine`](crate::factors::FactorEngine) — **there are no hardcoded
//! normalization constants in this file**. Two factors are marked **required**
//! (they declare a quality gate): `liquidity_depth` and `spread_efficiency`.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{
        factor::{FactorFamily, FactorNormalization},
        quant::{DataQualityStatus, FactorDirection},
    },
    runtime_config::FeaturesConfig,
    types::{FactorDefinitionId, Probability},
};
use rust_decimal::Decimal;

use crate::{
    factors::{
        computer::FactorComputer,
        names::{
            BOOK_IMBALANCE, DATA_QUALITY, LIQUIDITY_DEPTH, MARKET_ACTIVITY, MEAN_REVERSION,
            MOMENTUM_EMA_SLOPE, MOMENTUM_MACD, MOMENTUM_ROC, MOMENTUM_VOL_ADJUSTED,
            SPREAD_EFFICIENCY, TIME_TO_RESOLUTION, VOLATILITY_REGIME,
        },
        semantics::{
            DATA_QUALITY_SCORE, FEATURE_SCALAR_IDENTITY, PRIMARY_EMA_SLOPE, PRIMARY_REALIZED_VOL,
            PRIMARY_ROC, PRIMARY_VOL_ADJUSTED, contract,
        },
        value::{
            FactorAlphaOrientation, FactorContextEffect, FactorDefinitionDocument, FactorDriver,
            FactorName, FactorOutputSemantics, RawFactor, RawFactorEligibility,
        },
    },
    features::{
        self, FeatureCellState, FeatureName, FeatureSchema, FeatureStaleness, FeatureValue,
        FeatureVector, NullPolicy, StalenessRule,
        names::{
            book,
            market::TIME_TO_RESOLUTION_SECS,
            micro::QUOTE_UPDATE_RATE,
            ts::{MACD_NORM, PRICE_REVERSAL},
        },
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Aggregate feature-plane status that changes the data-quality score.
const QUALITY_STATUS: FeatureName = FeatureName::from_static("data_quality.aggregate_status");
/// Applicable, quality-bearing cell states used by the missingness ratio.
const QUALITY_STATES: FeatureName =
    FeatureName::from_static("data_quality.applicable_feature_states");
/// Applicable, quality-bearing cell ages used by the staleness penalty.
const QUALITY_STALENESS: FeatureName =
    FeatureName::from_static("data_quality.applicable_feature_staleness");

/// Every generic `(spec, computer)` pair, resolved against the feature config
/// (windowed factors bind their input feature name from the configured windows).
#[must_use]
pub fn generic_factors(
    features: &FeaturesConfig,
) -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
    let mut factors = vec![
        FeatureFactorInput {
            name: LIQUIDITY_DEPTH,
            family: FactorFamily::Liquidity,
            output: FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            },
            input: book::VISIBLE_LIQUIDITY_USD,
            normalization: FactorNormalization::Rank,
            required: true,
            headline_label: "visible liquidity",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
        FeatureFactorInput {
            name: SPREAD_EFFICIENCY,
            family: FactorFamily::Liquidity,
            output: FactorOutputSemantics::Context {
                effect: FactorContextEffect::LowerIsSupportive,
            },
            input: book::SPREAD_BPS,
            normalization: FactorNormalization::WinsorizedZScore,
            required: true,
            headline_label: "top-of-book spread",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
        FeatureFactorInput {
            name: BOOK_IMBALANCE,
            family: FactorFamily::Microstructure,
            output: FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::FeatureToken,
            },
            input: book::DEPTH_IMBALANCE,
            normalization: FactorNormalization::WinsorizedZScore,
            required: false,
            headline_label: "depth imbalance",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
    ];
    factors.extend(momentum_factors(features));
    factors.push(
        FeatureFactorInput {
            name: MEAN_REVERSION,
            family: FactorFamily::MeanReversion,
            output: FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::FeatureToken,
            },
            input: PRICE_REVERSAL,
            normalization: FactorNormalization::WinsorizedZScore,
            required: false,
            headline_label: "price reversal",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
    );
    // The volatility-regime factor binds the primary (first) volatility window;
    // additional configured windows remain feature-plane only. Config validation
    // guarantees the list is non-empty, so this only elides the factor on a
    // (rejected) empty config — fail-closed, never a fabricated `_0s` feature.
    if let Some(realized_vol) = windowed_realized_vol(features) {
        factors.push(
            FeatureFactorInput {
                name: VOLATILITY_REGIME,
                family: FactorFamily::Volatility,
                output: FactorOutputSemantics::Context {
                    effect: FactorContextEffect::LowerIsSupportive,
                },
                input: realized_vol,
                normalization: FactorNormalization::Rank,
                required: false,
                headline_label: "realized volatility",
                semantic_key: PRIMARY_REALIZED_VOL,
            }
            .build(),
        );
    }
    factors.extend([
        FeatureFactorInput {
            name: MARKET_ACTIVITY,
            family: FactorFamily::Activity,
            output: FactorOutputSemantics::Context {
                effect: FactorContextEffect::HigherIsSupportive,
            },
            input: QUOTE_UPDATE_RATE,
            normalization: FactorNormalization::WinsorizedZScore,
            required: false,
            headline_label: "quote update rate",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
        FeatureFactorInput {
            name: TIME_TO_RESOLUTION,
            family: FactorFamily::Resolution,
            // Horizon suitability is already governed by the model artifact's
            // explicit horizon multiplier. Raw seconds have no universal
            // monotone relationship with opportunity quality.
            output: FactorOutputSemantics::Diagnostic,
            input: TIME_TO_RESOLUTION_SECS,
            normalization: FactorNormalization::Rank,
            required: false,
            headline_label: "time to resolution",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
        data_quality_factor(features),
    ]);
    factors
}

/// The independent momentum family: four distinct estimators (never a return
/// clone), each vol/normalization-tuned via config.
///
/// The windowed estimators (ROC, EMA slope, vol-adjusted return) bind the
/// **primary (first)** configured window; extra configured windows are computed
/// as features but not registered as separate weighted factors (registering one
/// factor per window would introduce intra-family collinearity). Config
/// validation guarantees the window lists are non-empty, so a missing primary
/// window elides the factor (fail-closed) rather than fabricating a `_0s` name.
fn momentum_factors(
    features: &FeaturesConfig,
) -> Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)> {
    let mut factors = Vec::with_capacity(4);
    if let Some(input) = momentum_roc_feature(features) {
        factors.push(
            FeatureFactorInput {
                name: MOMENTUM_ROC,
                family: FactorFamily::Momentum,
                output: FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::FeatureToken,
                },
                input,
                normalization: FactorNormalization::WinsorizedZScore,
                required: false,
                headline_label: "lag-skipped rate of change",
                semantic_key: PRIMARY_ROC,
            }
            .build(),
        );
    }
    if let Some(input) = ema_slope_feature(features) {
        factors.push(
            FeatureFactorInput {
                name: MOMENTUM_EMA_SLOPE,
                family: FactorFamily::Momentum,
                output: FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::FeatureToken,
                },
                input,
                normalization: FactorNormalization::WinsorizedZScore,
                required: false,
                headline_label: "EMA slope",
                semantic_key: PRIMARY_EMA_SLOPE,
            }
            .build(),
        );
    }
    if let Some(input) = vol_adjusted_feature(features) {
        factors.push(
            FeatureFactorInput {
                name: MOMENTUM_VOL_ADJUSTED,
                family: FactorFamily::Momentum,
                output: FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::FeatureToken,
                },
                input,
                normalization: FactorNormalization::WinsorizedZScore,
                required: false,
                headline_label: "volatility-adjusted return",
                semantic_key: PRIMARY_VOL_ADJUSTED,
            }
            .build(),
        );
    }
    // MACD binds fixed fast/slow half-lives (no window list), so it is always
    // registered.
    factors.push(
        FeatureFactorInput {
            name: MOMENTUM_MACD,
            family: FactorFamily::Momentum,
            output: FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::FeatureToken,
            },
            input: MACD_NORM,
            normalization: FactorNormalization::WinsorizedZScore,
            required: false,
            headline_label: "vol-normalized MACD",
            semantic_key: FEATURE_SCALAR_IDENTITY,
        }
        .build(),
    );
    factors
}

/// Complete semantic input for one single-feature factor.
struct FeatureFactorInput {
    name: FactorName,
    family: FactorFamily,
    output: FactorOutputSemantics,
    input: FeatureName,
    normalization: FactorNormalization,
    required: bool,
    headline_label: &'static str,
    semantic_key: &'static str,
}

impl FeatureFactorInput {
    /// Build the governed definition and its exact computer together.
    fn build(self) -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
        let spec = FactorDefinitionDocument {
            name: self.name,
            family: self.family,
            input_features: vec![self.input.clone()],
            output: self.output,
            normalization: self.normalization,
            owner: "quant-research".to_owned(),
            required: self.required,
            computation: contract(self.semantic_key),
        };
        let computer = Arc::new(FeatureBackedFactor {
            spec: spec.clone(),
            input: self.input,
            headline_label: self.headline_label,
        }) as Arc<dyn FactorComputer>;
        (spec, computer)
    }
}

/// Build the data-quality `(spec, computer)` pair (`MinMax` bounds from config).
fn data_quality_factor(
    features: &FeaturesConfig,
) -> (FactorDefinitionDocument, Arc<dyn FactorComputer>) {
    let spec = FactorDefinitionDocument {
        name: DATA_QUALITY,
        family: FactorFamily::DataQuality,
        // This factor consumes aggregate vector metadata rather than one raw
        // feature value. These three explicit inputs close that lineage while
        // the sealed feature-contract hash binds the exact active schema whose
        // null/staleness policies interpret the aggregate.
        input_features: vec![QUALITY_STATUS, QUALITY_STALENESS, QUALITY_STATES],
        output: FactorOutputSemantics::Context {
            effect: FactorContextEffect::HigherIsSupportive,
        },
        normalization: FactorNormalization::MinMax,
        owner: "quant-research".to_owned(),
        required: false,
        computation: contract(DATA_QUALITY_SCORE),
    };
    let schema = FeatureSchema::build(features).map_err(|error| error.to_string());
    let computer = Arc::new(DataQualityFactor {
        spec: spec.clone(),
        schema,
    }) as Arc<dyn FactorComputer>;
    (spec, computer)
}

/// Resolve the ROC-momentum feature from the primary (first) ROC window.
///
/// `None` when no ROC window is configured — the factor is then elided rather
/// than bound to a fabricated `ts.momentum_roc_0s` feature.
fn momentum_roc_feature(features: &FeaturesConfig) -> Option<FeatureName> {
    features
        .momentum
        .roc_windows_secs
        .first()
        .copied()
        .map(FeatureName::ts_momentum_roc)
}

/// Resolve the EMA-slope feature from the primary (first) slope window.
fn ema_slope_feature(features: &FeaturesConfig) -> Option<FeatureName> {
    features
        .momentum
        .slope_windows_secs
        .first()
        .copied()
        .map(FeatureName::ts_ema_slope)
}

/// Resolve the vol-adjusted-return feature from the primary volatility window.
fn vol_adjusted_feature(features: &FeaturesConfig) -> Option<FeatureName> {
    features
        .volatility_windows_secs
        .first()
        .copied()
        .map(FeatureName::ts_vol_adjusted_return)
}

/// Resolve the realized-volatility feature from the primary volatility window.
fn windowed_realized_vol(features: &FeaturesConfig) -> Option<FeatureName> {
    features
        .volatility_windows_secs
        .first()
        .copied()
        .map(FeatureName::ts_realized_vol)
}

/// A factor backed by one numeric feature, extracted via the feature's canonical
/// decimal projection.
struct FeatureBackedFactor {
    spec: FactorDefinitionDocument,
    input: FeatureName,
    headline_label: &'static str,
}

impl FactorComputer for FeatureBackedFactor {
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(
        &self,
        definition_id: FactorDefinitionId,
        features: &FeatureVector,
    ) -> QuantResult<RawFactor> {
        let raw = features.value(&self.input).and_then(extract_decimal);
        let (confidence, headline, drivers) = raw.map_or_else(
            || {
                (
                    Probability::ZERO,
                    format!("{} unavailable", self.headline_label),
                    Vec::new(),
                )
            },
            |value| {
                (
                    Probability::new(data_quality_confidence(features.data_quality)),
                    format!("{} = {value}", self.headline_label),
                    vec![FactorDriver {
                        feature_name: self.input.clone(),
                        contribution: value,
                    }],
                )
            },
        );
        Ok(RawFactor {
            definition_id,
            name: self.spec.name.clone(),
            family: self.spec.family,
            raw_value: raw,
            eligibility: RawFactorEligibility::Normalizable,
            direction: raw
                .and_then(|value| self.spec.contribution_direction(value))
                .unwrap_or(FactorDirection::Neutral),
            confidence,
            headline,
            drivers,
            input_feature_refs: vec![self.input.clone()],
        })
    }
}

/// The data-quality factor: a `[0, 1]` quality score with signed drivers (base
/// quality minus missing-feature and staleness penalties).
struct DataQualityFactor {
    spec: FactorDefinitionDocument,
    schema: Result<FeatureSchema, String>,
}

impl FactorComputer for DataQualityFactor {
    fn spec(&self) -> &FactorDefinitionDocument {
        &self.spec
    }

    fn compute_raw(
        &self,
        definition_id: FactorDefinitionId,
        features: &FeatureVector,
    ) -> QuantResult<RawFactor> {
        let schema = self
            .schema
            .as_ref()
            .map_err(|detail| ResearchError::FactorComputation {
                detail: format!("data-quality feature schema is invalid: {detail}"),
            })?;
        let base = data_quality_confidence(features.data_quality);
        let summary = QualitySummary::from_vector(schema, features);
        let total = summary.applicable;
        let missing = summary.missing;
        let missing_ratio = if total == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(missing) / Decimal::from(total)
        };
        let missing_penalty = (missing_ratio * Decimal::new(5, 1)).round_dp(RESEARCH_DECIMAL_SCALE);
        let staleness_penalty = summary.staleness_penalty();
        let raw = (base - missing_penalty - staleness_penalty)
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        let drivers = vec![
            FactorDriver {
                feature_name: QUALITY_STATUS,
                contribution: base,
            },
            FactorDriver {
                feature_name: QUALITY_STATES,
                contribution: -missing_penalty,
            },
            FactorDriver {
                feature_name: QUALITY_STALENESS,
                contribution: -staleness_penalty,
            },
        ];
        Ok(RawFactor {
            definition_id,
            name: self.spec.name.clone(),
            family: self.spec.family,
            raw_value: Some(raw),
            eligibility: RawFactorEligibility::Normalizable,
            direction: FactorDirection::Neutral,
            // The quality assessment itself is always fully trusted; the score
            // is the subject, not the confidence.
            confidence: Probability::ONE,
            headline: format!(
                "data quality {} ({missing}/{total} applicable quality-bearing features missing)",
                features.data_quality
            ),
            drivers,
            input_feature_refs: self.spec.input_features.clone(),
        })
    }
}

/// Schema-aware feature quality summary.
///
/// Optional and neutral-substitutable features are not quality-bearing:
/// whether those cells are absent or populated must not change generic market
/// quality. An explicit structural `NotApplicable` cell is excluded under
/// every policy. Missingness is therefore charged only to an applicable
/// `Penalize`/`RejectMarket` feature.
#[derive(Debug, Default, PartialEq, Eq)]
struct QualitySummary {
    applicable: usize,
    missing: usize,
    max_staleness_ms: Option<u64>,
    unknown_staleness: bool,
}

impl QualitySummary {
    fn from_vector(schema: &FeatureSchema, features: &FeatureVector) -> Self {
        let mut summary = Self::default();
        for spec in schema.specs().iter().filter(|spec| {
            matches!(
                spec.null_policy,
                NullPolicy::Penalize | NullPolicy::RejectMarket
            )
        }) {
            let cell = features.cell(&spec.name);
            if matches!(
                cell.map(|cell| cell.state),
                Some(FeatureCellState::NotApplicable)
            ) {
                continue;
            }

            summary.applicable += 1;
            if cell.is_none_or(|cell| cell.state == FeatureCellState::Missing) {
                summary.missing += 1;
            }

            if spec.staleness_policy == StalenessRule::None {
                continue;
            }
            // Missingness already has its own penalty. Only an actually usable
            // value can contribute an age; otherwise an unavailable feature
            // would be charged twice (missing + unknown staleness).
            let Some(cell) = cell.filter(|cell| cell.value().is_some()) else {
                continue;
            };
            match cell.staleness {
                FeatureStaleness::Known { age_ms } => {
                    summary.max_staleness_ms = Some(
                        summary
                            .max_staleness_ms
                            .map_or(age_ms, |max| max.max(age_ms)),
                    );
                }
                FeatureStaleness::Unknown => summary.unknown_staleness = true,
            }
        }
        summary
    }

    fn staleness_penalty(&self) -> Decimal {
        if self.unknown_staleness {
            Decimal::new(5, 1)
        } else {
            self.max_staleness_ms
                .map_or(Decimal::ZERO, staleness_penalty)
        }
    }
}

/// Extract a numeric value from a present feature value.
pub(super) fn extract_decimal(value: &FeatureValue) -> Option<Decimal> {
    features::feature_scalar(value)
}

/// Map an aggregate data-quality status to a `[0, 1]` confidence / score.
///
/// These are **definitional governance** anchors for the status enum (Fresh is
/// full trust, Insufficient is none), not the tunable *normalization* heuristics
/// audit #3 removed — those (winsor / clamp / min-max) are fully config-driven in
/// [`crate::factors::normalize`]. Keeping this mapping fixed also preserves the
/// factor engine's infallible construction (no per-compute `DecimalValue`
/// parse). Distributional tuning of the alpha itself belongs to model training.
pub(super) fn data_quality_confidence(status: DataQualityStatus) -> Decimal {
    match status {
        DataQualityStatus::Fresh => Decimal::ONE,
        DataQualityStatus::Acceptable => Decimal::new(85, 2),
        DataQualityStatus::Degraded => Decimal::new(60, 2),
        DataQualityStatus::Stale => Decimal::new(40, 2),
        DataQualityStatus::Insufficient => Decimal::ZERO,
    }
}

/// Staleness penalty `min(age_ms / 60_000, 0.5)`.
fn staleness_penalty(staleness_ms: u64) -> Decimal {
    let cap = Decimal::new(5, 1);
    let penalty =
        (Decimal::from(staleness_ms) / Decimal::from(60_000u64)).round_dp(RESEARCH_DECIMAL_SCALE);
    penalty.min(cap)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            domain::DomainFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        runtime_config::{FeatureFamily, FeaturesConfig},
        types::{FactorDefinitionId, MarketId, SchemaVersion, TokenId},
    };
    use rust_decimal_macros::dec;

    use super::{
        QUALITY_STALENESS, QUALITY_STATES, QUALITY_STATUS, data_quality_factor, generic_factors,
    };
    use crate::{
        factors::{
            names::{
                BOOK_IMBALANCE, MEAN_REVERSION, MOMENTUM_EMA_SLOPE, MOMENTUM_MACD, MOMENTUM_ROC,
                MOMENTUM_VOL_ADJUSTED,
            },
            value::{FactorAlphaOrientation, FactorOutputSemantics, RawFactor},
        },
        features::{
            DomainFeatureSlice, FeatureCell, FeatureStaleness, FeatureValue, FeatureVector,
            NullReason,
            names::{
                domain_crypto::{
                    BASIS_VS_RESOLUTION_SOURCE, DISTANCE_TO_STRIKE, TIME_TO_OBSERVATION,
                    UNDERLYING_MOMENTUM, UNDERLYING_REALIZED_VOL,
                },
                domain_weather::{
                    BOUNDARY_DISTANCE, CONTRACT_PROBABILITY, FORECAST_DISPERSION,
                    SOURCE_BASIS_RISK, TRUTH_MATURITY_RISK,
                },
                market::{CATEGORY, EVENT_AGE_SECS, IS_ACTIVE, NEG_RISK, TIME_TO_RESOLUTION_SECS},
                structural::{
                    NEGRISK_CONVERT_EDGE, NEGRISK_LEG_ASK_SUM, NEGRISK_LEG_BID_SUM,
                    NEGRISK_LEG_COUNT,
                },
            },
        },
    };

    fn observed(value: FeatureValue) -> FeatureCell {
        FeatureCell::observed(value, None, FeatureStaleness::Unknown)
    }

    fn market_vector(category: MarketCategory) -> FeatureVector {
        let generic = BTreeMap::from([
            (CATEGORY, observed(FeatureValue::Category(category))),
            (
                TIME_TO_RESOLUTION_SECS,
                observed(FeatureValue::Count(3_600)),
            ),
            (EVENT_AGE_SECS, observed(FeatureValue::Count(300))),
            (NEG_RISK, observed(FeatureValue::Bool(false))),
            (IS_ACTIVE, observed(FeatureValue::Bool(true))),
        ]);
        FeatureVector {
            market_id: MarketId::new("quality-market"),
            token_id: Some(TokenId::new("quality-token")),
            decision_at: Utc::now(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn score(config: &FeaturesConfig, features: &FeatureVector) -> RawFactor {
        let (_, computer) = data_quality_factor(config);
        computer
            .compute_raw(FactorDefinitionId::from_v7(), features)
            .expect("compute data quality")
    }

    #[test]
    fn quality_counts_penalized_missing() {
        let config = FeaturesConfig {
            enabled_feature_families: vec![FeatureFamily::MarketMetadata],
            ..FeaturesConfig::default()
        };

        let baseline = market_vector(MarketCategory::Sports);
        let baseline_raw = score(&config, &baseline);
        assert_eq!(baseline_raw.raw_value, Some(dec!(1)));

        let mut penalized = baseline.clone();
        penalized.generic.insert(
            EVENT_AGE_SECS,
            FeatureCell::missing(
                NullReason::SourceUnavailable,
                None,
                FeatureStaleness::Unknown,
            ),
        );
        let penalized_raw = score(&config, &penalized);
        assert_eq!(penalized_raw.raw_value, Some(dec!(0.875)));
        assert!(
            penalized_raw
                .headline
                .contains("1/4 applicable quality-bearing features missing")
        );

        let mut structural_absence = baseline.clone();
        structural_absence.generic.insert(
            EVENT_AGE_SECS,
            FeatureCell::not_applicable(NullReason::NotApplicable),
        );
        assert_eq!(
            score(&config, &structural_absence).raw_value,
            baseline_raw.raw_value
        );

        let mut neutral_missing = baseline;
        neutral_missing.generic.insert(
            NEG_RISK,
            FeatureCell::missing(
                NullReason::SourceUnavailable,
                None,
                FeatureStaleness::Unknown,
            ),
        );
        assert_eq!(
            score(&config, &neutral_missing).raw_value,
            baseline_raw.raw_value
        );
    }

    #[test]
    fn quality_matches_market_shapes() {
        let config = FeaturesConfig {
            enabled_feature_families: vec![
                FeatureFamily::MarketMetadata,
                FeatureFamily::Structural,
                FeatureFamily::Domain,
            ],
            ..FeaturesConfig::default()
        };

        let generic = market_vector(MarketCategory::Other);

        let mut binary = market_vector(MarketCategory::Sports);
        for name in [
            NEGRISK_LEG_ASK_SUM,
            NEGRISK_LEG_BID_SUM,
            NEGRISK_LEG_COUNT,
            NEGRISK_CONVERT_EDGE,
        ] {
            binary
                .generic
                .insert(name, FeatureCell::not_applicable(NullReason::NotApplicable));
        }

        let mut crypto = market_vector(MarketCategory::Crypto);
        crypto.domain = Some(DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version: SchemaVersion::FIRST,
            values: [
                DISTANCE_TO_STRIKE,
                UNDERLYING_MOMENTUM,
                UNDERLYING_REALIZED_VOL,
                TIME_TO_OBSERVATION,
                BASIS_VS_RESOLUTION_SOURCE,
            ]
            .into_iter()
            .map(|name| {
                (
                    name,
                    FeatureCell::missing(
                        NullReason::DomainSourceUnavailable,
                        None,
                        FeatureStaleness::Known { age_ms: 120_000 },
                    ),
                )
            })
            .collect(),
        });

        let mut weather = market_vector(MarketCategory::Weather);
        weather.domain = Some(DomainFeatureSlice {
            family: DomainFamily::Weather,
            schema_version: SchemaVersion::FIRST,
            values: [
                CONTRACT_PROBABILITY,
                FORECAST_DISPERSION,
                BOUNDARY_DISTANCE,
                SOURCE_BASIS_RISK,
                TRUTH_MATURITY_RISK,
            ]
            .into_iter()
            .map(|name| {
                (
                    name,
                    FeatureCell::missing(
                        NullReason::DomainSourceUnavailable,
                        None,
                        FeatureStaleness::Known { age_ms: 300_000 },
                    ),
                )
            })
            .collect(),
        });

        let scores =
            [&generic, &binary, &crypto, &weather].map(|vector| score(&config, vector).raw_value);
        assert_eq!(scores, [scores[0]; 4]);
    }

    #[test]
    fn raw_lineage_is_declared() {
        let config = FeaturesConfig::default();
        let vector = market_vector(MarketCategory::Other);
        let (definition, computer) = data_quality_factor(&config);
        assert_eq!(
            definition.input_features,
            vec![QUALITY_STATUS, QUALITY_STALENESS, QUALITY_STATES]
        );
        let raw = computer
            .compute_raw(FactorDefinitionId::from_v7(), &vector)
            .expect("compute data quality");
        assert_eq!(raw.input_feature_refs, definition.input_features);
        assert!(
            raw.drivers
                .iter()
                .all(|driver| definition.input_features.contains(&driver.feature_name))
        );

        for (definition, computer) in generic_factors(&config) {
            let raw = computer
                .compute_raw(FactorDefinitionId::from_v7(), &vector)
                .expect("compute generic factor");
            assert_eq!(raw.input_feature_refs, definition.input_features);
            assert!(
                raw.drivers
                    .iter()
                    .all(|driver| definition.input_features.contains(&driver.feature_name)),
                "{} emitted an undeclared driver",
                definition.name
            );
            assert_eq!(
                raw.direction,
                raw.raw_value
                    .and_then(|value| definition.contribution_direction(value))
                    .unwrap_or(FactorDirection::Neutral),
                "{} emitted a direction outside its governed output semantics",
                definition.name
            );
        }
    }

    #[test]
    fn output_kinds_are_exact() {
        let config = FeaturesConfig::default();
        let directional = [
            BOOK_IMBALANCE,
            MEAN_REVERSION,
            MOMENTUM_ROC,
            MOMENTUM_EMA_SLOPE,
            MOMENTUM_VOL_ADJUSTED,
            MOMENTUM_MACD,
        ];
        for (definition, _) in generic_factors(&config) {
            let expected = if directional.contains(&definition.name) {
                FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::FeatureToken,
                }
            } else {
                definition.output
            };
            assert_eq!(
                definition.output, expected,
                "{} has the wrong output domain",
                definition.name
            );
        }
    }
}
