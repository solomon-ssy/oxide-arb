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

use quant_pivot_error::QuantResult;
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
        identity::provisional_factor_definition_id,
        names::{
            BOOK_IMBALANCE, DATA_QUALITY, LIQUIDITY_DEPTH, MARKET_ACTIVITY, MEAN_REVERSION,
            MOMENTUM_EMA_SLOPE, MOMENTUM_MACD, MOMENTUM_ROC, MOMENTUM_VOL_ADJUSTED,
            SPREAD_EFFICIENCY, TIME_TO_RESOLUTION, VOLATILITY_REGIME,
        },
        value::{
            FactorDefinitionSpec, FactorDriver, FactorName, FactorOutputKind, FactorQualityGate,
            RawFactor, RawFactorEligibility,
        },
    },
    features::{
        FeatureName, FeatureValue, FeatureVector,
        names::{book, market, micro, ts},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Every generic `(spec, computer)` pair, resolved against the feature config
/// (windowed factors bind their input feature name from the configured windows).
#[must_use]
pub fn generic_factors(
    features: &FeaturesConfig,
) -> Vec<(FactorDefinitionSpec, Arc<dyn FactorComputer>)> {
    let mut factors = vec![
        feature_factor(
            &LIQUIDITY_DEPTH,
            FactorFamily::Liquidity,
            book::VISIBLE_LIQUIDITY_USD,
            FactorDirection::Positive,
            FactorNormalization::Rank,
            true,
            "visible liquidity",
        ),
        feature_factor(
            &SPREAD_EFFICIENCY,
            FactorFamily::Liquidity,
            book::SPREAD_BPS,
            FactorDirection::Negative,
            FactorNormalization::WinsorizedZScore,
            true,
            "top-of-book spread",
        ),
        feature_factor(
            &BOOK_IMBALANCE,
            FactorFamily::Microstructure,
            book::DEPTH_IMBALANCE,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "depth imbalance",
        ),
    ];
    factors.extend(momentum_factors(features));
    factors.push(feature_factor(
        &MEAN_REVERSION,
        FactorFamily::MeanReversion,
        ts::PRICE_REVERSAL,
        FactorDirection::Positive,
        FactorNormalization::WinsorizedZScore,
        false,
        "price reversal",
    ));
    // The volatility-regime factor binds the primary (first) volatility window;
    // additional configured windows remain feature-plane only. Config validation
    // guarantees the list is non-empty, so this only elides the factor on a
    // (rejected) empty config — fail-closed, never a fabricated `_0s` feature.
    if let Some(realized_vol) = windowed_realized_vol(features) {
        factors.push(feature_factor(
            &VOLATILITY_REGIME,
            FactorFamily::Volatility,
            realized_vol,
            FactorDirection::Negative,
            FactorNormalization::Rank,
            false,
            "realized volatility",
        ));
    }
    factors.extend([
        feature_factor(
            &MARKET_ACTIVITY,
            FactorFamily::Activity,
            micro::QUOTE_UPDATE_RATE,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "quote update rate",
        ),
        feature_factor(
            &TIME_TO_RESOLUTION,
            FactorFamily::Resolution,
            market::TIME_TO_RESOLUTION_SECS,
            FactorDirection::Positive,
            FactorNormalization::Rank,
            false,
            "time to resolution",
        ),
        data_quality_factor(),
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
) -> Vec<(FactorDefinitionSpec, Arc<dyn FactorComputer>)> {
    let mut factors = Vec::with_capacity(4);
    if let Some(input) = momentum_roc_feature(features) {
        factors.push(feature_factor(
            &MOMENTUM_ROC,
            FactorFamily::Momentum,
            input,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "lag-skipped rate of change",
        ));
    }
    if let Some(input) = ema_slope_feature(features) {
        factors.push(feature_factor(
            &MOMENTUM_EMA_SLOPE,
            FactorFamily::Momentum,
            input,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "EMA slope",
        ));
    }
    if let Some(input) = vol_adjusted_feature(features) {
        factors.push(feature_factor(
            &MOMENTUM_VOL_ADJUSTED,
            FactorFamily::Momentum,
            input,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "volatility-adjusted return",
        ));
    }
    // MACD binds fixed fast/slow half-lives (no window list), so it is always
    // registered.
    factors.push(feature_factor(
        &MOMENTUM_MACD,
        FactorFamily::Momentum,
        ts::MACD_NORM,
        FactorDirection::Positive,
        FactorNormalization::WinsorizedZScore,
        false,
        "vol-normalized MACD",
    ));
    factors
}

/// Build a single-feature `(spec, computer)` pair.
fn feature_factor(
    name: &FactorName,
    family: FactorFamily,
    input: FeatureName,
    direction: FactorDirection,
    normalization: FactorNormalization,
    required: bool,
    headline_label: &'static str,
) -> (FactorDefinitionSpec, Arc<dyn FactorComputer>) {
    let name_str = name.as_str();
    let definition_id = provisional_factor_definition_id(name_str);
    let quality_gates = if required {
        vec![FactorQualityGate {
            name: format!("{name_str}_present"),
            min_confidence: Probability::new(Decimal::new(1, 1)),
        }]
    } else {
        Vec::new()
    };
    let spec = FactorDefinitionSpec {
        name: name.clone(),
        family,
        input_features: vec![input.clone()],
        output_kind: FactorOutputKind::NormalizedScore,
        default_direction: direction,
        normalization,
        owner: "quant-research".to_owned(),
        quality_gates,
    };
    let computer = Arc::new(FeatureBackedFactor {
        definition_id,
        spec: spec.clone(),
        input,
        headline_label,
    }) as Arc<dyn FactorComputer>;
    (spec, computer)
}

/// Build the data-quality `(spec, computer)` pair (`MinMax` bounds from config).
fn data_quality_factor() -> (FactorDefinitionSpec, Arc<dyn FactorComputer>) {
    let definition_id = provisional_factor_definition_id(DATA_QUALITY.as_str());
    let spec = FactorDefinitionSpec {
        name: DATA_QUALITY,
        family: FactorFamily::DataQuality,
        input_features: Vec::new(),
        output_kind: FactorOutputKind::NormalizedScore,
        default_direction: FactorDirection::Positive,
        normalization: FactorNormalization::MinMax,
        owner: "quant-research".to_owned(),
        quality_gates: Vec::new(),
    };
    let computer = Arc::new(DataQualityFactor {
        definition_id,
        spec: spec.clone(),
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
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionSpec,
    input: FeatureName,
    headline_label: &'static str,
}

impl FactorComputer for FeatureBackedFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }

    fn spec(&self) -> &FactorDefinitionSpec {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
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
            definition_id: self.definition_id.clone(),
            name: self.spec.name.clone(),
            family: self.spec.family,
            raw_value: raw,
            eligibility: RawFactorEligibility::Normalizable,
            direction: self.spec.default_direction,
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
    definition_id: FactorDefinitionId,
    spec: FactorDefinitionSpec,
}

impl FactorComputer for DataQualityFactor {
    fn definition_id(&self) -> FactorDefinitionId {
        self.definition_id.clone()
    }

    fn spec(&self) -> &FactorDefinitionSpec {
        &self.spec
    }

    fn compute_raw(&self, features: &FeatureVector) -> QuantResult<RawFactor> {
        let base = data_quality_confidence(features.data_quality);
        let total = features.value_count();
        let missing = features
            .iter_cells()
            .filter(|(_, cell)| {
                matches!(
                    cell.state,
                    crate::features::FeatureCellState::Missing
                        | crate::features::FeatureCellState::NotApplicable
                )
            })
            .count();
        let missing_ratio = if total == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(missing) / Decimal::from(total)
        };
        let missing_penalty = (missing_ratio * Decimal::new(5, 1)).round_dp(RESEARCH_DECIMAL_SCALE);
        let staleness_penalty = features
            .max_known_staleness_ms()
            .map_or_else(|| Decimal::new(5, 1), staleness_penalty);
        let raw = (base - missing_penalty - staleness_penalty)
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        let drivers = vec![
            FactorDriver {
                feature_name: FeatureName::from_static("data_quality.base"),
                contribution: base,
            },
            FactorDriver {
                feature_name: FeatureName::from_static("data_quality.missing_penalty"),
                contribution: -missing_penalty,
            },
            FactorDriver {
                feature_name: FeatureName::from_static("data_quality.staleness_penalty"),
                contribution: -staleness_penalty,
            },
        ];
        Ok(RawFactor {
            definition_id: self.definition_id.clone(),
            name: self.spec.name.clone(),
            family: self.spec.family,
            raw_value: Some(raw),
            eligibility: RawFactorEligibility::Normalizable,
            direction: FactorDirection::Positive,
            // The quality assessment itself is always fully trusted; the score
            // is the subject, not the confidence.
            confidence: Probability::ONE,
            headline: format!(
                "data quality {} ({missing}/{total} features missing)",
                features.data_quality
            ),
            drivers,
            input_feature_refs: Vec::new(),
        })
    }
}

/// Extract a numeric value from a present feature value.
pub(super) fn extract_decimal(value: &FeatureValue) -> Option<Decimal> {
    crate::features::feature_scalar(value)
}

/// Map an aggregate data-quality status to a `[0, 1]` confidence / score.
///
/// These are **definitional governance** anchors for the status enum (Fresh is
/// full trust, Insufficient is none), not the tunable *normalization* heuristics
/// audit #3 removed — those (winsor / clamp / min-max) are fully config-driven in
/// [`crate::factors::normalize`]. Keeping this mapping fixed also preserves the
/// factor engine's infallible construction (no per-compute `DecimalString`
/// parse). Distributional tuning of the alpha itself is learned in 11.4.
pub(super) fn data_quality_confidence(status: DataQualityStatus) -> Decimal {
    match status {
        DataQualityStatus::Fresh => Decimal::ONE,
        DataQualityStatus::Acceptable => Decimal::new(85, 2),
        DataQualityStatus::Degraded => Decimal::new(60, 2),
        DataQualityStatus::Stale => Decimal::new(40, 2),
        DataQualityStatus::Insufficient => Decimal::ZERO,
    }
}

/// Staleness penalty in `[0, 0.5]`: linear in age, fully penalized at 60s.
fn staleness_penalty(staleness_ms: u64) -> Decimal {
    let cap = Decimal::new(5, 1);
    let penalty =
        (Decimal::from(staleness_ms) / Decimal::from(60_000u64)).round_dp(RESEARCH_DECIMAL_SCALE);
    penalty.min(cap)
}
