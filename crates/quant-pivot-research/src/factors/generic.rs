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
use uuid::Uuid;

use crate::{
    factors::{
        computer::FactorComputer,
        names::{
            BOOK_IMBALANCE, DATA_QUALITY, LIQUIDITY_DEPTH, MARKET_ACTIVITY, MEAN_REVERSION,
            MOMENTUM_EMA_SLOPE, MOMENTUM_MACD, MOMENTUM_ROC, MOMENTUM_VOL_ADJUSTED,
            SPREAD_EFFICIENCY, TIME_TO_RESOLUTION, VOLATILITY_REGIME,
        },
        value::{
            FactorDefinitionSpec, FactorDriver, FactorName, FactorOutputKind, FactorQualityGate,
            RawFactor,
        },
    },
    features::{
        FeatureName, FeatureValue, FeatureVector,
        names::{book, market, micro, ts},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Stable namespace for deterministic factor-definition ids (UUID v5). Fixed
/// forever: changing it would re-key every persisted factor definition.
const FACTOR_NAMESPACE: Uuid = Uuid::from_u128(0x7c9e_6a55_3f1b_4d2a_8e0f_1c2d_3e4f_5a6b);

/// Deterministic factor-definition id from a stable factor name.
#[must_use]
pub fn factor_definition_id(name: &str) -> FactorDefinitionId {
    FactorDefinitionId::new(Uuid::new_v5(&FACTOR_NAMESPACE, name.as_bytes()))
}

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
    factors.extend([
        feature_factor(
            &MEAN_REVERSION,
            FactorFamily::MeanReversion,
            ts::PRICE_REVERSAL,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "price reversal",
        ),
        feature_factor(
            &VOLATILITY_REGIME,
            FactorFamily::Volatility,
            windowed_realized_vol(features),
            FactorDirection::Negative,
            FactorNormalization::Rank,
            false,
            "realized volatility",
        ),
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
fn momentum_factors(
    features: &FeaturesConfig,
) -> Vec<(FactorDefinitionSpec, Arc<dyn FactorComputer>)> {
    vec![
        feature_factor(
            &MOMENTUM_ROC,
            FactorFamily::Momentum,
            momentum_roc_feature(features),
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "lag-skipped rate of change",
        ),
        feature_factor(
            &MOMENTUM_EMA_SLOPE,
            FactorFamily::Momentum,
            ema_slope_feature(features),
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "EMA slope",
        ),
        feature_factor(
            &MOMENTUM_VOL_ADJUSTED,
            FactorFamily::Momentum,
            vol_adjusted_feature(features),
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "volatility-adjusted return",
        ),
        feature_factor(
            &MOMENTUM_MACD,
            FactorFamily::Momentum,
            ts::MACD_NORM,
            FactorDirection::Positive,
            FactorNormalization::WinsorizedZScore,
            false,
            "vol-normalized MACD",
        ),
    ]
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
    let definition_id = factor_definition_id(name_str);
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
    let definition_id = factor_definition_id(DATA_QUALITY.as_str());
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

/// Resolve the ROC-momentum feature from the first configured ROC window.
fn momentum_roc_feature(features: &FeaturesConfig) -> FeatureName {
    FeatureName::ts_momentum_roc(
        features
            .momentum
            .roc_windows_secs
            .first()
            .copied()
            .unwrap_or(0),
    )
}

/// Resolve the EMA-slope feature from the first configured slope window.
fn ema_slope_feature(features: &FeaturesConfig) -> FeatureName {
    FeatureName::ts_ema_slope(
        features
            .momentum
            .slope_windows_secs
            .first()
            .copied()
            .unwrap_or(0),
    )
}

/// Resolve the vol-adjusted-return feature from the first volatility window.
fn vol_adjusted_feature(features: &FeaturesConfig) -> FeatureName {
    FeatureName::ts_vol_adjusted_return(
        features
            .volatility_windows_secs
            .first()
            .copied()
            .unwrap_or(0),
    )
}

/// Resolve the realized-volatility feature from the first volatility window.
fn windowed_realized_vol(features: &FeaturesConfig) -> FeatureName {
    FeatureName::ts_realized_vol(
        features
            .volatility_windows_secs
            .first()
            .copied()
            .unwrap_or(0),
    )
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
        let raw = features.values.get(&self.input).and_then(extract_decimal);
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
        let total = features.values.len();
        let missing = features
            .values
            .values()
            .filter(|value| value.is_missing())
            .count();
        let missing_ratio = if total == 0 {
            Decimal::ZERO
        } else {
            Decimal::from(missing) / Decimal::from(total)
        };
        let missing_penalty = (missing_ratio * Decimal::new(5, 1)).round_dp(RESEARCH_DECIMAL_SCALE);
        let staleness_penalty = staleness_penalty(features.staleness_ms);
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

/// Extract a numeric value from a feature, treating `Missing` as absent.
fn extract_decimal(value: &FeatureValue) -> Option<Decimal> {
    if value.is_missing() {
        None
    } else {
        value.to_fact_decimal()
    }
}

/// Map an aggregate data-quality status to a `[0, 1]` confidence / score.
fn data_quality_confidence(status: DataQualityStatus) -> Decimal {
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
