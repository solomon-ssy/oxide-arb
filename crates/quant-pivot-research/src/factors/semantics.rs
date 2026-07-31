//! Versioned executable semantics for the stable factor catalog.
//!
//! A factor revision must change when its computation changes even if its
//! descriptive fields and feature names do not. Every factor definition
//! therefore binds one explicit raw-computation contract plus the shared
//! boundaries that actually participate in its output. These keys describe
//! algorithms and binding names only. Runtime parameter values, calibration
//! artifact identities, snapshot hashes, and source-code hashes are committed
//! by their own immutable artifacts and must not be copied into these keys.
//!
//! `quant-pivot/data-quality-confidence@1` freezes:
//! - `Fresh/Acceptable/Degraded/Stale/Insufficient` to `1/0.85/0.60/0.40/0`;
//! - present generic/structural values inherit that aggregate confidence;
//! - missing values carry zero confidence.
//!
//! `quant-pivot/raw-schema-null-policy-data-quality-score@2` freezes:
//! - the active `FeatureSchema` as the authority for quality-bearing inputs;
//! - only applicable `Penalize`/`RejectMarket` cells enter the denominator;
//!   `Optional`, `NeutralValue`, and structural `NotApplicable` cells cannot
//!   dilute or increase the missingness penalty;
//! - an absent/`Missing` quality-bearing cell enters the numerator, while only
//!   usable, staleness-governed cells enter the maximum-age calculation;
//! - the score subtracts `0.5 × missing_ratio` and
//!   `min(staleness_ms / 60000, 0.5)`, charges `0.5` for a usable bounded cell
//!   with unknown age, clamps to `[0, 1]`, and rounds to 12 dp.
//!
//! `quant-pivot/factor-normalization-boundary@1` freezes:
//! - per-factor method overrides take precedence over the definition method;
//!   per-factor winsor/clamp parameters take precedence over section defaults;
//! - winsorized z-score uses interpolated quantiles, population standard
//!   deviation, sigma clamping, `[−σ,+σ] → [0,1]`, and 12-dp output;
//! - rank uses average ranks for ties in a live cross-section and historical
//!   interpolation against a frozen reference, with 12-dp output;
//! - min-max clamps to configured semantic bounds, maps to `[0,1]`, and rounds
//!   to 12 dp;
//! - a too-small cross-section follows the governed `Indeterminate` or frozen
//!   reference policy; missing input is never fabricated as `0.5`;
//! - `NotApplicable` and `Indeterminate` raw eligibility override any numeric
//!   normalization result.
//!
//! The named raw components freeze the following executable boundaries:
//! - feature-scalar contracts use the canonical numeric projection verbatim;
//!   the primary-window variants bind index zero of
//!   `momentum.roc_windows_secs`, `momentum.slope_windows_secs`, or
//!   `volatility_windows_secs` and elide the factor when that binding is absent;
//! - Weather contracts route only `DomainFamily::Weather`; crypto contracts
//!   route only `DomainFamily::Crypto`. A present domain value has confidence
//!   one, independent of aggregate data quality; an explicit structural null is
//!   `NotApplicable`, while a source gap is missing with zero confidence.
//!   Ensemble probability is centered as `2p - 1`; the other Weather identity
//!   factors preserve their feature scalar. Observed band distance is a
//!   non-negative diagnostic measuring breach distance from both bounds;
//! - crypto strike pressure is
//!   `distance × sqrt(86400 / max(time_to_observation, 60))`, quantized to
//!   12 dp after the float square root. Crypto beta regime is
//!   `abs(underlying_momentum / underlying_realized_vol)`, requires positive
//!   volatility, and is rounded to 12 dp;
//! - shock reversal binds `reversal_after_shock.shock_k` and `shock_cap`;
//!   resolution proximity divides signed extremity by
//!   `max(time_to_resolution_secs / 86400, 1)`; participant concentration binds
//!   the named Gini, CR1, and HHI weights. Each output is rounded to 12 dp where
//!   its implementation currently does so;
//! - neg-risk applicability binds `negrisk.min_legs`: an explicit structural
//!   null or a count below the threshold is `NotApplicable`; a missing leg book
//!   is `Indeterminate(LegBookMissing)`. Leg-sum drift is `sum(ask) − 1` and its
//!   confidence is corroborated by clamped per-leg bid/ask tightness; convert
//!   edge is its absolute magnitude;
//! - favorite-longshot lookup binds category, time-to-resolution and price
//!   bucket plus `per_category_ic_gate`; an absent/inapplicable fitted lookup is
//!   missing, never a heuristic constant.
//!
//! The feature builders are also part of the named raw contracts: elapsed
//! market resolution and crypto observation times are rejected as
//! `OutOfValidRange` before entering the factor plane. Factor implementations
//! retain their existing defensive handling for hand-built malformed vectors.

use quant_pivot_models::types::factor::FactorComputationContract;

pub(super) const COMPUTATION_SEMANTIC_VERSION: u32 = 1;

#[cfg(test)]
const NORMALIZATION: &str = "quant-pivot/factor-normalization-boundary@1";
#[cfg(test)]
const DATA_QUALITY: &str = "quant-pivot/data-quality-confidence@1";

pub(super) const FEATURE_SCALAR_IDENTITY: &str = concat!(
    "quant-pivot/raw-feature-scalar-identity@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const PRIMARY_ROC: &str = concat!(
    "quant-pivot/raw-primary-roc-window-feature-scalar@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const PRIMARY_EMA_SLOPE: &str = concat!(
    "quant-pivot/raw-primary-ema-slope-window-feature-scalar@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const PRIMARY_VOL_ADJUSTED: &str = concat!(
    "quant-pivot/raw-primary-vol-adjusted-window-feature-scalar@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const PRIMARY_REALIZED_VOL: &str = concat!(
    "quant-pivot/raw-primary-realized-vol-window-feature-scalar@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const DATA_QUALITY_SCORE: &str = concat!(
    "quant-pivot/raw-schema-null-policy-data-quality-score@2+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const WEATHER_CONTRACT_PROBABILITY: &str = concat!(
    "quant-pivot/raw-weather-contract-probability-centered@4+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const WEATHER_CONTEXT_IDENTITY: &str = concat!(
    "quant-pivot/raw-weather-category-context-identity@3+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const WEATHER_BOUNDARY_DISTANCE: &str = concat!(
    "quant-pivot/raw-weather-contract-boundary-distance@4+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const CRYPTO_STRIKE_PRESSURE: &str = concat!(
    "quant-pivot/raw-crypto-category-strike-sqrt-urgency@2+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const CRYPTO_BETA_REGIME: &str = concat!(
    "quant-pivot/raw-crypto-category-abs-vol-normalized-momentum@3+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const REVERSAL_AFTER_SHOCK: &str = concat!(
    "quant-pivot/raw-reversal-after-shock@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const RESOLUTION_PROXIMITY: &str = concat!(
    "quant-pivot/raw-resolution-proximity-regime@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const PARTICIPANT_CONCENTRATION: &str = concat!(
    "quant-pivot/raw-participant-concentration@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const NEGRISK_LEG_SUM_DRIFT: &str = concat!(
    "quant-pivot/raw-negrisk-absolute-leg-sum-drift@2+",
    "quant-pivot/negrisk-applicability@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const NEGRISK_CONVERT_EDGE: &str = concat!(
    "quant-pivot/raw-negrisk-absolute-convert-edge@2+",
    "quant-pivot/negrisk-applicability@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);
pub(super) const FAVORITE_LONGSHOT: &str = concat!(
    "quant-pivot/raw-favorite-longshot-bias@1+",
    "quant-pivot/market-price-bias-lookup@1+",
    "quant-pivot/data-quality-confidence@1+",
    "quant-pivot/factor-normalization-boundary@1"
);

/// Bind one explicitly selected executable semantic key to a definition.
pub(super) fn contract(semantic_key: &'static str) -> FactorComputationContract {
    FactorComputationContract {
        semantic_version: COMPUTATION_SEMANTIC_VERSION,
        semantic_key: semantic_key.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::factor::FactorNormalization,
        runtime_config::{FactorsConfig, FeaturesConfig},
        types::factor::{FactorAlphaOrientation, FactorContextEffect, FactorOutputSemantics},
    };

    use super::{
        COMPUTATION_SEMANTIC_VERSION, CRYPTO_BETA_REGIME, CRYPTO_STRIKE_PRESSURE, DATA_QUALITY,
        DATA_QUALITY_SCORE, FAVORITE_LONGSHOT, FEATURE_SCALAR_IDENTITY, NEGRISK_CONVERT_EDGE,
        NEGRISK_LEG_SUM_DRIFT, NORMALIZATION, PARTICIPANT_CONCENTRATION, PRIMARY_EMA_SLOPE,
        PRIMARY_REALIZED_VOL, PRIMARY_ROC, PRIMARY_VOL_ADJUSTED, RESOLUTION_PROXIMITY,
        REVERSAL_AFTER_SHOCK, WEATHER_BOUNDARY_DISTANCE, WEATHER_CONTEXT_IDENTITY,
        WEATHER_CONTRACT_PROBABILITY,
    };
    use crate::factors::{
        domain::{crypto_domain_factors, weather_domain_factors},
        generic::generic_factors,
        structural::structural_factors,
    };

    type CatalogRow = (
        String,
        FactorOutputSemantics,
        FactorNormalization,
        bool,
        u32,
        String,
    );

    const FEATURE_ALPHA: FactorOutputSemantics = FactorOutputSemantics::OutcomeAlpha {
        orientation: FactorAlphaOrientation::FeatureToken,
    };
    const YES_ALPHA: FactorOutputSemantics = FactorOutputSemantics::OutcomeAlpha {
        orientation: FactorAlphaOrientation::CanonicalYes,
    };
    const HIGH_CONTEXT: FactorOutputSemantics = FactorOutputSemantics::Context {
        effect: FactorContextEffect::HigherIsSupportive,
    };
    const LOW_CONTEXT: FactorOutputSemantics = FactorOutputSemantics::Context {
        effect: FactorContextEffect::LowerIsSupportive,
    };

    fn catalog() -> Vec<CatalogRow> {
        let features = FeaturesConfig::default();
        let factors = FactorsConfig::default();
        let mut catalog = generic_factors(&features)
            .into_iter()
            .chain(crypto_domain_factors())
            .chain(weather_domain_factors())
            .chain(structural_factors(&factors, &features, None))
            .map(|(definition, _)| {
                (
                    definition.name.as_str().to_owned(),
                    definition.output,
                    definition.normalization,
                    definition.required,
                    definition.computation.semantic_version,
                    definition.computation.semantic_key,
                )
            })
            .collect::<Vec<_>>();
        catalog.sort_by(|left, right| left.0.cmp(&right.0));
        catalog
    }

    #[test]
    fn catalog_core_is_exact() {
        let catalog = catalog();
        let expected = [
            (
                "book_imbalance",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "data_quality",
                HIGH_CONTEXT,
                FactorNormalization::MinMax,
                false,
                DATA_QUALITY_SCORE,
            ),
            (
                "domain.weather.boundary_distance",
                FactorOutputSemantics::Diagnostic,
                FactorNormalization::Rank,
                false,
                WEATHER_BOUNDARY_DISTANCE,
            ),
            (
                "domain.weather.contract_probability",
                YES_ALPHA,
                FactorNormalization::MinMax,
                false,
                WEATHER_CONTRACT_PROBABILITY,
            ),
            (
                "domain.weather.forecast_dispersion",
                LOW_CONTEXT,
                FactorNormalization::Rank,
                false,
                WEATHER_CONTEXT_IDENTITY,
            ),
            (
                "domain.weather.source_basis_risk",
                LOW_CONTEXT,
                FactorNormalization::Rank,
                false,
                WEATHER_CONTEXT_IDENTITY,
            ),
            (
                "domain.weather.truth_maturity_risk",
                LOW_CONTEXT,
                FactorNormalization::MinMax,
                false,
                WEATHER_CONTEXT_IDENTITY,
            ),
            (
                "domain_crypto_beta_regime",
                FactorOutputSemantics::Diagnostic,
                FactorNormalization::WinsorizedZScore,
                false,
                CRYPTO_BETA_REGIME,
            ),
            (
                "domain_crypto_strike_pressure",
                YES_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                CRYPTO_STRIKE_PRESSURE,
            ),
            (
                "liquidity_depth",
                HIGH_CONTEXT,
                FactorNormalization::Rank,
                true,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "market_activity",
                HIGH_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "mean_reversion",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "momentum_ema_slope",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                PRIMARY_EMA_SLOPE,
            ),
            (
                "momentum_macd",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
        ]
        .map(|(name, output, normalization, required, key)| {
            (
                name.to_owned(),
                output,
                normalization,
                required,
                COMPUTATION_SEMANTIC_VERSION,
                key.to_owned(),
            )
        });
        assert_eq!(&catalog[..expected.len()], expected.as_slice());
    }

    #[test]
    fn catalog_tail_is_exact() {
        let catalog = catalog();
        let expected = [
            (
                "momentum_roc",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                PRIMARY_ROC,
            ),
            (
                "momentum_vol_adjusted",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                PRIMARY_VOL_ADJUSTED,
            ),
            (
                "spread_efficiency",
                LOW_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                true,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "struct.book_churn_intensity",
                LOW_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "struct.favorite_longshot",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                FAVORITE_LONGSHOT,
            ),
            (
                "struct.negrisk_convert_edge",
                LOW_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                false,
                NEGRISK_CONVERT_EDGE,
            ),
            (
                "struct.negrisk_leg_sum_drift",
                LOW_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                false,
                NEGRISK_LEG_SUM_DRIFT,
            ),
            (
                "struct.participant_concentration",
                LOW_CONTEXT,
                FactorNormalization::WinsorizedZScore,
                false,
                PARTICIPANT_CONCENTRATION,
            ),
            (
                "struct.resolution_proximity_regime",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                RESOLUTION_PROXIMITY,
            ),
            (
                "struct.reversal_after_shock",
                FEATURE_ALPHA,
                FactorNormalization::WinsorizedZScore,
                false,
                REVERSAL_AFTER_SHOCK,
            ),
            (
                "time_to_resolution",
                FactorOutputSemantics::Diagnostic,
                FactorNormalization::Rank,
                false,
                FEATURE_SCALAR_IDENTITY,
            ),
            (
                "volatility_regime",
                LOW_CONTEXT,
                FactorNormalization::Rank,
                false,
                PRIMARY_REALIZED_VOL,
            ),
        ]
        .map(|(name, output, normalization, required, key)| {
            (
                name.to_owned(),
                output,
                normalization,
                required,
                COMPUTATION_SEMANTIC_VERSION,
                key.to_owned(),
            )
        });

        assert_eq!(
            &catalog[catalog.len() - expected.len()..],
            expected.as_slice()
        );
        assert!(catalog.iter().all(|(_, _, _, _, version, key)| {
            *version != 0
                && key.contains(NORMALIZATION)
                && !key.contains("hash=")
                && !key.contains("artifact_id=")
        }));
        assert_eq!(
            catalog
                .iter()
                .filter(|(_, _, _, _, _, key)| key.contains(DATA_QUALITY))
                .count(),
            19
        );
    }
}
