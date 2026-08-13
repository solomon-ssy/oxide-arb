//! Runtime-config section structs grouped by document area.

use std::{collections::BTreeMap, iter};

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::{FactorFamily, FactorNormalization},
        quant::CalibrationMethod,
    },
    runtime_config::{
        BuyModelRoute,
        wire::{
            DecimalValue, FeatureFamily, FeatureStalenessPolicy, ModelVersionRef,
            NeutralizeDimension, RankLossKind, ReportDeliveryPolicy, ScheduleCadence,
            SmallCrossSectionPolicy, TrainingOptimizerKind,
        },
    },
    types::{
        ContentHash, FeedbackCycleId, ModelVersionId, PolicyBundleGeneration,
        PortfolioScenarioModelArtifactId, ReportScheduleId, SchemaVersion, Usd,
    },
};

/// Market selection selection policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SelectionConfig {
    /// Category slugs eligible for quant reports.
    pub enabled_categories: Vec<MarketCategory>,
    /// Minimum displayed liquidity in USD.
    pub min_liquidity_usd: DecimalValue,
    /// Minimum 24h volume in USD.
    pub min_volume_24h_usd: DecimalValue,
    /// Maximum allowed top-of-book spread in basis points.
    pub max_spread_bps: u32,
    /// Whether near-resolution markets may enter the selection.
    pub allow_near_resolution: bool,
    /// Minimum seconds until market resolution.
    pub min_time_to_resolution_secs: u64,
    /// Maximum seconds until market resolution.
    pub max_time_to_resolution_secs: u64,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            enabled_categories: Vec::new(),
            min_liquidity_usd: DecimalValue::new(rust_decimal_macros::dec!(0)),
            min_volume_24h_usd: DecimalValue::new(rust_decimal_macros::dec!(0)),
            max_spread_bps: 2_500,
            allow_near_resolution: false,
            min_time_to_resolution_secs: 3_600,
            max_time_to_resolution_secs: 31_536_000,
        }
    }
}

/// Data quality thresholds for PIT features and facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DataQualityConfig {
    /// Maximum age for a book snapshot before it is stale.
    pub max_book_age_ms: u64,
    /// Maximum acceptable ingest **pipeline** lag (enqueue→ClickHouse flush-ack),
    /// in milliseconds. Live-plane backpressure health only — NOT venue book age.
    /// Governs `DataQualitySnapshot.ingest_lag_exceeded`, execution admission, and
    /// market-candidate selection.
    pub max_ingest_lag_ms: u64,
    /// Maximum acceptable age (seconds) of the freshest materialized feature
    /// bucket at decision time. Governs offline/online feature staleness
    /// (`StalenessRule::MaxFeatureBucketAge`) — independent of live ingest lag.
    pub max_feature_bucket_age_secs: u64,
    /// Maximum acceptable age (seconds) of the freshest trade-tape print at
    /// decision time (`StalenessRule::MaxTradeTapeAge`).
    pub max_trade_tape_age_secs: u64,
    /// Maximum acceptable age (seconds) of the freshest domain observation for
    /// a linked instrument at decision time
    /// (`StalenessRule::MaxDomainObservationAge`).
    pub max_domain_observation_age_secs: u64,
    /// Reject crossed books before feature generation.
    pub reject_crossed_books: bool,
    /// Reject empty books before feature generation.
    pub reject_empty_books: bool,
    /// Named policy for stale feature handling.
    pub feature_staleness_policy: FeatureStalenessPolicy,
    /// Maximum tolerated stale-book ratio across the live book plane (basis points).
    ///
    /// Consumed by execution admission `DataQualityCheck` (#6): deny when
    /// `stale_tokens / total_tokens * 10_000` exceeds this cap. Distilled into
    /// frozen admission input at build time so checks never read config directly.
    pub max_stale_book_ratio_bps: u64,
}

impl Default for DataQualityConfig {
    fn default() -> Self {
        Self {
            // The default serving boundary intentionally withholds the newest
            // ten seconds of source data. The freshness ceiling therefore has
            // to exceed that PIT lag or every canonical book is stale by
            // construction before the feature plane sees it.
            max_book_age_ms: 15_000,
            max_ingest_lag_ms: 10_000,
            max_feature_bucket_age_secs: 30,
            max_trade_tape_age_secs: 300,
            max_domain_observation_age_secs: 300,
            reject_crossed_books: true,
            reject_empty_books: true,
            feature_staleness_policy: FeatureStalenessPolicy::RejectStaleRequired,
            max_stale_book_ratio_bps: 2_000,
        }
    }
}

/// Momentum feature-family windows and lags.
///
/// Momentum is deliberately **not** a plain windowed return (that would be
/// collinear with `ts.return_{W}`). Each estimator is a distinct signal:
/// lag-skipped rate-of-change, EMA slope, EMA-crossover (MACD), and
/// volatility-adjusted return.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct MomentumFeaturesConfig {
    /// Rate-of-change lookback windows in seconds (`ts.momentum_roc_{W}s`).
    pub roc_windows_secs: Vec<u64>,
    /// Seconds skipped at the near edge of each ROC window (classic 12-1
    /// momentum: exclude the most recent reversal-prone segment).
    pub roc_lag_secs: u64,
    /// Fast EMA **half-life in seconds** (MACD fast leg + EMA-slope base): an
    /// observation's weight halves every `ema_fast_secs` of elapsed time. This
    /// is a true duration, applied by the time-decayed EMA regardless of how
    /// densely the book is sampled — never a fixed point count.
    pub ema_fast_secs: u64,
    /// Slow EMA **half-life in seconds** (MACD slow leg); same duration
    /// semantics as `ema_fast_secs`.
    pub ema_slow_secs: u64,
    /// EMA-slope estimator windows in seconds (`ts.ema_slope_{W}s`): the trailing
    /// span of book samples the slope's time-decayed EMA is computed over.
    pub slope_windows_secs: Vec<u64>,
}

impl Default for MomentumFeaturesConfig {
    fn default() -> Self {
        Self {
            roc_windows_secs: vec![900, 3_600],
            roc_lag_secs: 300,
            ema_fast_secs: 300,
            ema_slow_secs: 900,
            slope_windows_secs: vec![900],
        }
    }
}

/// Structural feature-family windows.
///
/// These windows drive the shock/realized-vol, book-churn proxy, and persisted
/// trade-tape participant-concentration estimators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct StructuralFeaturesConfig {
    /// Lookback (seconds) for the shock ratio / realized-vol estimator that gates
    /// `struct.reversal_after_shock`.
    pub shock_window_secs: u64,
    /// Lookback (seconds) for the book-churn intensity proxy window.
    pub book_churn_window_secs: u64,
    /// Lookback (seconds) for trade-tape participant concentration.
    pub trade_tape_window_secs: u64,
    /// Minimum distinct fill-participant addresses before concentration features are scored.
    pub trade_tape_min_unique_participants: u64,
    /// Minimum notional USD required before concentration features are scored.
    pub trade_tape_min_notional_usd: DecimalValue,
    /// Minimum participant-address coverage ratio in `[0, 1]`.
    pub trade_tape_min_coverage_ratio: DecimalValue,
}

impl Default for StructuralFeaturesConfig {
    fn default() -> Self {
        Self {
            shock_window_secs: 900,
            book_churn_window_secs: 900,
            trade_tape_window_secs: 86_400,
            trade_tape_min_unique_participants: 20,
            trade_tape_min_notional_usd: DecimalValue::new(rust_decimal_macros::dec!(100.00)),
            trade_tape_min_coverage_ratio: DecimalValue::new(rust_decimal_macros::dec!(0.95)),
        }
    }
}

/// Feature schema and enabled feature families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    /// Current feature schema version.
    pub feature_schema_version: SchemaVersion,
    /// Enabled feature family names.
    pub enabled_feature_families: Vec<FeatureFamily>,
    /// Bar aggregation windows in seconds.
    pub bar_windows_secs: Vec<u64>,
    /// Momentum feature-family windows and lags.
    pub momentum: MomentumFeaturesConfig,
    /// Volatility windows in seconds.
    pub volatility_windows_secs: Vec<u64>,
    /// Order-book depth levels to inspect.
    pub depth_levels: Vec<u32>,
    /// Structural feature-family windows.
    pub structural: StructuralFeaturesConfig,
    /// Maximum concurrent per-market PIT resolves in the feature pipeline.
    pub max_concurrent_market_resolves: u32,
}

impl FeaturesConfig {
    /// The maximum trailing microstructure lookback (seconds) any enabled
    /// book-derived feature needs.
    #[must_use]
    pub fn max_microstructure_lookback_secs(&self) -> u64 {
        self.bar_windows_secs
            .iter()
            .chain(self.momentum.roc_windows_secs.iter())
            .chain(self.momentum.slope_windows_secs.iter())
            .chain(iter::once(&self.momentum.ema_slow_secs))
            .chain(self.volatility_windows_secs.iter())
            .copied()
            .max()
            .unwrap_or(0)
    }

    /// The maximum trailing fact lookback (seconds) any enabled time-series
    /// feature needs across all source families.
    #[must_use]
    pub fn max_lookback_secs(&self) -> u64 {
        self.max_microstructure_lookback_secs()
            .max(self.structural.trade_tape_window_secs)
    }
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            feature_schema_version: SchemaVersion::new(1),
            enabled_feature_families: vec![
                FeatureFamily::MarketMetadata,
                FeatureFamily::PriceBook,
                FeatureFamily::TimeSeries,
                FeatureFamily::Microstructure,
                FeatureFamily::Structural,
                FeatureFamily::Domain,
            ],
            bar_windows_secs: vec![60, 300, 900],
            momentum: MomentumFeaturesConfig::default(),
            volatility_windows_secs: vec![900, 3_600],
            depth_levels: vec![1, 3, 5],
            structural: StructuralFeaturesConfig::default(),
            max_concurrent_market_resolves: 32,
        }
    }
}

/// A per-factor normalization override.
///
/// The `method` is required; the distributional parameters are optional and, if
/// omitted, fall back to [`FactorNormalizationConfig`] defaults. `MinMax`
/// requires explicit `min`/`max` bounds (its semantic domain).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PerFactorNormalization {
    /// The normalization method for this factor (overrides its default).
    pub method: FactorNormalization,
    /// Winsorize percentile in `(0, 0.5)` (`WinsorizedZScore` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winsor_p: Option<DecimalValue>,
    /// Sigma clamp bound (`WinsorizedZScore` only), `> 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clamp_sigma: Option<DecimalValue>,
    /// Lower semantic bound mapped to 0 (`MinMax` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<DecimalValue>,
    /// Upper semantic bound mapped to 1 (`MinMax` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<DecimalValue>,
}

/// Cross-sectional normalization parameters (no magic constants in code).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorNormalizationConfig {
    /// Default winsorize percentile in `(0, 0.5)` for `WinsorizedZScore` factors.
    pub default_winsor_p: DecimalValue,
    /// Default sigma clamp bound for `WinsorizedZScore` factors, `> 0`.
    pub default_clamp_sigma: DecimalValue,
    /// Per-factor overrides keyed by stable factor name.
    pub per_factor: BTreeMap<String, PerFactorNormalization>,
}

impl Default for FactorNormalizationConfig {
    fn default() -> Self {
        let mut per_factor = BTreeMap::new();
        for factor_name in [
            // Data quality is already a semantic [0, 1] score.
            "data_quality",
            // Weather probability is centered to signed alpha before the
            // normalization boundary; its magnitude therefore remains [0, 1].
            "domain.weather.contract_probability",
            // Preliminary/final maturity is a closed binary risk.
            "domain.weather.truth_maturity_risk",
        ] {
            per_factor.insert(
                factor_name.to_owned(),
                PerFactorNormalization {
                    method: FactorNormalization::MinMax,
                    winsor_p: None,
                    clamp_sigma: None,
                    min: Some(DecimalValue::new(rust_decimal_macros::dec!(0))),
                    max: Some(DecimalValue::new(rust_decimal_macros::dec!(1))),
                },
            );
        }
        Self {
            default_winsor_p: DecimalValue::new(rust_decimal_macros::dec!(0.01)),
            default_clamp_sigma: DecimalValue::new(rust_decimal_macros::dec!(3)),
            per_factor,
        }
    }
}

/// Small-cross-section / cross-section normalization policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorCrossSectionConfig {
    /// Minimum present cross-section size for cross-sectional normalization.
    pub min_size: u32,
    /// What to do below `min_size` (never a silent neutral).
    pub small_cross_section_policy: SmallCrossSectionPolicy,
}

impl Default for FactorCrossSectionConfig {
    fn default() -> Self {
        Self {
            min_size: 5,
            small_cross_section_policy: SmallCrossSectionPolicy::Indeterminate,
        }
    }
}

/// Factor orthogonalization / collinearity policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorOrthogonalizeConfig {
    /// Maximum tolerated absolute pairwise Spearman correlation between factors.
    pub max_correlation: DecimalValue,
    /// Dimensions to neutralize each factor against before normalization.
    pub neutralize_by: Vec<NeutralizeDimension>,
}

impl Default for FactorOrthogonalizeConfig {
    fn default() -> Self {
        Self {
            max_correlation: DecimalValue::new(rust_decimal_macros::dec!(0.90)),
            neutralize_by: Vec::new(),
        }
    }
}

/// Shock-gated reversal factor parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReversalAfterShockConfig {
    /// Shock threshold `k`: reversal only fires when `|ret| / realized_vol > k`.
    pub shock_k: DecimalValue,
    /// Cap on the reported shock magnitude (bounds an extreme normalized signal).
    pub shock_cap: DecimalValue,
}

impl Default for ReversalAfterShockConfig {
    fn default() -> Self {
        Self {
            shock_k: DecimalValue::new(rust_decimal_macros::dec!(2.5)),
            shock_cap: DecimalValue::new(rust_decimal_macros::dec!(6)),
        }
    }
}

/// Neg-risk structural factor parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NegRiskStructuralConfig {
    /// Minimum resolved YES legs for a neg-risk structural factor to compute
    /// (below this the factor is structurally `NotApplicable`, never a silent
    /// value; missing a required leg book remains `Indeterminate`).
    pub min_legs: u32,
}

impl Default for NegRiskStructuralConfig {
    fn default() -> Self {
        Self { min_legs: 3 }
    }
}

/// Favorite-longshot bias factor parameters.
///
/// `bias_table_ref` points at a fitted `CalibrationArtifactId` artifact
/// (as its UUID string); `None` disables the factor (it stays inert — never a
/// silent heuristic fallback).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FavoriteLongshotConfig {
    /// Active fitted bias-table artifact id (`CalibrationArtifactId` UUID with
    /// `kind = market_price_bias`), or `None` (inert).
    pub bias_table_ref: Option<String>,
    /// Number of equal-width price buckets over `(0, 1)` the fit uses.
    pub bins: u32,
    /// Ascending time-to-resolution bucket boundaries (seconds). `n` bounds
    /// define `n + 1` conditioning buckets: `[0, b0), …, [b_{n-1}, ∞)`. The
    /// favorite-longshot bias is conditioned on residual time to resolution as
    /// well as category, so a bias measured a week out is never served as if it
    /// applied an hour out.
    pub ttr_bucket_bounds_secs: Vec<u64>,
    /// Minimum samples per `(category, ttr_bucket, price_bucket)` bin for a
    /// usable bias.
    pub min_bin_samples: u64,
    /// Minimum samples per `(category, ttr_bucket)` curve for it to be retained.
    pub min_curve_samples: u64,
    /// Two-sided confidence level for the Wilson interval and the IC
    /// significance test (e.g. `0.95`).
    pub ci_confidence: DecimalValue,
    /// Absolute `|IC|` floor a curve must additionally clear (the significance
    /// test is a Student-t on the correlation; this is a belt-and-suspenders
    /// magnitude floor).
    pub ic_significance_min: DecimalValue,
    /// Spacing between the point-in-time sample instants the fit draws over each
    /// market's lifecycle (seconds). The fit samples the entry mid across the
    /// whole life (not a single pre-resolution lead), so the empirical bias is
    /// measured on the same distribution the factor is served on.
    pub fit_sample_stride_secs: u64,
}

impl Default for FavoriteLongshotConfig {
    fn default() -> Self {
        Self {
            bias_table_ref: None,
            bins: 10,
            // Hour / day / week boundaries → four ttr conditioning buckets.
            ttr_bucket_bounds_secs: vec![3_600, 86_400, 604_800],
            min_bin_samples: 200,
            min_curve_samples: 1_000,
            ci_confidence: DecimalValue::new(rust_decimal_macros::dec!(0.95)),
            ic_significance_min: DecimalValue::new(rust_decimal_macros::dec!(0.02)),
            fit_sample_stride_secs: 21_600,
        }
    }
}

/// Structural factor-plane configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct StructuralFactorsConfig {
    /// Shock-gated reversal parameters.
    pub reversal_after_shock: ReversalAfterShockConfig,
    /// Neg-risk structural factor parameters.
    pub negrisk: NegRiskStructuralConfig,
    /// Favorite-longshot bias factor parameters.
    pub favorite_longshot: FavoriteLongshotConfig,
    /// Participant-concentration composite weights.
    pub participant_concentration: ParticipantConcentrationConfig,
    /// Soft per-category IC gate: disable a bias curve whose category IC is not
    /// statistically significant. Model publication has its own hard gate.
    pub per_category_ic_gate: bool,
}

impl Default for StructuralFactorsConfig {
    fn default() -> Self {
        Self {
            reversal_after_shock: ReversalAfterShockConfig::default(),
            negrisk: NegRiskStructuralConfig::default(),
            favorite_longshot: FavoriteLongshotConfig::default(),
            participant_concentration: ParticipantConcentrationConfig::default(),
            per_category_ic_gate: true,
        }
    }
}

/// Neutral structural participant-concentration composite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ParticipantConcentrationConfig {
    /// Weight on participant notional Gini.
    pub gini_weight: DecimalValue,
    /// Weight on largest single-participant notional share (CR1).
    pub cr1_share_weight: DecimalValue,
    /// Weight on participant notional HHI.
    pub hhi_weight: DecimalValue,
}

impl Default for ParticipantConcentrationConfig {
    fn default() -> Self {
        Self {
            gini_weight: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
            cr1_share_weight: DecimalValue::new(rust_decimal_macros::dec!(0.30)),
            hhi_weight: DecimalValue::new(rust_decimal_macros::dec!(0.20)),
        }
    }
}

/// Governed seed for the revision-bound `OutcomeAlpha` and `Context` heads.
///
/// Empty maps mean "expand canonically over the exact sealed serving plane";
/// non-empty maps must exactly cover the matching semantic head. These values
/// are training inputs only. Serving always consumes the immutable
/// [`crate::types::factor::FactorServingPlane`] and
/// artifact-frozen head produced from this seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorHeadConfig {
    /// Initial simplex over every `OutcomeAlpha` factor in the sealed plane.
    pub alpha_seed_weights: BTreeMap<String, DecimalValue>,
    /// Reliability-coverage simplex over every `Context` factor in the plane.
    pub context_coverage_weights: BTreeMap<String, DecimalValue>,
    /// Independent `[0, 1]` opportunity penalty for every `Context` factor.
    pub context_penalty_strengths: BTreeMap<String, DecimalValue>,
    /// Penalty expanded for every `Context` factor when the explicit map is empty.
    pub default_context_penalty_strength: DecimalValue,
    /// Absolute canonical-YES alpha at or below this value emits no side.
    pub alpha_deadband: DecimalValue,
}

impl Default for FactorHeadConfig {
    fn default() -> Self {
        Self {
            alpha_seed_weights: BTreeMap::new(),
            context_coverage_weights: BTreeMap::new(),
            context_penalty_strengths: BTreeMap::new(),
            default_context_penalty_strength: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
            alpha_deadband: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
        }
    }
}

/// Governed Hold-vs-Exit estimator and post-estimator projection.
///
/// The five estimator weights form one simplex. Position-state inputs are
/// model-intrinsic contract bindings, never globally published factors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SellScorerConfig {
    pub market_head_weight: DecimalValue,
    pub position_take_profit_weight: DecimalValue,
    pub position_stop_loss_weight: DecimalValue,
    pub position_time_in_trade_weight: DecimalValue,
    pub position_peak_drawdown_weight: DecimalValue,
    pub max_exit_alpha_bps: DecimalValue,
    pub p_exit_gain: DecimalValue,
    pub exit_deadband: DecimalValue,
    pub default_sell_pct: DecimalValue,
}

impl Default for SellScorerConfig {
    fn default() -> Self {
        Self {
            market_head_weight: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
            position_take_profit_weight: DecimalValue::new(rust_decimal_macros::dec!(0.125)),
            position_stop_loss_weight: DecimalValue::new(rust_decimal_macros::dec!(0.125)),
            position_time_in_trade_weight: DecimalValue::new(rust_decimal_macros::dec!(0.125)),
            position_peak_drawdown_weight: DecimalValue::new(rust_decimal_macros::dec!(0.125)),
            max_exit_alpha_bps: DecimalValue::new(rust_decimal_macros::dec!(300)),
            p_exit_gain: DecimalValue::new(rust_decimal_macros::dec!(4)),
            exit_deadband: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
            default_sell_pct: DecimalValue::new(rust_decimal_macros::dec!(1)),
        }
    }
}

/// Factor selection and immutable estimator-head seed configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorsConfig {
    /// Config-selectable factor families enabled for online computation
    /// (generic + structural).
    ///
    /// Vertical/domain factor families are routed by market category and must
    /// not appear here.
    pub enabled_factor_families: Vec<FactorFamily>,
    /// Revision-bound OutcomeAlpha/Context head seed.
    pub factor_head: FactorHeadConfig,
    /// Hold-vs-Exit estimator and output projection.
    pub sell_scorer: SellScorerConfig,
    /// Minimum confidence for a factor to contribute to scoring.
    pub min_factor_confidence: DecimalValue,
    /// Cross-sectional normalization parameters.
    pub normalization: FactorNormalizationConfig,
    /// Small-cross-section / cross-section policy.
    pub cross_section: FactorCrossSectionConfig,
    /// Orthogonalization / collinearity policy.
    pub orthogonalize: FactorOrthogonalizeConfig,
    /// Structural factor-plane parameters.
    pub structural: StructuralFactorsConfig,
}

impl Default for FactorsConfig {
    fn default() -> Self {
        // All generic families plus the platform-internal structural plane are
        // enabled by default: diversity is the baseline, weighting is learned.
        // Domain families are routed by category and never appear here.
        let mut enabled_factor_families = FactorFamily::ALL_GENERIC.to_vec();
        enabled_factor_families.push(FactorFamily::Structural);
        Self {
            enabled_factor_families,
            factor_head: FactorHeadConfig::default(),
            sell_scorer: SellScorerConfig::default(),
            min_factor_confidence: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
            normalization: FactorNormalizationConfig::default(),
            cross_section: FactorCrossSectionConfig::default(),
            orthogonalize: FactorOrthogonalizeConfig::default(),
            structural: StructuralFactorsConfig::default(),
        }
    }
}

/// Cross-check policy between the crypto feature source (Binance) and the
/// settlement oracle (Chainlink).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CryptoCrossCheckConfig {
    /// Maximum tolerated |Binance − Chainlink| basis, in basis points of the
    /// oracle price. A wider observed basis raises the risk flag and marks the
    /// linkage for review; it never fabricates or clamps a feature value.
    pub max_basis_bps: u32,
    /// Minimum seconds between two persisted alert rows for the same market.
    /// A market whose basis persistently exceeds the threshold across many
    /// consecutive report rounds raises one alert per cooldown window, not
    /// one per round — the governance feed stays a signal, not a flood.
    pub alert_cooldown_secs: u64,
    /// Maximum tolerated age (seconds) of a Chainlink Data Streams signed
    /// report before basis and price-to-beat features reject it as stale.
    /// Binance is never substituted for a stale or unavailable settlement
    /// binding.
    pub max_oracle_staleness_secs: u64,
}

impl Default for CryptoCrossCheckConfig {
    fn default() -> Self {
        Self {
            max_basis_bps: 50,
            alert_cooldown_secs: 300,
            max_oracle_staleness_secs: 60,
        }
    }
}

/// Crypto external-vertical parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CryptoDomainConfig {
    /// Source-specific publication/availability lag. The effective domain
    /// cutoff is the earlier of the global knowledge cutoff and this lag from
    /// the decision time.
    pub availability_lag_secs: u64,
    /// Days of history the ingest worker backfills on bootstrap, applied as a
    /// uniform **time** lower bound (`now - backfill_days`) to the historical
    /// Binance kline feature source. Live aggregate trades and Chainlink Data
    /// Streams maintain their own source-native sequence checkpoints and gap
    /// recovery windows.
    pub backfill_days: u32,
    /// Lookback (seconds) for the underlying momentum feature.
    pub momentum_window_secs: u64,
    /// Lookback (seconds) for the underlying realized-vol feature.
    pub volatility_window_secs: u64,
    /// Feature-source vs settlement-oracle basis cross-check policy.
    pub cross_check: CryptoCrossCheckConfig,
}

impl Default for CryptoDomainConfig {
    fn default() -> Self {
        Self {
            availability_lag_secs: 5,
            backfill_days: 90,
            momentum_window_secs: 3_600,
            volatility_window_secs: 3_600,
            cross_check: CryptoCrossCheckConfig::default(),
        }
    }
}

/// Category-routed external-vertical domain plane.
///
/// Domain families are **never** part of `enabled_factor_families`: routing is
/// by market category, and this section only gates which verticals may serve
/// data at all (a disabled family fails closed to `domain: None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct DomainConfig {
    /// Per-family enablement. A family absent from the map is disabled.
    pub enabled_by_family: BTreeMap<DomainFamily, bool>,
    /// Crypto vertical parameters.
    pub crypto: CryptoDomainConfig,
    /// Airport local-day maximum-temperature vertical parameters.
    pub weather: WeatherDomainConfig,
}

/// Weather feature-source PIT and calibration policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WeatherDomainConfig {
    /// Source publication/ingest lag subtracted from the decision clock before
    /// `AviationWeather`, `GHCNh`, or `GEFS` facts become PIT-visible.
    pub availability_lag_secs: u64,
    /// Maximum age in seconds of the newest complete GEFS run accepted for
    /// forecast factors. Older runs fail closed.
    pub max_forecast_age_secs: u64,
    /// Exact minimum number of complete GEFS ensemble members required for a
    /// forecast factor projection.
    pub minimum_complete_members: u8,
    /// Minimum distinct historical samples required for each station-by-lead
    /// GEFS bias calibration cell.
    pub minimum_bias_samples_per_lead: u32,
    /// Maximum historical lookback in local calendar days used to fit Weather
    /// station/lead bias and source-basis calibration.
    pub calibration_lookback_days: u32,
}

impl Default for WeatherDomainConfig {
    fn default() -> Self {
        Self {
            availability_lag_secs: 300,
            max_forecast_age_secs: 21_600,
            minimum_complete_members: 31,
            minimum_bias_samples_per_lead: 30,
            calibration_lookback_days: 730,
        }
    }
}

impl DomainConfig {
    /// Whether a domain family is enabled (absent ⇒ disabled, fail-closed).
    #[must_use]
    pub fn family_enabled(&self, family: DomainFamily) -> bool {
        self.enabled_by_family
            .get(&family)
            .copied()
            .unwrap_or(false)
    }

    /// A domain plane with every vertical disabled.
    ///
    /// For windows that structurally consume no domain data (e.g. the
    /// favorite-longshot bias fit, which reads only settlement mids) — the
    /// prefetch then skips the linkage/observation reads entirely.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled_by_family: BTreeMap::new(),
            crypto: CryptoDomainConfig::default(),
            weather: WeatherDomainConfig::default(),
        }
    }
}

impl Default for DomainConfig {
    fn default() -> Self {
        let mut enabled_by_family = BTreeMap::new();
        enabled_by_family.insert(DomainFamily::Crypto, true);
        enabled_by_family.insert(DomainFamily::Weather, true);
        Self {
            enabled_by_family,
            crypto: CryptoDomainConfig::default(),
            weather: WeatherDomainConfig::default(),
        }
    }
}

/// Active and shadow model references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelCalibrationConfig {
    /// Default calibrator fitting method (`isotonic` or `platt`).
    pub method: CalibrationMethod,
    /// Minimum samples required to select isotonic (below ⇒ fit must use Platt).
    pub min_samples_isotonic: u64,
    /// Minimum embargo gap (seconds) between a model's training-dataset window
    /// and its calibration-dataset window.
    pub embargo_secs: u64,
    /// Two-sided confidence level for reliability-bin Wilson intervals.
    pub ci_confidence: DecimalValue,
}

impl Default for ModelCalibrationConfig {
    fn default() -> Self {
        Self {
            method: CalibrationMethod::Isotonic,
            min_samples_isotonic: 1_000,
            embargo_secs: 86_400,
            ci_confidence: DecimalValue::new(rust_decimal_macros::dec!(0.95)),
        }
    }
}

/// Immutable provenance of one route-owned model binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "source_kind", deny_unknown_fields)]
pub enum ModelBindingSource {
    /// Explicit first-champion governance transaction.
    Bootstrap,
    /// Feedback cycle that trained and bound the model.
    Feedback { feedback_cycle_id: FeedbackCycleId },
}

/// One role binding inside a Buy route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelBinding {
    /// Immutable model version selected for this exact Route and serving role.
    pub model_version_id: ModelVersionId,
    /// Governance provenance that created the binding.
    pub source: ModelBindingSource,
    /// Database decision time at which the binding became visible.
    pub bound_at: DateTime<Utc>,
    /// Policy-bundle revision atomically committed with this binding.
    pub config_revision: PolicyBundleGeneration,
    /// Monotonic Route-serving generation used to reject stale readers.
    pub generation: u64,
}

impl ModelBinding {
    #[must_use]
    pub const fn new(
        model_version_id: ModelVersionId,
        source: ModelBindingSource,
        bound_at: DateTime<Utc>,
        config_revision: PolicyBundleGeneration,
        generation: u64,
    ) -> Self {
        Self {
            model_version_id,
            source,
            bound_at,
            config_revision,
            generation,
        }
    }
}

/// Champion plus the optional challenger bound to one exact Buy route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BuyRouteBinding {
    /// Active champion used for production inference on this Route.
    pub champion: ModelBinding,
    /// Optional governed challenger evaluated in shadow without serving trades.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shadow: Option<ModelBinding>,
}

/// Exact promoted scenario-generation model for one ordered represented Route set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioScenarioModelArtifactBinding {
    pub portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId,
    pub ordered_routes: Vec<BuyModelRoute>,
    pub route_set_digest: ContentHash,
    pub serving_contract_digest: ContentHash,
    pub calibration_contract_digest: ContentHash,
    pub trade_policy_contract_digest: ContentHash,
    pub scenario_model_schema_version: SchemaVersion,
    /// Digest of the strictly ordered capital-time boundaries. Per-bucket USD
    /// caps remain in the frozen `ExecutionRiskPolicy` and do not require a
    /// statistical scenario-model refit when only their values change.
    pub capital_time_bucket_contract_digest: ContentHash,
    pub model_content_hash: ContentHash,
    pub bound_at: DateTime<Utc>,
}

/// Route-owned Buy models and independent Sell model reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// Exact Buy-side bindings. A missing route fails closed and never falls
    /// back to another route.
    pub buy_routes: BTreeMap<BuyModelRoute, BuyRouteBinding>,
    /// Champion Sell-side hold-vs-exit scorer version. The
    /// opportunistic-Sell exit evaluator loads this; a distinct pointer from
    /// Buy routes so Buy and Sell models are governed separately.
    pub active_exit_model_version_id: Option<ModelVersionRef>,
    /// Shadow/live diff threshold.
    pub shadow_diff_threshold: DecimalValue,
    /// Model-score probability-calibrator fit policy.
    pub calibration: ModelCalibrationConfig,
    /// Exact scenario-model bindings. Lookup is by ordered Route-set digest;
    /// absence or ambiguity fails the entire report.
    pub portfolio_scenario_model_bindings: Vec<PortfolioScenarioModelArtifactBinding>,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            buy_routes: BTreeMap::new(),
            active_exit_model_version_id: None,
            shadow_diff_threshold: DecimalValue::new(rust_decimal_macros::dec!(0.10)),
            calibration: ModelCalibrationConfig::default(),
            portfolio_scenario_model_bindings: Vec::new(),
        }
    }
}

/// Sell-side hold-vs-exit quality-gate thresholds; alpha-significance fields
/// are hard CPCV gates.
///
/// DSR significance lives only under `research.validation.gates.dsr_significance`
/// (shared with Buy CPCV compute + gate) — never duplicated here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SellQualityGateConfig {
    /// Minimum `ExitDecision` sample count for a sell scorer to clear the gate.
    pub min_sample_count: u64,
    /// Minimum sell-side label coverage in `[0, 1]`.
    pub min_label_coverage: DecimalValue,
    /// Minimum CPCV path-set median rank IC in `[-1, 1]`; this hard gate
    /// replaces the deleted single-path soft `min_exit_alpha_rank_ic` and
    /// mirrors the Buy-side `research.validation.gates.rank_ic_min`.
    pub rank_ic_min: DecimalValue,
    /// Maximum tolerated Probability of Backtest Overfitting (hard gate).
    pub max_pbo: DecimalValue,
    /// Minimum fraction of `ExitDecision` rows simulated from full L2 books.
    pub min_l2_book_fidelity_ratio: DecimalValue,
    /// Maximum fraction of `ExitDecision` rows using microstructure fallback.
    pub max_fallback_ratio: DecimalValue,
}

impl Default for SellQualityGateConfig {
    fn default() -> Self {
        Self {
            min_sample_count: 200,
            min_label_coverage: DecimalValue::new(rust_decimal_macros::dec!(0.60)),
            rank_ic_min: DecimalValue::new(rust_decimal_macros::dec!(0.02)),
            max_pbo: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
            min_l2_book_fidelity_ratio: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
            max_fallback_ratio: DecimalValue::new(rust_decimal_macros::dec!(0.50)),
        }
    }
}

/// Governed quality-gate thresholds.
///
/// Hot-reloadable knobs consumed by the model quality gate and the publish /
/// rollback / dataset-promotion governance. Decimal-valued thresholds are
/// stored as [`DecimalValue`] (lossless), matching every other money /
/// probability config field; the core governance layer parses them into the
/// research `QualityGateThresholds`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct QualityGateConfig {
    /// Minimum resolved sample count for a model / dataset to clear the gate.
    pub min_sample_count: u64,
    /// Minimum label coverage in `[0, 1]`.
    pub min_label_coverage: DecimalValue,
    /// Minimum planned-sample materialization coverage in `[0, 1]`.
    pub min_materialization_coverage: DecimalValue,
    /// Maximum tolerated backtest drawdown in `[0, 1]`.
    pub max_drawdown: DecimalValue,
    /// Minimum liquidity-exit feasibility in `[0, 1]` (auto-execution gate).
    pub min_liquidity_exit_feasibility: DecimalValue,
    /// Minimum signed `TopN` decision overlap in `[0, 1]` (route-promotion gate).
    pub min_shadow_decision_overlap: DecimalValue,
    /// Maximum (soft) per-category sample concentration in `[0, 1]`.
    pub max_category_concentration: DecimalValue,
    /// Minimum shadow comparison window (seconds) required before publish.
    pub required_shadow_window_secs: u64,
    /// Sell-side hold-vs-exit scorer thresholds.
    pub sell: SellQualityGateConfig,
}

impl Default for QualityGateConfig {
    fn default() -> Self {
        Self {
            min_sample_count: 500,
            min_label_coverage: DecimalValue::new(rust_decimal_macros::dec!(0.70)),
            min_materialization_coverage: DecimalValue::new(rust_decimal_macros::dec!(0.95)),
            max_drawdown: DecimalValue::new(rust_decimal_macros::dec!(0.30)),
            min_liquidity_exit_feasibility: DecimalValue::new(rust_decimal_macros::dec!(0.90)),
            min_shadow_decision_overlap: DecimalValue::new(rust_decimal_macros::dec!(0.60)),
            max_category_concentration: DecimalValue::new(rust_decimal_macros::dec!(0.60)),
            required_shadow_window_secs: 86_400,
            sell: SellQualityGateConfig::default(),
        }
    }
}

/// Offline training-dataset build parameters.
///
/// Distinct from online [`DataQualityConfig`]: historical PIT book lookup uses a
/// much wider staleness window than live feature gates, and label thresholds are
/// not the same as feature rejection thresholds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TrainingConfig {
    /// Maximum lookback for historical PIT book resolution during dataset build (ms).
    ///
    /// Snapshots older than `as_of - max_book_staleness_ms` are treated as missing.
    pub max_book_staleness_ms: u64,
    /// Minimum forward top-1 depth (USD) for the `liquidity_exit_possible` label.
    pub min_exit_depth_usd: DecimalValue,
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            max_book_staleness_ms: 300_000,
            min_exit_depth_usd: DecimalValue::new(rust_decimal_macros::dec!(100)),
        }
    }
}

impl TrainingConfig {
    /// Resolve [`TrainingConfig::min_exit_depth_usd`] into a typed [`Usd`] value.
    pub const fn min_exit_depth(&self) -> Usd {
        Usd::new(self.min_exit_depth_usd.value)
    }
}

/// Absolute operational ceiling for one report's recommendation set.
///
/// This bounds transactional publication/revocation cascades independently of
/// a deployment's governed `max_top_n` choice.
pub const MAX_REPORT_TOP_N: u32 = 1_000;

/// Report schedules and payload sizing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportsConfig {
    /// Deployment-proven upper bound for one complete catalog-visible report.
    /// Exceeding it fails the report; it never truncates market candidates.
    pub hard_candidate_ceiling: u32,
    /// Maximum `TopN` size (hard upper bound for every schedule and ad-hoc run,
    /// capped by [`MAX_REPORT_TOP_N`]).
    pub max_top_n: u32,
    /// Default `TopN` frozen when an ad-hoc request omits its override.
    pub ad_hoc_default_top_n: u32,
    /// Default global knowledge lag frozen for an ad-hoc request without an override.
    pub ad_hoc_default_knowledge_lag_secs: u64,
    /// Entry-window ratio in `(0, 1]`: a recommendation's entry-by deadline is
    /// `as_of + effective_horizon * entry_window_ratio`. `0.5` enters only while
    /// at least half the signal's edge remains (the half-life point); the
    /// time-stop / exit still uses the full effective horizon.
    pub entry_window_ratio: DecimalValue,
    /// Whether ad-hoc report generation is enabled.
    pub ad_hoc_report_enabled: bool,
    /// Delivery policy name.
    pub delivery_policy: ReportDeliveryPolicy,
}

impl Default for ReportsConfig {
    fn default() -> Self {
        Self {
            hard_candidate_ceiling: 100_000,
            max_top_n: 100,
            ad_hoc_default_top_n: 20,
            ad_hoc_default_knowledge_lag_secs: 10,
            entry_window_ratio: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
            ad_hoc_report_enabled: false,
            delivery_policy: ReportDeliveryPolicy::StoreAndNotify,
        }
    }
}

/// One report schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportScheduleConfig {
    /// Stable schedule id.
    pub schedule_id: ReportScheduleId,
    /// How often this schedule fires (fixed interval or cron).
    pub cadence: ScheduleCadence,
    /// `TopN` size for this schedule.
    pub top_n: u32,
    /// Global knowledge lag in seconds.
    pub knowledge_lag_secs: u64,
    /// Whether this schedule is enabled.
    pub enabled: bool,
}

impl Default for ReportScheduleConfig {
    fn default() -> Self {
        Self {
            schedule_id: "default_interval".into(),
            cadence: ScheduleCadence::default(),
            top_n: 20,
            knowledge_lag_secs: 10,
            enabled: true,
        }
    }
}

/// Portfolio policy expressed entirely as hard economic constraints.
///
/// Policy limits only — never account state. Real equity and positions come
/// from the account snapshot, never from this configuration.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct PortfolioConfig {
    pub budget: PortfolioBudget,
    pub exposure_limits: PortfolioExposureLimits,
    pub tail_risk: PortfolioTailRisk,
    pub admission: PortfolioAdmission,
}

/// Capital budget governance caps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioBudget {
    /// Maximum account capital that this strategy may govern.
    pub total_budget_usd: DecimalValue,
    /// Cash that must remain immediately available in every scenario.
    pub cash_reserve_usd: DecimalValue,
    /// Maximum capital that may be locked by all open positions and intents.
    pub max_open_capital_usd: DecimalValue,
}

impl Default for PortfolioBudget {
    fn default() -> Self {
        Self {
            total_budget_usd: DecimalValue::new(rust_decimal_macros::dec!(10000)),
            cash_reserve_usd: DecimalValue::new(rust_decimal_macros::dec!(1000)),
            max_open_capital_usd: DecimalValue::new(rust_decimal_macros::dec!(9000)),
        }
    }
}

/// Exposure caps evaluated against existing positions plus selected tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioExposureLimits {
    /// Maximum USD allocated to one recommendation.
    pub max_single_recommendation_usd: DecimalValue,
    /// Maximum USD exposure per market.
    pub max_market_exposure_usd: DecimalValue,
    /// Maximum USD exposure per event.
    pub max_event_exposure_usd: DecimalValue,
    /// Maximum USD exposure per category.
    pub max_category_exposure_usd: DecimalValue,
    /// Maximum USD exposure per model Route.
    pub max_route_exposure_usd: DecimalValue,
    /// Maximum number of simultaneously open recommendations.
    pub max_open_recommendations: u32,
}

impl Default for PortfolioExposureLimits {
    fn default() -> Self {
        Self {
            max_single_recommendation_usd: DecimalValue::new(rust_decimal_macros::dec!(1000)),
            max_market_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(2000)),
            max_event_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(3000)),
            max_category_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(5000)),
            max_route_exposure_usd: DecimalValue::new(rust_decimal_macros::dec!(6000)),
            max_open_recommendations: 20,
        }
    }
}

/// Maximum capital lock permitted through one elapsed-time boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapitalTimeBucketLimit {
    /// Exclusive upper duration of the bucket from decision time.
    pub end_secs: u64,
    /// Maximum capital locked inside this bucket.
    pub max_capital_usd: DecimalValue,
}

/// Scenario loss, `CVaR`, drawdown, and time-weighted capital limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioTailRisk {
    /// `CVaR` confidence level in basis points, strictly between 0 and 10,000.
    pub cvar_confidence_bps: u32,
    /// Maximum allowed `CVaR` loss in USD.
    pub max_cvar_usd: DecimalValue,
    /// Maximum allowed loss in any promoted or stress scenario.
    pub max_scenario_loss_usd: DecimalValue,
    /// Maximum strategy drawdown admitted at the decision boundary.
    pub max_drawdown_usd: DecimalValue,
    /// Strictly increasing capital-lock buckets; every tier must terminate in a covered bucket.
    pub capital_time_buckets: Vec<CapitalTimeBucketLimit>,
}

impl Default for PortfolioTailRisk {
    fn default() -> Self {
        Self {
            cvar_confidence_bps: 9_500,
            max_cvar_usd: DecimalValue::new(rust_decimal_macros::dec!(1500)),
            max_scenario_loss_usd: DecimalValue::new(rust_decimal_macros::dec!(2500)),
            max_drawdown_usd: DecimalValue::new(rust_decimal_macros::dec!(2000)),
            capital_time_buckets: vec![
                CapitalTimeBucketLimit {
                    end_secs: 3_600,
                    max_capital_usd: DecimalValue::new(rust_decimal_macros::dec!(3000)),
                },
                CapitalTimeBucketLimit {
                    end_secs: 86_400,
                    max_capital_usd: DecimalValue::new(rust_decimal_macros::dec!(6000)),
                },
                CapitalTimeBucketLimit {
                    end_secs: 604_800,
                    max_capital_usd: DecimalValue::new(rust_decimal_macros::dec!(9000)),
                },
            ],
        }
    }
}

/// Candidate admission floors applied before global optimization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortfolioAdmission {
    /// Minimum nominal discounted expected net USD required for a tier to enter optimization.
    pub min_nominal_expected_net_usd: DecimalValue,
    /// Minimum ambiguity-robust discounted expected net USD required before optimization.
    pub min_robust_expected_net_usd: DecimalValue,
    /// Conservative lower bound on the probability of positive net USD, in basis points.
    pub min_profit_probability_bps: u32,
    /// Maximum calibrated probability-interval width admitted, in basis points.
    pub max_probability_interval_width_bps: u32,
    /// Additional executable-liquidity haircut applied to available depth, in basis points.
    pub liquidity_buffer_bps: u32,
}

impl Default for PortfolioAdmission {
    fn default() -> Self {
        Self {
            min_nominal_expected_net_usd: DecimalValue::new(rust_decimal_macros::dec!(1)),
            min_robust_expected_net_usd: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
            min_profit_probability_bps: 5_200,
            max_probability_interval_width_bps: 2_000,
            liquidity_buffer_bps: 1_000,
        }
    }
}

/// Durable condition evaluator cadence, lease, and bounded-pass policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryConditionWorkerConfig {
    /// Millisecond cadence of the safety backstop scan. Source notifications,
    /// book wakes, and clock deadlines remain the primary wake paths.
    pub backstop_interval_ms: u64,
    /// Milliseconds before a still-active instance becomes eligible for its
    /// next scheduled evaluation after one worker pass.
    pub next_evaluation_delay_ms: u64,
    /// Duration in seconds of an exclusive instance-processing lease.
    pub lease_duration_secs: u64,
    /// Renewal cadence in seconds for a held lease; must remain shorter than
    /// the lease duration so takeover is explicit and auditable.
    pub lease_renew_interval_secs: u64,
    /// Maximum number of due condition instances evaluated in one worker pass.
    pub pass_limit: usize,
    /// Maximum number of expired instances transitioned in one expiry sweep.
    pub expiry_batch_limit: u64,
}

impl Default for EntryConditionWorkerConfig {
    fn default() -> Self {
        Self {
            backstop_interval_ms: 1_000,
            next_evaluation_delay_ms: 1_000,
            lease_duration_secs: 15,
            lease_renew_interval_secs: 5,
            pass_limit: 256,
            expiry_batch_limit: 512,
        }
    }
}

/// Semi-auto approval policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SemiAutoConfig {
    /// Approval time-to-live in seconds.
    pub approval_ttl_secs: u64,
}

impl Default for SemiAutoConfig {
    fn default() -> Self {
        Self {
            approval_ttl_secs: 900,
        }
    }
}

/// Auto-execution policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AutoExecutionConfig {
    /// Maximum orders auto-created per report.
    pub max_orders_per_report: u32,
    /// Maximum total USD auto-executed per report.
    pub max_total_usd_per_report: DecimalValue,
}

impl Default for AutoExecutionConfig {
    fn default() -> Self {
        Self {
            max_orders_per_report: 0,
            max_total_usd_per_report: DecimalValue::new(rust_decimal_macros::dec!(0)),
        }
    }
}

/// Learning-to-rank objective used by governed weighted-model training.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchTrainingConfig {
    /// Ranking loss optimized inside each same-`as_of` cross-section.
    pub rank_loss: RankLossKind,
    /// Simplex optimizer policy.
    pub optimizer: TrainingOptimizerKind,
    /// Weight on lower-tail portfolio-return penalty.
    pub lambda_tail: DecimalValue,
    /// Lower-tail fraction for tail penalty (e.g. `0.10` = worst decile).
    pub tail_fraction: DecimalValue,
    /// Weight on mean per-tick allocation turnover.
    pub lambda_turnover: DecimalValue,
    /// L2 coefficient on `Σ weightᵢ²`.
    pub lambda_l2: DecimalValue,
    /// Truncation `k` for diagnostic NDCG@k (not part of the training loss).
    pub ndcg_k: u32,
    /// Truncation for score-derived `TopN` pseudo-portfolio used by tail/turnover.
    pub pseudo_top_n: u32,
}

impl Default for ResearchTrainingConfig {
    fn default() -> Self {
        Self {
            rank_loss: RankLossKind::default(),
            optimizer: TrainingOptimizerKind::default(),
            lambda_tail: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
            tail_fraction: DecimalValue::new(rust_decimal_macros::dec!(0.10)),
            lambda_turnover: DecimalValue::new(rust_decimal_macros::dec!(0.2)),
            lambda_l2: DecimalValue::new(rust_decimal_macros::dec!(0.01)),
            ndcg_k: 20,
            pseudo_top_n: 20,
        }
    }
}

/// Label-horizon-aware purge + embargo.
///
/// `embargo_pct` is the **only** knob — whether to purge by label horizon is
/// deliberately not configurable (see `quant_pivot_research::validation::purge`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationPurgeConfig {
    /// Embargo window as a fraction of the full timeline span.
    pub embargo_pct: DecimalValue,
}

impl Default for ResearchValidationPurgeConfig {
    fn default() -> Self {
        Self {
            embargo_pct: DecimalValue::new(rust_decimal_macros::dec!(0.02)),
        }
    }
}

/// Combinatorial Purged Cross-Validation partition config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationCpcvConfig {
    /// Number of contiguous timeline partitions (`N`).
    pub n_groups: u32,
    /// Number of partitions held out as the test set per combination (`k`).
    pub k_test: u32,
    /// Share of each outer fold's eligible training groups reserved for an
    /// inner purge/embargo-isolated calibration and scenario-residual fit.
    /// Integer basis points avoid a floating split boundary.
    pub nested_estimator_holdout_bps: u32,
    /// Initial minimum number of distinct timeline groups reserved for the
    /// nested estimator holdout. Four is the structural lower bound for two
    /// downstream populations, but it is not assumed to survive overlapping
    /// labels: each fold expands the chronological holdout only as far as its
    /// real label intervals require, evaluates every feasible calibration /
    /// scenario boundary after purge/embargo, and chooses the most balanced
    /// post-purge population while preserving the largest model-fit prefix.
    /// Model, calibration, and scenario fit must each retain at least two
    /// distinct decision-time groups; a fold with no feasible split fails
    /// closed without weakening purge, embargo, or population floors.
    pub nested_estimator_min_groups: u32,
}

impl Default for ResearchValidationCpcvConfig {
    fn default() -> Self {
        Self {
            n_groups: 8,
            k_test: 3,
            nested_estimator_holdout_bps: 2_000,
            nested_estimator_min_groups: 4,
        }
    }
}

impl ResearchValidationCpcvConfig {
    /// Number of complete full-timeline CPCV paths,
    /// `phi(N, k) = C(N - 1, k - 1)`.
    pub fn expected_path_count(&self) -> Result<u64, String> {
        let n = self
            .n_groups
            .checked_sub(1)
            .ok_or_else(|| "cpcv.n_groups must be positive".to_owned())?;
        let k = self
            .k_test
            .checked_sub(1)
            .ok_or_else(|| "cpcv.k_test must be positive".to_owned())?;
        Self::binomial(n, k)
    }

    /// Number of purge/train/evaluate combinations, `C(N, k)`.
    pub fn expected_combination_count(&self) -> Result<u64, String> {
        Self::binomial(self.n_groups, self.k_test)
    }

    fn binomial(n: u32, k: u32) -> Result<u64, String> {
        if k > n {
            return Err(format!("cannot compute C({n}, {k}) with k > n"));
        }
        let n = u64::from(n);
        let k = u64::from(k).min(n - u64::from(k));
        let mut value: u128 = 1;
        for index in 0..k {
            value = value
                .checked_mul(u128::from(n - index))
                .ok_or_else(|| format!("C({n}, {k}) overflowed u128"))?
                / u128::from(index + 1);
        }
        u64::try_from(value).map_err(|error| format!("C({n}, {k})={value} exceeds u64: {error}"))
    }
}

/// Governed hyperparameter trial grid for the multi-testing-corrected
/// Deflated Sharpe Ratio + CSCV/PBO.
///
/// Weighted-factor and classical families share `max_trials` but expand
/// different Cartesian dimensions — the CPCV port selects the matching
/// grid from `model_family` at run time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationTrialsConfig {
    /// Multipliers applied to the base `lambda_tail`/`lambda_turnover`/`lambda_l2`
    /// (`WeightedFactor` trial grid).
    pub lambda_multipliers: Vec<DecimalValue>,
    /// Rank-loss variants to cross with each lambda multiplier (`WeightedFactor`).
    pub rank_loss_kinds: Vec<RankLossKind>,
    /// Multipliers applied to base `ForestParams.n_trees` (classical trial grid).
    pub forest_n_trees_multipliers: Vec<DecimalValue>,
    /// Multipliers applied to base `LinearParams.alpha` (classical trial grid).
    pub linear_alpha_multipliers: Vec<DecimalValue>,
    /// Hard cap on the number of trials the selected family grid may expand to.
    pub max_trials: u32,
}

impl Default for ResearchValidationTrialsConfig {
    fn default() -> Self {
        Self {
            lambda_multipliers: vec![
                DecimalValue::new(rust_decimal_macros::dec!(0.25)),
                DecimalValue::new(rust_decimal_macros::dec!(0.5)),
                DecimalValue::new(rust_decimal_macros::dec!(0.75)),
                DecimalValue::new(rust_decimal_macros::dec!(1)),
                DecimalValue::new(rust_decimal_macros::dec!(1.25)),
                DecimalValue::new(rust_decimal_macros::dec!(1.5)),
                DecimalValue::new(rust_decimal_macros::dec!(2)),
                DecimalValue::new(rust_decimal_macros::dec!(3)),
            ],
            rank_loss_kinds: vec![
                RankLossKind::RankIcWeightedRanknet,
                RankLossKind::PairwiseRanknet,
            ],
            forest_n_trees_multipliers: vec![
                DecimalValue::new(rust_decimal_macros::dec!(0.5)),
                DecimalValue::new(rust_decimal_macros::dec!(1)),
                DecimalValue::new(rust_decimal_macros::dec!(2)),
            ],
            linear_alpha_multipliers: vec![
                DecimalValue::new(rust_decimal_macros::dec!(0.5)),
                DecimalValue::new(rust_decimal_macros::dec!(1)),
                DecimalValue::new(rust_decimal_macros::dec!(2)),
            ],
            max_trials: 32,
        }
    }
}

/// Combinatorially Symmetric Cross-Validation block config following
/// Bailey, Borwein, López de Prado, and Zhu (2014/2017), Algorithm 2.3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationPboConfig {
    /// Number of equal-length time blocks (`S`, must be even and within
    /// `4..=16`).
    pub block_count: u32,
}

impl Default for ResearchValidationPboConfig {
    fn default() -> Self {
        // Bailey et al. identify S=16 as a generally reasonable balance: it
        // yields 12,870 symmetric logits without allowing the evidence surface
        // to grow without bound.
        Self { block_count: 16 }
    }
}

/// hard/soft alpha-significance gate thresholds
/// (`research.validation.gates.*`) — distinct from the single-path risk
/// thresholds in `quality_gate.*` (`max_drawdown`/etc, unchanged).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationGatesConfig {
    /// Minimum number of complete, reconstructed CPCV paths (hard gate).
    /// This is deliberately distinct from `C(N, k)` fold combinations.
    pub min_cpcv_paths: u32,
    /// Minimum CPCV path-set median rank IC (hard gate; replaces the deleted
    /// single-path `quality_gate.min_rank_ic` soft threshold).
    pub rank_ic_min: DecimalValue,
    /// Target significance (`α`) the Deflated Sharpe Ratio must clear:
    /// `deflated_sharpe >= 1 - dsr_significance` (hard gate).
    pub dsr_significance: DecimalValue,
    /// Maximum tolerated Probability of Backtest Overfitting (hard gate).
    pub max_pbo: DecimalValue,
    /// Maximum tolerated single-path turnover (hard gate; risk/execution
    /// realism, reads the debug-view single-path `BacktestReport`, not the
    /// CPCV path set).
    pub max_turnover: DecimalValue,
    /// Minimum tolerated single-path tail loss, in bps (hard gate; `tail_loss`
    /// is typically negative, so this is a **floor**, not a ceiling — named
    /// accordingly, correcting the original design draft's ambiguous
    /// `max_tail_loss`).
    pub min_tail_loss_bps: DecimalValue,
}

impl Default for ResearchValidationGatesConfig {
    fn default() -> Self {
        Self {
            min_cpcv_paths: 21,
            rank_ic_min: DecimalValue::new(rust_decimal_macros::dec!(0.02)),
            dsr_significance: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
            // The reference CSCV paper describes 0.05 as the customary model
            // rejection boundary. A 0.5 threshold admits coin-flip selection.
            max_pbo: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
            max_turnover: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
            min_tail_loss_bps: DecimalValue::new(rust_decimal_macros::dec!(-500)),
        }
    }
}

/// Leakage-aware validation & overfitting-control methodology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchValidationConfig {
    pub purge: ResearchValidationPurgeConfig,
    pub cpcv: ResearchValidationCpcvConfig,
    pub trials: ResearchValidationTrialsConfig,
    pub pbo: ResearchValidationPboConfig,
    pub gates: ResearchValidationGatesConfig,
}

/// Runtime v1 operational limits for policy fitting.
///
/// Statistical methodology is versioned code and publication thresholds belong
/// exclusively to the immutable research profile. Keeping only resource and
/// evidence-window limits here prevents a second, mutable quality-gate truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyValidationConfig {
    /// Maximum complete candidates in one immutable experiment.
    pub max_candidates_per_experiment: u32,
    /// Minimum signed production latency-probe history in seconds.
    pub min_latency_profile_secs: u64,
}

impl PolicyValidationConfig {
    /// Frozen contiguous CPCV partition count.
    pub const CPCV_N_GROUPS: u32 = 8;
    /// Frozen test partitions per CPCV combination.
    pub const CPCV_K_TEST: u32 = 3;
    /// Frozen CSCV/PBO block count.
    pub const PBO_BLOCK_COUNT: u32 = 8;
    /// Number of complete CPCV paths implied by N=8, k=3.
    pub const COMPLETE_PATH_COUNT: u32 = 21;
    /// Number of CPCV folds implied by C(8, 3).
    pub const FOLD_COUNT: u32 = 56;
    /// One-sided utility confidence, expressed in basis points.
    pub const UTILITY_CONFIDENCE_BPS: u32 = 9_500;
    /// Mandatory production-latency stress multiplier.
    pub const LATENCY_STRESS_MULTIPLIER: u32 = 2;
}

impl Default for PolicyValidationConfig {
    fn default() -> Self {
        Self {
            max_candidates_per_experiment: 32,
            min_latency_profile_secs: 86_400,
        }
    }
}

/// Research plane configuration (training objective + validation methodology).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchConfig {
    /// Governed training objective for weighted buy/sell scorers.
    pub training: ResearchTrainingConfig,
    /// Governed leakage-aware validation & overfitting-control methodology.
    pub validation: ResearchValidationConfig,
    /// Executable L2 policy-fit operational limits.
    pub policy_validation: PolicyValidationConfig,
}
