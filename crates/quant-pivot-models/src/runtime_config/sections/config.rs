//! Runtime-config section structs grouped by document area.

use crate::{
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::{FactorFamily, FactorNormalization},
    },
    runtime_config::wire::{
        AttributionPolicy, CapitalPolicy, CorrelationConfig, DecimalString, EntryOrderPolicy,
        ExecutionBreakerConfig, ExitMonitorPolicy, FactorWeights, FeatureFamily, FeatureNameRef,
        FeatureStalenessPolicy, KillSwitchPolicy, MissingFactorPolicy, ModelVersionRef,
        NeutralizeDimension, NotificationPolicies, PortfolioOptimizerConfig, ReconciliationPolicy,
        ReportDeliveryPolicy, ScheduleCadence, SettlementRedeemPolicy, SizingModelConfig,
        SmallCrossSectionPolicy,
    },
    types::{SchemaVersion, Usd},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Market selection selection policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SelectionConfig {
    /// Category slugs eligible for quant reports.
    pub enabled_categories: Vec<MarketCategory>,
    /// Minimum displayed liquidity in USD.
    pub min_liquidity_usd: DecimalString,
    /// Minimum 24h volume in USD.
    pub min_volume_24h_usd: DecimalString,
    /// Maximum allowed top-of-book spread in basis points.
    pub max_spread_bps: u32,
    /// Whether near-resolution markets may enter the selection.
    pub allow_near_resolution: bool,
    /// Minimum seconds until market resolution.
    pub min_time_to_resolution_secs: u64,
    /// Maximum seconds until market resolution.
    pub max_time_to_resolution_secs: u64,
    /// Hard cap on selected markets.
    pub max_selection_size: u32,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            enabled_categories: Vec::new(),
            min_liquidity_usd: DecimalString::new("0"),
            min_volume_24h_usd: DecimalString::new("0"),
            max_spread_bps: 2_500,
            allow_near_resolution: false,
            min_time_to_resolution_secs: 3_600,
            max_time_to_resolution_secs: 31_536_000,
            max_selection_size: 1_000,
        }
    }
}

/// Data quality thresholds for PIT features and facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
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
            max_book_age_ms: 5_000,
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

/// Structural feature-family windows (Phase 11.2.1).
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
    pub trade_tape_min_notional_usd: DecimalString,
    /// Minimum participant-address coverage ratio in `[0, 1]`.
    pub trade_tape_min_coverage_ratio: DecimalString,
}

impl Default for StructuralFeaturesConfig {
    fn default() -> Self {
        Self {
            shock_window_secs: 900,
            book_churn_window_secs: 900,
            trade_tape_window_secs: 86_400,
            trade_tape_min_unique_participants: 20,
            trade_tape_min_notional_usd: DecimalString::new("100.00"),
            trade_tape_min_coverage_ratio: DecimalString::new("0.95"),
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
    /// Required feature names (each must exist in the active feature schema).
    pub required_features: Vec<FeatureNameRef>,
    /// Bar aggregation windows in seconds.
    pub bar_windows_secs: Vec<u64>,
    /// Momentum feature-family windows and lags.
    pub momentum: MomentumFeaturesConfig,
    /// Volatility windows in seconds.
    pub volatility_windows_secs: Vec<u64>,
    /// Order-book depth levels to inspect.
    pub depth_levels: Vec<u32>,
    /// Structural feature-family windows (Phase 11.2.1).
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
            .chain(std::iter::once(&self.momentum.ema_slow_secs))
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
            feature_schema_version: SchemaVersion::new(5),
            enabled_feature_families: vec![
                FeatureFamily::MarketMetadata,
                FeatureFamily::PriceBook,
                FeatureFamily::TimeSeries,
                FeatureFamily::Microstructure,
                FeatureFamily::Structural,
                FeatureFamily::Domain,
            ],
            required_features: Vec::new(),
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
    pub winsor_p: Option<DecimalString>,
    /// Sigma clamp bound (`WinsorizedZScore` only), `> 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clamp_sigma: Option<DecimalString>,
    /// Lower semantic bound mapped to 0 (`MinMax` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<DecimalString>,
    /// Upper semantic bound mapped to 1 (`MinMax` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<DecimalString>,
}

/// Cross-sectional normalization parameters (no magic constants in code).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorNormalizationConfig {
    /// Default winsorize percentile in `(0, 0.5)` for `WinsorizedZScore` factors.
    pub default_winsor_p: DecimalString,
    /// Default sigma clamp bound for `WinsorizedZScore` factors, `> 0`.
    pub default_clamp_sigma: DecimalString,
    /// Per-factor overrides keyed by stable factor name.
    pub per_factor: BTreeMap<String, PerFactorNormalization>,
}

impl Default for FactorNormalizationConfig {
    fn default() -> Self {
        // data_quality is already a semantic [0, 1] score → MinMax identity.
        let mut per_factor = BTreeMap::new();
        per_factor.insert(
            "data_quality".to_owned(),
            PerFactorNormalization {
                method: FactorNormalization::MinMax,
                winsor_p: None,
                clamp_sigma: None,
                min: Some(DecimalString::new("0")),
                max: Some(DecimalString::new("1")),
            },
        );
        Self {
            default_winsor_p: DecimalString::new("0.01"),
            default_clamp_sigma: DecimalString::new("3"),
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
    /// Rolling lookback (seconds) for the `HistoricalQuantile` policy.
    pub historical_lookback_secs: u64,
}

impl Default for FactorCrossSectionConfig {
    fn default() -> Self {
        Self {
            min_size: 5,
            small_cross_section_policy: SmallCrossSectionPolicy::Indeterminate,
            historical_lookback_secs: 86_400,
        }
    }
}

/// Factor orthogonalization / collinearity policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorOrthogonalizeConfig {
    /// Maximum tolerated absolute pairwise Spearman correlation between factors.
    pub max_correlation: DecimalString,
    /// Dimensions to neutralize each factor against before normalization.
    pub neutralize_by: Vec<NeutralizeDimension>,
}

impl Default for FactorOrthogonalizeConfig {
    fn default() -> Self {
        Self {
            max_correlation: DecimalString::new("0.90"),
            neutralize_by: Vec::new(),
        }
    }
}

/// Shock-gated reversal factor parameters (Phase 11.2.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReversalAfterShockConfig {
    /// Shock threshold `k`: reversal only fires when `|ret| / realized_vol > k`.
    pub shock_k: DecimalString,
    /// Cap on the reported shock magnitude (bounds an extreme normalized signal).
    pub shock_cap: DecimalString,
}

impl Default for ReversalAfterShockConfig {
    fn default() -> Self {
        Self {
            shock_k: DecimalString::new("2.5"),
            shock_cap: DecimalString::new("6"),
        }
    }
}

/// Neg-risk structural factor parameters (Phase 11.2.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NegRiskStructuralConfig {
    /// Minimum resolved YES legs for a neg-risk structural factor to compute
    /// (below this the factor is `Indeterminate`, never a silent value).
    pub min_legs: u32,
}

impl Default for NegRiskStructuralConfig {
    fn default() -> Self {
        Self { min_legs: 3 }
    }
}

/// Favorite-longshot bias factor parameters (Phase 11.2.1).
///
/// `bias_table_ref` points at a fitted [`FavoriteLongshotBiasTableId`] artifact
/// (as its UUID string); `None` disables the factor (it stays inert — never a
/// fabricated constant). The sample-count gates fail closed on thin data.
///
/// [`FavoriteLongshotBiasTableId`]: crate::types::FavoriteLongshotBiasTableId
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FavoriteLongshotConfig {
    /// Active fitted bias-table artifact id (UUID string), or `None` (inert).
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
    pub ci_confidence: DecimalString,
    /// Absolute `|IC|` floor a curve must additionally clear (the significance
    /// test is a Student-t on the correlation; this is a belt-and-suspenders
    /// magnitude floor).
    pub ic_significance_min: DecimalString,
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
            ci_confidence: DecimalString::new("0.95"),
            ic_significance_min: DecimalString::new("0.02"),
            fit_sample_stride_secs: 21_600,
        }
    }
}

/// Structural factor-plane configuration (Phase 11.2.1).
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
    /// significant (the hard publish-gate is Phase 11.5).
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
    pub gini_weight: DecimalString,
    /// Weight on largest single-participant notional share (CR1).
    pub cr1_share_weight: DecimalString,
    /// Weight on participant notional HHI.
    pub hhi_weight: DecimalString,
}

impl Default for ParticipantConcentrationConfig {
    fn default() -> Self {
        Self {
            gini_weight: DecimalString::new("0.50"),
            cr1_share_weight: DecimalString::new("0.30"),
            hhi_weight: DecimalString::new("0.20"),
        }
    }
}

/// Factor selection and weighted-scorer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorsConfig {
    /// Config-selectable factor families enabled for online computation
    /// (generic + structural).
    ///
    /// Vertical/domain factor families are routed by market category and must
    /// not appear here.
    pub enabled_factor_families: Vec<FactorFamily>,
    /// Factor weights keyed by factor name.
    pub factor_weights: FactorWeights,
    /// Minimum confidence for a factor to contribute to scoring.
    pub min_factor_confidence: DecimalString,
    /// Missing factor handling policy.
    pub missing_factor_policy: MissingFactorPolicy,
    /// Cross-sectional normalization parameters.
    pub normalization: FactorNormalizationConfig,
    /// Small-cross-section / cross-section policy.
    pub cross_section: FactorCrossSectionConfig,
    /// Orthogonalization / collinearity policy.
    pub orthogonalize: FactorOrthogonalizeConfig,
    /// Structural factor-plane parameters (Phase 11.2.1).
    pub structural: StructuralFactorsConfig,
}

impl Default for FactorsConfig {
    fn default() -> Self {
        // All generic families plus the platform-internal structural plane are
        // enabled by default: diversity is the baseline, weighting is learned
        // (11.4). Domain families are routed by category and never appear here.
        let mut enabled_factor_families = FactorFamily::ALL_GENERIC.to_vec();
        enabled_factor_families.push(FactorFamily::Structural);
        Self {
            enabled_factor_families,
            factor_weights: FactorWeights::default(),
            min_factor_confidence: DecimalString::new("0.50"),
            missing_factor_policy: MissingFactorPolicy::ZeroWeight,
            normalization: FactorNormalizationConfig::default(),
            cross_section: FactorCrossSectionConfig::default(),
            orthogonalize: FactorOrthogonalizeConfig::default(),
            structural: StructuralFactorsConfig::default(),
        }
    }
}

/// Cross-check policy between the crypto feature source (Binance) and the
/// settlement oracle (Chainlink) — Phase 11.2.2 §3.6.6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CryptoCrossCheckConfig {
    /// Maximum tolerated |Binance − Chainlink| basis, in basis points of the
    /// oracle price. A wider observed basis raises the risk flag and marks the
    /// linkage for review; it never fabricates or clamps a feature value.
    pub max_basis_bps: u32,
}

impl Default for CryptoCrossCheckConfig {
    fn default() -> Self {
        Self { max_basis_bps: 50 }
    }
}

/// Crypto external-vertical parameters (Phase 11.2.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CryptoDomainConfig {
    /// Source visibility delay (seconds) applied to domain-observation PIT
    /// reads: only rows with `event_time <= as_of - source_delay` are visible.
    pub source_delay_secs: u64,
    /// Days of 1m kline history the ingest worker backfills on bootstrap.
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
            source_delay_secs: 5,
            backfill_days: 90,
            momentum_window_secs: 3_600,
            volatility_window_secs: 3_600,
            cross_check: CryptoCrossCheckConfig::default(),
        }
    }
}

/// External-vertical domain plane (category-routed; Phase 11.2.2).
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
        }
    }
}

impl Default for DomainConfig {
    fn default() -> Self {
        let mut enabled_by_family = BTreeMap::new();
        enabled_by_family.insert(DomainFamily::Crypto, true);
        Self {
            enabled_by_family,
            crypto: CryptoDomainConfig::default(),
        }
    }
}

/// Active and shadow model references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ModelConfig {
    /// Active published model version id.
    pub active_model_version_id: Option<ModelVersionRef>,
    /// Shadow model version id.
    pub shadow_model_version_id: Option<ModelVersionRef>,
    /// Active published Sell-side hold-vs-exit scorer version (Phase 06.1). The
    /// opportunistic-Sell exit evaluator loads this; a distinct pointer from
    /// `active_model_version_id` so Buy and Sell models are governed separately.
    pub active_exit_model_version_id: Option<ModelVersionRef>,
    /// Category-specific Buy-side model pointers (Phase 11.2.2 `ModelRouting`).
    ///
    /// A market whose category has a pointer here scores through that artifact
    /// (which may consume the category's domain slice); categories without a
    /// pointer — or whose artifact is unavailable — fall back to the generic
    /// `active_model_version_id`. Governed exactly like the active/shadow
    /// pointers (versioned config + activation audit).
    pub category_model_pointers: BTreeMap<MarketCategory, ModelVersionRef>,
    /// Minimum model confidence.
    pub min_model_confidence: DecimalString,
    /// Maximum age of a quality-gate report before model load is denied.
    /// Consumed by Phase 3.7 governance (`ModelQualityGate` / load-time deny), not by
    /// the 3.4 `ModelRunner` inference path.
    pub min_quality_gate_age_secs: u64,
    /// Minimum candidate score to enter portfolio pruning.
    pub candidate_score_floor: DecimalString,
    /// Shadow/live diff threshold.
    pub shadow_diff_threshold: DecimalString,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            active_model_version_id: None,
            shadow_model_version_id: None,
            active_exit_model_version_id: None,
            category_model_pointers: BTreeMap::new(),
            min_model_confidence: DecimalString::new("0.50"),
            min_quality_gate_age_secs: 86_400,
            candidate_score_floor: DecimalString::new("0.00"),
            shadow_diff_threshold: DecimalString::new("0.10"),
        }
    }
}

/// Sell-side hold-vs-exit quality-gate thresholds (Phase 06.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SellQualityGateConfig {
    /// Minimum `ExitDecision` sample count for a sell scorer to clear the gate.
    pub min_sample_count: u64,
    /// Minimum sell-side label coverage in `[0, 1]`.
    pub min_label_coverage: DecimalString,
    /// Minimum (soft) exit-alpha rank IC in `[-1, 1]`.
    pub min_exit_alpha_rank_ic: DecimalString,
    /// Minimum fraction of `ExitDecision` rows simulated from full L2 books.
    pub min_l2_book_fidelity_ratio: DecimalString,
    /// Maximum fraction of `ExitDecision` rows using microstructure fallback.
    pub max_fallback_ratio: DecimalString,
}

impl Default for SellQualityGateConfig {
    fn default() -> Self {
        Self {
            min_sample_count: 200,
            min_label_coverage: DecimalString::new("0.60"),
            min_exit_alpha_rank_ic: DecimalString::new("0.05"),
            min_l2_book_fidelity_ratio: DecimalString::new("0.50"),
            max_fallback_ratio: DecimalString::new("0.50"),
        }
    }
}

/// Governed quality-gate thresholds (Phase 3.7).
///
/// Hot-reloadable knobs consumed by the model quality gate and the publish /
/// rollback / dataset-promotion governance. Decimal-valued thresholds are
/// stored as [`DecimalString`] (lossless), matching every other money /
/// probability config field; the core governance layer parses them into the
/// research `QualityGateThresholds`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct QualityGateConfig {
    /// Minimum resolved sample count for a model / dataset to clear the gate.
    pub min_sample_count: u64,
    /// Minimum label coverage in `[0, 1]`.
    pub min_label_coverage: DecimalString,
    /// Minimum critical-feature (build) coverage in `[0, 1]`.
    pub min_critical_feature_coverage: DecimalString,
    /// Maximum tolerated backtest drawdown in `[0, 1]`.
    pub max_drawdown: DecimalString,
    /// Minimum liquidity-exit feasibility in `[0, 1]` (auto-execution gate).
    pub min_liquidity_exit_feasibility: DecimalString,
    /// Minimum shadow overlap stability in `[0, 1]` (publish gate).
    pub min_shadow_overlap_stability: DecimalString,
    /// Minimum (soft) rank IC; `<=` raises a soft warning.
    pub min_rank_ic: DecimalString,
    /// Maximum (soft) per-category sample concentration in `[0, 1]`.
    pub max_category_concentration: DecimalString,
    /// Minimum shadow comparison window (seconds) required before publish.
    pub required_shadow_window_secs: u64,
    /// Sell-side hold-vs-exit scorer thresholds (Phase 06.1).
    pub sell: SellQualityGateConfig,
}

impl Default for QualityGateConfig {
    fn default() -> Self {
        Self {
            min_sample_count: 500,
            min_label_coverage: DecimalString::new("0.70"),
            min_critical_feature_coverage: DecimalString::new("0.95"),
            max_drawdown: DecimalString::new("0.30"),
            min_liquidity_exit_feasibility: DecimalString::new("0.90"),
            min_shadow_overlap_stability: DecimalString::new("0.60"),
            min_rank_ic: DecimalString::new("0.00"),
            max_category_concentration: DecimalString::new("0.60"),
            required_shadow_window_secs: 86_400,
            sell: SellQualityGateConfig::default(),
        }
    }
}

/// Offline training-dataset build parameters (Phase 3.5+).
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
    pub min_exit_depth_usd: DecimalString,
    /// Minimum book-derived liquidity (combined visible USD depth) a market must
    /// show at an `as_of` to enter the point-in-time selection during an offline
    /// dataset build.
    ///
    /// The offline plane has no Gamma `liquidity_usd`/`volume_24h` history, so the
    /// online selection funnel is replayed with book depth as the liquidity proxy
    /// (and the volume floor skipped). This is that depth floor — frozen with the
    /// runtime config and captured in `dataset_hash` for reproducibility. It is a
    /// book-depth quantity, deliberately distinct from the Gamma-calibrated online
    /// `selection.min_liquidity_usd`.
    pub min_selection_depth_usd: DecimalString,
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            max_book_staleness_ms: 300_000,
            min_exit_depth_usd: DecimalString::new("100"),
            min_selection_depth_usd: DecimalString::new("500"),
        }
    }
}

impl TrainingConfig {
    /// Resolve [`TrainingConfig::min_exit_depth_usd`] into a typed [`Usd`] value.
    pub fn min_exit_depth_usd_typed(&self) -> Result<Usd, String> {
        use rust_decimal::Decimal;
        self.min_exit_depth_usd
            .value
            .parse::<Decimal>()
            .map(Usd::new)
            .map_err(|error| format!("training.min_exit_depth_usd is not a valid decimal: {error}"))
    }

    /// Resolve [`TrainingConfig::min_selection_depth_usd`] into a typed [`Usd`].
    pub fn min_selection_depth_usd_typed(&self) -> Result<Usd, String> {
        use rust_decimal::Decimal;
        self.min_selection_depth_usd
            .value
            .parse::<Decimal>()
            .map(Usd::new)
            .map_err(|error| {
                format!("training.min_selection_depth_usd is not a valid decimal: {error}")
            })
    }
}

/// Report schedules and payload sizing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReportsConfig {
    /// Configured report schedules.
    pub schedules: Vec<ReportScheduleConfig>,
    /// Maximum `TopN` size (hard upper bound for every schedule and ad-hoc run).
    pub max_top_n: u32,
    /// Fallback prediction horizon (seconds), used **only** when the model
    /// provides no per-candidate `suggested_horizon_secs` (classical / non-ML
    /// runs). The per-recommendation validity is otherwise data-driven from the
    /// model's frozen horizon capped by the market's time-to-resolution; this is
    /// never a flat TTL applied uniformly.
    pub fallback_horizon_secs: u64,
    /// Whether empty reports are published with reason summaries.
    pub publish_empty_reports: bool,
    /// Entry-window ratio in `(0, 1]`: a recommendation's entry-by deadline is
    /// `as_of + effective_horizon * entry_window_ratio`. `0.5` enters only while
    /// at least half the signal's edge remains (the half-life point); the
    /// time-stop / exit still uses the full effective horizon.
    pub entry_window_ratio: DecimalString,
    /// Whether ad-hoc report generation is enabled.
    pub ad_hoc_report_enabled: bool,
    /// Delivery policy name.
    pub delivery_policy: ReportDeliveryPolicy,
}

impl Default for ReportsConfig {
    fn default() -> Self {
        Self {
            schedules: vec![ReportScheduleConfig::default()],
            max_top_n: 100,
            fallback_horizon_secs: 86_400,
            publish_empty_reports: true,
            entry_window_ratio: DecimalString::new("0.5"),
            ad_hoc_report_enabled: false,
            delivery_policy: ReportDeliveryPolicy::StoreAndNotify,
        }
    }
}

/// One report schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReportScheduleConfig {
    /// Stable schedule id.
    pub schedule_id: String,
    /// How often this schedule fires (fixed interval or cron).
    pub cadence: ScheduleCadence,
    /// `TopN` size for this schedule.
    pub top_n: u32,
    /// Source delay in seconds.
    pub source_delay_secs: u64,
    /// Whether this schedule is enabled.
    pub enabled: bool,
}

impl Default for ReportScheduleConfig {
    fn default() -> Self {
        Self {
            schedule_id: "default_interval".to_owned(),
            cadence: ScheduleCadence::default(),
            top_n: 20,
            source_delay_secs: 10,
            enabled: true,
        }
    }
}

/// Portfolio policy: budget governance, exposure constraints, and sizing model.
///
/// Policy (limits) only — never account state. Real equity / positions come
/// from the account snapshot, never from here (see 04.0 §6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PortfolioConfig {
    /// Capital budget governance caps.
    pub budget: PortfolioBudget,
    /// Exposure / liquidity constraints.
    pub constraints: PortfolioConstraints,
    /// Position-sizing model.
    pub sizing: SizingModelConfig,
    /// Portfolio optimizer (`good_lp` LP/MILP) policy.
    pub optimizer: PortfolioOptimizerConfig,
}

/// Capital budget governance caps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PortfolioBudget {
    /// Maximum deployable capital (governance cap, all modes).
    ///
    /// `equity = min(real net-liquidation value, total_budget_usd)`; this value
    /// **never** stands in for equity itself.
    pub total_budget_usd: DecimalString,
    /// Minimum useful recommendation size in USD.
    pub min_recommendation_usd: DecimalString,
    /// Maximum USD allocated to one recommendation.
    pub max_single_recommendation_usd: DecimalString,
}

impl Default for PortfolioBudget {
    fn default() -> Self {
        Self {
            total_budget_usd: DecimalString::new("0"),
            min_recommendation_usd: DecimalString::new("0"),
            max_single_recommendation_usd: DecimalString::new("0"),
        }
    }
}

/// Exposure and liquidity constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PortfolioConstraints {
    /// Maximum USD exposure per market.
    pub max_market_exposure_usd: DecimalString,
    /// Maximum USD exposure per event.
    pub max_event_exposure_usd: DecimalString,
    /// Maximum USD exposure per category.
    pub max_category_exposure_usd: DecimalString,
    /// Maximum correlated exposure in USD.
    pub max_correlated_exposure_usd: DecimalString,
    /// Maximum fraction of visible liquidity an allocation may consume.
    pub liquidity_usage_cap_pct: DecimalString,
    /// Correlation-cluster estimation policy gating `max_correlated_exposure_usd`.
    pub correlation: CorrelationConfig,
}

impl Default for PortfolioConstraints {
    fn default() -> Self {
        Self {
            max_market_exposure_usd: DecimalString::new("0"),
            max_event_exposure_usd: DecimalString::new("0"),
            max_category_exposure_usd: DecimalString::new("0"),
            max_correlated_exposure_usd: DecimalString::new("0"),
            liquidity_usage_cap_pct: DecimalString::new("0.05"),
            correlation: CorrelationConfig::default(),
        }
    }
}

/// Optional execution policy rooted in recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Semi-auto approval policy.
    pub semi_auto: SemiAutoConfig,
    /// Auto-execution policy.
    pub auto_execution: AutoExecutionConfig,
    /// Entry order policy document.
    pub entry_order_policy: EntryOrderPolicy,
    /// Exit-monitor cadence + signal-degradation policy.
    pub exit_monitor: ExitMonitorPolicy,
    /// Kill-switch policy document.
    pub kill_switch: KillSwitchPolicy,
    /// Capital policy document.
    pub capital: CapitalPolicy,
    /// Reconciliation policy document.
    pub reconciliation: ReconciliationPolicy,
    /// On-chain settlement redemption policy.
    pub settlement_redeem: SettlementRedeemPolicy,
    /// Recommendation attribution worker policy.
    pub attribution: AttributionPolicy,
    /// Execution-breaker thresholds (venue health + auto kill-switch trip).
    pub breaker: ExecutionBreakerConfig,
}

/// Semi-auto approval policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SemiAutoConfig {
    /// Approval time-to-live in seconds.
    pub approval_ttl_secs: u64,
    /// Whether approvers may reduce order size.
    pub allow_size_reduction: bool,
}

impl Default for SemiAutoConfig {
    fn default() -> Self {
        Self {
            approval_ttl_secs: 900,
            allow_size_reduction: true,
        }
    }
}

/// Auto-execution policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AutoExecutionConfig {
    /// Whether auto-execution policy may approve intents.
    pub enabled: bool,
    /// Maximum orders auto-created per report.
    pub max_orders_per_report: u32,
    /// Maximum total USD auto-executed per report.
    pub max_total_usd_per_report: DecimalString,
    /// Minimum score for auto-execution.
    pub min_score: DecimalString,
    /// Minimum confidence for auto-execution.
    pub min_confidence: DecimalString,
}

impl Default for AutoExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_orders_per_report: 0,
            max_total_usd_per_report: DecimalString::new("0"),
            min_score: DecimalString::new("0.00"),
            min_confidence: DecimalString::new("1.00"),
        }
    }
}

/// Operator notification channels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationConfig {
    /// Telegram notification configuration.
    pub telegram: TelegramNotificationConfig,
    /// Webhook notification configuration.
    pub webhook: WebhookNotificationConfig,
    /// Notification policy flags keyed by event name.
    pub policies: NotificationPolicies,
}

/// Telegram notification configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct TelegramNotificationConfig {
    /// Telegram bot token.
    #[schemars(extend("x-sensitive" = true))]
    pub bot_token: String,
    /// Telegram chat id.
    pub chat_id: String,
}

/// Webhook notification configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookNotificationConfig {
    /// Webhook URL.
    #[schemars(extend("x-sensitive" = true))]
    pub url: String,
}

/// Research plane configuration (training objective + validation methodology).
///
/// Reserved skeleton frozen at schema v11. Later Phase 11 sub-phases add fields
/// here without a further schema bump:
/// - 11.4 adds the training-objective (learning-to-rank + downside/turnover) knobs;
/// - 11.5 adds the leakage-aware validation (purged/embargo/CPCV, DSR/PBO) knobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchConfig {}

/// Research-feedback plane configuration (attribution feedback + auto-retraining).
///
/// Reserved skeleton frozen at schema v11; 11.9 adds the attribution-feedback,
/// drift-triggered retraining, and champion-challenger knobs without a bump.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct FeedbackConfig {}
