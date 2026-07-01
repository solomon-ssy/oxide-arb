//! Runtime-config section structs grouped by document area.

use crate::{
    enums::{common::MarketCategory, factor::FactorFamily},
    runtime_config::wire::{
        AttributionPolicy, CapitalPolicy, CorrelationConfig, DecimalString, EntryOrderPolicy,
        ExecutionBreakerConfig, ExitMonitorPolicy, FactorWeights, FeatureFamily, FeatureNameRef,
        FeatureStalenessPolicy, KillSwitchPolicy, MissingFactorPolicy, ModelVersionRef,
        NotificationPolicies, PortfolioOptimizerConfig, ReconciliationPolicy, ReportDeliveryPolicy,
        ScheduleCadence, SettlementRedeemPolicy, SizingModelConfig,
    },
    types::{SchemaVersion, Usd},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    /// Maximum acceptable `ClickHouse` fact lag.
    pub max_fact_lag_secs: u64,
    /// Minimum visible book depth in USD.
    pub min_book_depth_usd: DecimalString,
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
            max_fact_lag_secs: 30,
            min_book_depth_usd: DecimalString::new("0"),
            reject_crossed_books: true,
            reject_empty_books: true,
            feature_staleness_policy: FeatureStalenessPolicy::RejectStaleRequired,
            max_stale_book_ratio_bps: 2_000,
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
    /// Momentum windows in seconds.
    pub momentum_windows_secs: Vec<u64>,
    /// Volatility windows in seconds.
    pub volatility_windows_secs: Vec<u64>,
    /// Order-book depth levels to inspect.
    pub depth_levels: Vec<u32>,
    /// Maximum concurrent per-market PIT resolves in the feature pipeline.
    pub max_concurrent_market_resolves: u32,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            feature_schema_version: SchemaVersion::FIRST,
            enabled_feature_families: vec![
                FeatureFamily::MarketMetadata,
                FeatureFamily::PriceBook,
                FeatureFamily::TimeSeries,
                FeatureFamily::Microstructure,
            ],
            required_features: Vec::new(),
            bar_windows_secs: vec![60, 300, 900],
            momentum_windows_secs: vec![300, 900, 3_600],
            volatility_windows_secs: vec![900, 3_600],
            depth_levels: vec![1, 3, 5],
            max_concurrent_market_resolves: 32,
        }
    }
}

/// Factor selection and weighted-scorer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorsConfig {
    /// Generic factor families enabled for online computation.
    ///
    /// Domain (`FactorFamily::Domain`) variants are routed by market category and
    /// must not appear here.
    pub enabled_factor_families: Vec<FactorFamily>,
    /// Factor weights keyed by factor name.
    pub factor_weights: FactorWeights,
    /// Minimum confidence for a factor to contribute to scoring.
    pub min_factor_confidence: DecimalString,
    /// Missing factor handling policy.
    pub missing_factor_policy: MissingFactorPolicy,
}

impl Default for FactorsConfig {
    fn default() -> Self {
        Self {
            enabled_factor_families: vec![FactorFamily::Liquidity, FactorFamily::Momentum],
            factor_weights: FactorWeights::default(),
            min_factor_confidence: DecimalString::new("0.50"),
            missing_factor_policy: MissingFactorPolicy::ZeroWeight,
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
    /// Minimum model confidence.
    pub min_model_confidence: DecimalString,
    /// Maximum age of a quality-gate report before model load is denied.
    /// Consumed by Phase 3.7 governance (`ModelQualityGate` / load-time deny), not by
    /// the 3.4 `ModelRunner` inference path.
    pub min_quality_gate_age_secs: u64,
    /// Prediction horizon used when authoring / training artifacts
    /// (`WeightedFactorModelArtifact.prediction_horizon_secs`). Online inference reads
    /// the frozen artifact field, not this config value (Phase 3.6 trainer writes it).
    pub prediction_horizon_secs: u64,
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
            min_model_confidence: DecimalString::new("0.50"),
            min_quality_gate_age_secs: 86_400,
            prediction_horizon_secs: 86_400,
            candidate_score_floor: DecimalString::new("0.00"),
            shadow_diff_threshold: DecimalString::new("0.10"),
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
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            max_book_staleness_ms: 300_000,
            min_exit_depth_usd: DecimalString::new("100"),
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
