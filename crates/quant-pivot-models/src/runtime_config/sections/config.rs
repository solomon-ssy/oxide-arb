//! Runtime-config section structs grouped by document area.

use crate::{
    enums::{common::MarketCategory, quant::QuantRuntimeMode},
    runtime_config::wire::{
        CapitalPolicy, ConfidenceSizeCurve, DecimalString, DomainFeaturePolicy,
        DrawdownMultiplierPolicy, EntryOrderPolicy, ExecutionAdmissionPolicy, ExitOrderPolicy,
        FactorWeights, FeatureFamily, FeatureStalenessPolicy, KillSwitchPolicy, MarketIdList,
        MissingFactorPolicy, ModelVersionRef, NotificationPolicies, ReconciliationPolicy,
        ReportDeliveryPolicy,
    },
    types::SchemaVersion,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Market selection selection policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SelectionConfig {
    /// Category slugs eligible for quant reports.
    pub enabled_categories: Vec<MarketCategory>,
    /// Explicitly excluded Polymarket condition ids.
    pub excluded_market_ids: MarketIdList,
    /// Explicitly included Polymarket condition ids.
    pub included_market_ids: MarketIdList,
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
            excluded_market_ids: MarketIdList::default(),
            included_market_ids: MarketIdList::default(),
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
    /// Whether degraded domain features can be used in reports.
    pub allow_degraded_domain_features: bool,
    /// Reject crossed books before feature generation.
    pub reject_crossed_books: bool,
    /// Reject empty books before feature generation.
    pub reject_empty_books: bool,
    /// Source delay applied to report generation for late facts.
    pub source_delay_secs: u64,
    /// Named policy for stale feature handling.
    pub feature_staleness_policy: FeatureStalenessPolicy,
}

impl Default for DataQualityConfig {
    fn default() -> Self {
        Self {
            max_book_age_ms: 5_000,
            max_fact_lag_secs: 30,
            min_book_depth_usd: DecimalString::new("0"),
            allow_degraded_domain_features: false,
            reject_crossed_books: true,
            reject_empty_books: true,
            source_delay_secs: 10,
            feature_staleness_policy: FeatureStalenessPolicy::RejectStaleRequired,
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
    /// Required feature names.
    pub required_features: Vec<String>,
    /// Domain feature missing/null policy.
    pub domain_feature_policy: DomainFeaturePolicy,
    /// Bar aggregation windows in seconds.
    pub bar_windows_secs: Vec<u64>,
    /// Momentum windows in seconds.
    pub momentum_windows_secs: Vec<u64>,
    /// Volatility windows in seconds.
    pub volatility_windows_secs: Vec<u64>,
    /// Order-book depth levels to inspect.
    pub depth_levels: Vec<u32>,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        Self {
            feature_schema_version: SchemaVersion::FIRST,
            enabled_feature_families: vec![FeatureFamily::Book, FeatureFamily::Liquidity],
            required_features: Vec::new(),
            domain_feature_policy: DomainFeaturePolicy::RejectMissingRequired,
            bar_windows_secs: vec![60, 300, 900],
            momentum_windows_secs: vec![300, 900, 3_600],
            volatility_windows_secs: vec![900, 3_600],
            depth_levels: vec![1, 3, 5],
        }
    }
}

/// Factor selection and weighted-scorer configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FactorsConfig {
    /// Enabled factor family names.
    pub enabled_factor_families: Vec<String>,
    /// Factor weights keyed by factor name.
    pub factor_weights: FactorWeights,
    /// Minimum confidence for a factor to contribute to scoring.
    pub min_factor_confidence: DecimalString,
    /// Missing factor handling policy.
    pub missing_factor_policy: MissingFactorPolicy,
    /// Published factor set id.
    pub published_factor_set_id: Option<String>,
    /// Shadow factor set id.
    pub shadow_factor_set_id: Option<String>,
}

impl Default for FactorsConfig {
    fn default() -> Self {
        Self {
            enabled_factor_families: vec!["liquidity".to_owned(), "momentum".to_owned()],
            factor_weights: FactorWeights::default(),
            min_factor_confidence: DecimalString::new("0.50"),
            missing_factor_policy: MissingFactorPolicy::ZeroWeight,
            published_factor_set_id: None,
            shadow_factor_set_id: None,
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
    /// Maximum age of quality gates before model use is denied.
    pub min_quality_gate_age_secs: u64,
    /// Prediction horizon in seconds.
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

/// Report schedules and payload sizing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReportsConfig {
    /// Configured report schedules.
    pub schedules: Vec<ReportScheduleConfig>,
    /// Default `TopN` size.
    pub default_top_n: u32,
    /// Maximum `TopN` size.
    pub max_top_n: u32,
    /// Report horizon in seconds.
    pub report_horizon_secs: u64,
    /// Whether empty reports are published with reason summaries.
    pub publish_empty_reports: bool,
    /// Report TTL in seconds.
    pub report_ttl_secs: u64,
    /// Whether ad-hoc report generation is enabled.
    pub ad_hoc_report_enabled: bool,
    /// Delivery policy name.
    pub delivery_policy: ReportDeliveryPolicy,
}

impl Default for ReportsConfig {
    fn default() -> Self {
        Self {
            schedules: vec![ReportScheduleConfig::default()],
            default_top_n: 20,
            max_top_n: 100,
            report_horizon_secs: 86_400,
            publish_empty_reports: true,
            report_ttl_secs: 3_600,
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
    /// Interval in seconds.
    pub interval_secs: u64,
    /// `TopN` size for this schedule.
    pub top_n: u32,
    /// Optional selection filter reference.
    pub market_filter_ref: Option<String>,
    /// Optional model version override.
    pub model_version_ref: Option<String>,
    /// Source delay in seconds.
    pub source_delay_secs: u64,
    /// Whether this schedule is enabled.
    pub enabled: bool,
}

impl Default for ReportScheduleConfig {
    fn default() -> Self {
        Self {
            schedule_id: "default_interval".to_owned(),
            interval_secs: 300,
            top_n: 20,
            market_filter_ref: None,
            model_version_ref: None,
            source_delay_secs: 10,
            enabled: true,
        }
    }
}

/// Portfolio budget and exposure constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PortfolioConfig {
    /// Total report budget in USD.
    pub total_budget_usd: DecimalString,
    /// Maximum USD allocated to one recommendation.
    pub max_single_recommendation_usd: DecimalString,
    /// Maximum USD exposure per market.
    pub max_market_exposure_usd: DecimalString,
    /// Maximum USD exposure per event.
    pub max_event_exposure_usd: DecimalString,
    /// Maximum USD exposure per category.
    pub max_category_exposure_usd: DecimalString,
    /// Maximum correlated exposure in USD.
    pub max_correlated_exposure_usd: DecimalString,
    /// Minimum useful recommendation size in USD.
    pub min_recommendation_usd: DecimalString,
    /// Maximum percentage of visible liquidity to consume.
    pub liquidity_usage_cap_pct: DecimalString,
    /// Named confidence-to-size curve.
    pub confidence_size_curve: ConfidenceSizeCurve,
    /// Drawdown multiplier policy.
    pub drawdown_multiplier: DrawdownMultiplierPolicy,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            total_budget_usd: DecimalString::new("0"),
            max_single_recommendation_usd: DecimalString::new("0"),
            max_market_exposure_usd: DecimalString::new("0"),
            max_event_exposure_usd: DecimalString::new("0"),
            max_category_exposure_usd: DecimalString::new("0"),
            max_correlated_exposure_usd: DecimalString::new("0"),
            min_recommendation_usd: DecimalString::new("0"),
            liquidity_usage_cap_pct: DecimalString::new("0.05"),
            confidence_size_curve: ConfidenceSizeCurve::Linear,
            drawdown_multiplier: DrawdownMultiplierPolicy::Fixed,
        }
    }
}

/// Optional execution policy rooted in recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    /// Governed runtime mode.
    pub runtime_mode: QuantRuntimeMode,
    /// Semi-auto approval policy.
    pub semi_auto: SemiAutoConfig,
    /// Auto-execution policy.
    pub auto_execution: AutoExecutionConfig,
    /// Entry order policy document.
    pub entry_order_policy: EntryOrderPolicy,
    /// Exit order policy document.
    pub exit_order_policy: ExitOrderPolicy,
    /// Admission policy document.
    pub admission: ExecutionAdmissionPolicy,
    /// Kill-switch policy document.
    pub kill_switch: KillSwitchPolicy,
    /// Capital policy document.
    pub capital: CapitalPolicy,
    /// Reconciliation policy document.
    pub reconciliation: ReconciliationPolicy,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            runtime_mode: QuantRuntimeMode::ReportOnly,
            semi_auto: SemiAutoConfig::default(),
            auto_execution: AutoExecutionConfig::default(),
            entry_order_policy: EntryOrderPolicy::default(),
            exit_order_policy: ExitOrderPolicy::default(),
            admission: ExecutionAdmissionPolicy::default(),
            kill_switch: KillSwitchPolicy::default(),
            capital: CapitalPolicy::default(),
            reconciliation: ReconciliationPolicy::default(),
        }
    }
}

/// Semi-auto approval policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SemiAutoConfig {
    /// Approval time-to-live in seconds.
    pub approval_ttl_secs: u64,
    /// Required role name for approval.
    pub required_role: String,
    /// Whether approvers may reduce order size.
    pub allow_size_reduction: bool,
}

impl Default for SemiAutoConfig {
    fn default() -> Self {
        Self {
            approval_ttl_secs: 900,
            required_role: "quant_operator".to_owned(),
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
    /// Require shadow validation before auto-execution.
    pub require_shadow_passed: bool,
}

impl Default for AutoExecutionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_orders_per_report: 0,
            max_total_usd_per_report: DecimalString::new("0"),
            min_score: DecimalString::new("0.00"),
            min_confidence: DecimalString::new("1.00"),
            require_shadow_passed: true,
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

/// Policy for all oracle sources being unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AllSourcesDownStrategy {
    DegradedReport,
    HaltExecution,
}

/// Settlement oracle source configuration retained for Polymarket data clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementOracleConfig {
    /// UMA oracle endpoint.
    pub uma_endpoint: Option<String>,
    /// Gamma fallback endpoint.
    pub gamma_endpoint: Option<String>,
    /// CTF fallback endpoint.
    pub ctf_endpoint: Option<String>,
    /// Strategy when every source is unavailable.
    pub all_sources_down_strategy: AllSourcesDownStrategy,
}

impl Default for SettlementOracleConfig {
    fn default() -> Self {
        Self {
            uma_endpoint: None,
            gamma_endpoint: None,
            ctf_endpoint: None,
            all_sources_down_strategy: AllSourcesDownStrategy::DegradedReport,
        }
    }
}

/// CTF redeem routing policy retained for the Polymarket API wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedeemRoutingPolicy {
    Standard,
    NegRisk,
}

/// Resolved CTF redeem routing plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedRedeemPlan {
    pub policy: RedeemRoutingPolicy,
    pub exchange: String,
}
