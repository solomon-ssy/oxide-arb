//! Shared wire types and nested policy documents for runtime config.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Placeholder substituted for sensitive values on read surfaces.
pub const MASKED_SECRET: &str = "***";

/// Decimal wire value stored as a string to preserve exact operator input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct DecimalString {
    #[schemars(extend("x-format" = "decimal"))]
    pub value: String,
}

/// Runtime-config market-id list wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct MarketIdList {
    pub ids: Vec<String>,
}

/// Runtime-config model-version id reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ModelVersionRef {
    pub id: String,
}

impl DecimalString {
    /// Build a decimal string value from a static default.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl Default for DecimalString {
    fn default() -> Self {
        Self::new("0")
    }
}

/// Named policy for stale feature handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeatureStalenessPolicy {
    RejectStaleRequired,
    AllowDegraded,
}

/// Domain feature missing/null policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DomainFeaturePolicy {
    RejectMissingRequired,
    ImputeMissingOptional,
}

/// Missing factor handling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissingFactorPolicy {
    ZeroWeight,
    RejectCandidate,
}

/// Report delivery policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportDeliveryPolicy {
    StoreAndNotify,
    StoreOnly,
}

/// Confidence-to-size curve policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceSizeCurve {
    Linear,
    Step,
}

/// Drawdown multiplier policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DrawdownMultiplierPolicy {
    Fixed,
    Conservative,
}

/// Supported feature families for v3 feature generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFamily {
    Book,
    Liquidity,
    Momentum,
    Volatility,
}

/// Factor weights keyed by factor name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct FactorWeights {
    pub weights: BTreeMap<String, DecimalString>,
}

/// Entry order policy for recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EntryOrderPolicy {
    /// Maximum allowed entry-order slippage in basis points.
    pub max_slippage_bps: u32,
    /// Whether entry orders may use marketable order types.
    pub allow_market_orders: bool,
}

impl Default for EntryOrderPolicy {
    fn default() -> Self {
        Self {
            max_slippage_bps: 50,
            allow_market_orders: false,
        }
    }
}

/// Exit order policy for recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExitOrderPolicy {
    /// Whether generated exit orders must only reduce existing exposure.
    pub allow_reduce_only: bool,
    /// Maximum allowed exit-order slippage in basis points.
    pub max_slippage_bps: u32,
}

impl Default for ExitOrderPolicy {
    fn default() -> Self {
        Self {
            allow_reduce_only: true,
            max_slippage_bps: 100,
        }
    }
}

/// Admission gates for creating order intents from recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionAdmissionPolicy {
    /// Minimum recommendation score for intent admission.
    pub min_score: DecimalString,
    /// Minimum recommendation confidence for intent admission.
    pub min_confidence: DecimalString,
    /// Require fresh feature vectors before creating an intent.
    pub require_fresh_features: bool,
}

impl Default for ExecutionAdmissionPolicy {
    fn default() -> Self {
        Self {
            min_score: DecimalString::new("0.00"),
            min_confidence: DecimalString::new("0.50"),
            require_fresh_features: true,
        }
    }
}

/// Execution kill-switch policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct KillSwitchPolicy {
    /// Whether execution is globally disabled by policy.
    pub enabled: bool,
    /// Operator-visible reason for the kill switch.
    pub reason: Option<String>,
}

/// Capital policy for execution admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CapitalPolicy {
    /// Maximum USD that may be reserved by open execution intents.
    pub max_reserved_usd: DecimalString,
    /// Maximum number of open execution intents.
    pub max_open_intents: u32,
}

impl Default for CapitalPolicy {
    fn default() -> Self {
        Self {
            max_reserved_usd: DecimalString::new("0"),
            max_open_intents: 0,
        }
    }
}

/// Execution reconciliation policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ReconciliationPolicy {
    /// Whether execution reconciliation is enabled.
    pub enabled: bool,
    /// Reconciliation interval in seconds.
    pub interval_secs: u64,
}

impl Default for ReconciliationPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
        }
    }
}

/// Notification routing policy flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationPolicies {
    /// Notify operators when a recommendation report is published.
    pub report_published: bool,
    /// Notify operators when execution is halted.
    pub execution_halted: bool,
    /// Notify operators when runtime config is activated.
    pub config_activated: bool,
}

impl Default for NotificationPolicies {
    fn default() -> Self {
        Self {
            report_published: true,
            execution_halted: true,
            config_activated: true,
        }
    }
}
