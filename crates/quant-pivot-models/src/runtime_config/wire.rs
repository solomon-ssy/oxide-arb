//! Shared wire types and nested policy documents for runtime config.

use quant_pivot_error::QuantError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::types::ModelVersionId;

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

/// Runtime-config model-version id reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ModelVersionRef {
    pub id: String,
}

impl TryFrom<&ModelVersionRef> for ModelVersionId {
    type Error = QuantError;

    fn try_from(reference: &ModelVersionRef) -> Result<Self, Self::Error> {
        use std::str::FromStr;

        Self::from_str(reference.id.trim()).map_err(|error| {
            Self::Error::config(format!(
                "invalid model_version_id `{}`: {error}",
                reference.id
            ))
        })
    }
}

/// Runtime-config feature-name reference (wire label for a governed feature).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct FeatureNameRef {
    /// Stable feature identifier (must exist in the active [`FeatureSchema`]).
    pub name: String,
}

impl FeatureNameRef {
    /// Build a feature-name reference from a wire label.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
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

/// Fractional-Kelly position-sizing parameters for the portfolio planner.
///
/// Kelly is the **only** sizing model: it is the single edge-driven optimal-growth
/// sizer, and an edge-free confidence-to-size curve has no place in a capital
/// system (it would deploy capital regardless of expected value). `confidence`
/// is an evidence-quality measure, **not** a calibrated win probability, so it
/// never stands in for `q`; `q` is derived from the candidate's expected return,
/// downside, and target reward multiple, while `confidence` enters only as an
/// estimation-uncertainty shrinkage on the Kelly fraction (`confidence_weighting`)
/// — the production-standard mitigation for Kelly's sensitivity to edge
/// mis-estimation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SizingModelConfig {
    /// Fraction of full Kelly to apply, in `(0, 1]` (half-Kelly ≈ `0.5`).
    pub kelly_fraction: DecimalString,
    /// Maximum single-position size as a fraction of equity (`(0, 1]`).
    pub max_position_pct: DecimalString,
    /// Target reward-to-risk multiple `R` (`> 0`): the target gain is
    /// `R × downside`, fixing the binary bet structure so the win probability
    /// `q = (E[r] + l) / (g + l)` is recoverable from the candidate's expected
    /// return `E[r]` and downside `l`.
    pub target_reward_multiple: DecimalString,
    /// Confidence-driven shrinkage of the Kelly fraction (estimation
    /// uncertainty): `confidence` high → near fractional Kelly, low → compressed.
    pub confidence_weighting: ConfidenceSizeCurve,
    /// Drawdown-driven scaling policy.
    pub drawdown_scaling: DrawdownMultiplierPolicy,
}

impl Default for SizingModelConfig {
    fn default() -> Self {
        Self {
            kelly_fraction: DecimalString::new("0.5"),
            max_position_pct: DecimalString::new("0.1"),
            target_reward_multiple: DecimalString::new("2.0"),
            confidence_weighting: ConfidenceSizeCurve::Linear,
            drawdown_scaling: DrawdownMultiplierPolicy::Fixed,
        }
    }
}

/// How often a report schedule fires.
///
/// Tagged on `kind`. The cron variant is parsed/scheduled by the 04.3 runner;
/// 04.0 validation only checks structural validity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleCadence {
    /// Fixed interval in seconds (`> 0`).
    Interval {
        /// Interval between fires, in seconds.
        interval_secs: u64,
    },
    /// 6-field cron expression with an optional IANA timezone.
    Cron {
        /// 6-field cron expression.
        expr: String,
        /// Optional IANA timezone (e.g. `America/New_York`).
        timezone: Option<String>,
    },
}

impl Default for ScheduleCadence {
    fn default() -> Self {
        Self::Interval { interval_secs: 300 }
    }
}

/// Supported feature families for v3 feature generation.
///
/// One family ≈ one feature-builder group. The set gates which groups the
/// feature plane computes (`features.enabled_feature_families`) and tags each
/// `FeatureSpec` in the research schema registry, so config and the compute
/// schema share a single, precise taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFamily {
    /// Gamma market/event metadata (category, resolution timing, neg-risk, …).
    MarketMetadata,
    /// Top-of-book price and depth structure.
    PriceBook,
    /// Windowed return / volatility / momentum / trend features.
    TimeSeries,
    /// Order-flow microstructure (quote rate, churn, queue depletion, …).
    Microstructure,
    /// Vertical/domain-specific features (sports/politics/crypto/weather/geo).
    Domain,
}

impl FeatureFamily {
    /// Every feature family in declaration order.
    pub const ALL: [Self; 5] = [
        Self::MarketMetadata,
        Self::PriceBook,
        Self::TimeSeries,
        Self::Microstructure,
        Self::Domain,
    ];

    /// The stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketMetadata => "market_metadata",
            Self::PriceBook => "price_book",
            Self::TimeSeries => "time_series",
            Self::Microstructure => "microstructure",
            Self::Domain => "domain",
        }
    }
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
    /// Seconds a limit trigger must hold before entry fires.
    pub confirmation_window_secs: u64,
}

impl Default for EntryOrderPolicy {
    fn default() -> Self {
        Self {
            max_slippage_bps: 50,
            allow_market_orders: false,
            confirmation_window_secs: 0,
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

/// Exit-monitor cadence and signal-degradation policy (Phase 05.6).
///
/// The monitor scans open position lots every `monitor_secs` for the price /
/// time / trailing / partial ladder; the heavier signal re-inference runs at
/// Model-backed thesis-invalidation re-inference policy (Phase 06.0).
///
/// When `enabled`, the exit monitor re-scores each lot's market via the
/// intent-frozen model and compares the fresh composite score against the entry
/// baseline. `shadow_mode` runs the full pipeline but suppresses
/// `ThesisInvalidated` exits (metrics + logs only) until operators disable it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExitSignalReinferencePolicy {
    /// Whether model-backed signal re-inference is active.
    pub enabled: bool,
    /// When true, re-inference runs and is audited, but thesis-invalidation
    /// exits are suppressed (fail-safe hold; SL/time/trailing still apply).
    pub shadow_mode: bool,
}

impl Default for ExitSignalReinferencePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            shadow_mode: true,
        }
    }
}

/// Exit-monitor cadence, signal-degradation threshold, and re-inference policy.
///
/// Price/time/trailing tiers run every `monitor_secs`; model re-inference runs
/// at most every `signal_recheck_secs` per lot. A fresh composite score below
/// `entry_composite_score × signal_invalidation_ratio` invalidates the thesis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExitMonitorPolicy {
    /// Whether the exit-monitor worker actively evaluates open lots.
    pub enabled: bool,
    /// Seconds between exit-monitor scans (price / time / trailing cadence).
    pub monitor_secs: u64,
    /// Minimum seconds between signal re-inference checks for one lot.
    pub signal_recheck_secs: u64,
    /// Fresh composite-score fraction of the entry score below which the thesis
    /// is treated as invalidated (signal-degradation re-inference).
    pub signal_invalidation_ratio: DecimalString,
    /// Model-backed thesis-invalidation re-inference (Phase 06.0).
    pub signal_reinference: ExitSignalReinferencePolicy,
}

impl Default for ExitMonitorPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            monitor_secs: 10,
            signal_recheck_secs: 60,
            signal_invalidation_ratio: DecimalString::new("0.6"),
            signal_reinference: ExitSignalReinferencePolicy::default(),
        }
    }
}

/// Emergency-exit action used when operational kill-switch handling escalates
/// to emergency state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmergencyExitKind {
    /// Submit reduce-only liquidation exits within the configured slippage cap.
    LiquidateAll,
    /// Route exits to manual operator handling only.
    ManualOnly,
}

/// Default emergency-exit policy consumed by the operational kill-switch plane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EmergencyExitPolicy {
    /// Emergency-exit behavior.
    pub kind: EmergencyExitKind,
    /// Maximum slippage for automated emergency liquidation, in basis points.
    pub max_slippage_bps: u32,
}

impl Default for EmergencyExitPolicy {
    fn default() -> Self {
        Self {
            kind: EmergencyExitKind::ManualOnly,
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

/// Execution kill-switch default policy.
///
/// Operational state lives in the `system_kill_switch` singleton. Runtime
/// config only carries the policy to apply when that state escalates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(default, deny_unknown_fields)]
pub struct KillSwitchPolicy {
    /// Emergency-exit behavior for kill-switch escalation.
    pub emergency_exit: EmergencyExitPolicy,
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
    /// Reconciliation sweep interval in seconds.
    pub interval_secs: u64,
    /// Seconds an order may remain unreconciled (resting open, or unreadable at
    /// the venue) before the worker forces a terminal resolution: a stale
    /// resting order is actively cancelled, an unreadable order is escalated to
    /// `Unresolvable`. Bounds how long capital can stay in-flight.
    pub stale_open_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AttributionPolicy {
    /// Whether the final recommendation-attribution worker is enabled.
    pub enabled: bool,
    /// Attribution sweep interval in seconds.
    pub sweep_secs: u64,
    /// Maximum terminal recommendation/intent candidates processed per sweep.
    pub batch_size: u64,
}

impl Default for AttributionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            sweep_secs: 60,
            batch_size: 256,
        }
    }
}

impl Default for ReconciliationPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            stale_open_secs: 300,
        }
    }
}

/// Venue-dimension execution-breaker thresholds (Phase 05.4 §6.5).
///
/// Drives the stateful [`ExecutionBreaker`] that watches venue submit/cancel
/// outcomes and publishes a `VenueHealth` seam for admission `#18` while
/// auto-tripping the operational kill-switch on sustained failure. Transient
/// degradation defers (admission retries); sustained failure halts and latches
/// the kill-switch (`execution_halted`, operator ack required to clear).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionBreakerConfig {
    /// Consecutive venue failures that move the breaker to `Degraded` (admission defers).
    pub venue_consecutive_failures_to_degrade: u32,
    /// Consecutive venue failures that move the breaker to `Halted` and trip the kill-switch.
    pub venue_consecutive_failures_to_halt: u32,
    /// Rolling-window venue error rate (basis points) that trips `Halted`.
    pub venue_error_rate_bps_to_halt: u32,
    /// Minimum window samples before the error-rate gate is evaluated (avoids small-N trips).
    pub venue_min_window_samples: u32,
    /// Rolling observation window length in seconds.
    pub venue_window_secs: u64,
    /// Seconds of failure-free operation before `Degraded` self-recovers to `Healthy`.
    pub cooldown_secs: u64,
    /// Daily realized-loss cap in USD (UTC day). Cumulative same-day realized
    /// loss `≥ 80%` of the cap degrades venue health (admission `#18` defers);
    /// `≥` the cap trips the kill-switch (`execution_halted`, latched). `0`
    /// disables the daily-realized-loss dimension.
    pub daily_realized_loss_cap_usd: DecimalString,
}

impl Default for ExecutionBreakerConfig {
    fn default() -> Self {
        Self {
            venue_consecutive_failures_to_degrade: 3,
            venue_consecutive_failures_to_halt: 6,
            venue_error_rate_bps_to_halt: 5_000,
            venue_min_window_samples: 10,
            venue_window_secs: 60,
            cooldown_secs: 30,
            daily_realized_loss_cap_usd: DecimalString::new("0"),
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
