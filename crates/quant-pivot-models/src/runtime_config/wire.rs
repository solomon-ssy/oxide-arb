//! Shared wire types and nested policy documents for runtime config.

use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::ModelVersionId;

/// Placeholder substituted for sensitive values on read surfaces.
pub const MASKED_SECRET: &str = "***";

/// Validated decimal value serialized as a JSON string without losing precision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct DecimalValue {
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String", extend("x-format" = "decimal"))]
    pub value: Decimal,
}

/// Runtime-config model-version id reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ModelVersionRef {
    #[schemars(with = "String", extend("x-format" = "uuid"))]
    pub id: ModelVersionId,
}

impl ModelVersionRef {
    #[must_use]
    pub const fn new(id: ModelVersionId) -> Self {
        Self { id }
    }

    #[must_use]
    pub const fn id(&self) -> &ModelVersionId {
        &self.id
    }
}

impl DecimalValue {
    /// Build a validated decimal wire value.
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self { value }
    }

    /// Return the exact decimal value.
    #[must_use]
    pub const fn value(&self) -> Decimal {
        self.value
    }
}

impl Default for DecimalValue {
    fn default() -> Self {
        Self::new(Decimal::ZERO)
    }
}

/// Named policy for stale feature handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeatureStalenessPolicy {
    RejectStaleRequired,
    AllowDegraded,
}

/// How a cross-sectional factor is normalized when the present same-`as_of`
/// cross-section is smaller than `factors.cross_section.min_size`.
///
/// There is **no silent-neutral** option: either the factor is normalized
/// against the model artifact's frozen training reference, or it is explicitly
/// indeterminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SmallCrossSectionPolicy {
    /// Emit an indeterminate factor (recorded reason, contributes nothing).
    Indeterminate,
    /// Normalize against the factor CDF frozen into the model artifact.
    FrozenReferenceQuantile,
}

/// Ranking loss optimized by the governed learning-to-rank trainer.
///
/// These are **simplex black-box surrogates**, not XGBoost/LightGBM `LambdaMART`
/// λ-gradient rankers. `TargetRankIcWeightedRanknet` weights `RankNet` pairs by the
/// closed-form `RankIC` swap delta; it is not a GBDT `LambdaRankIC` implementation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RankLossKind {
    /// RankIC-swap-weighted pairwise `RankNet` logistic loss (simplex surrogate).
    #[default]
    TargetRankIcWeightedRanknet,
    /// Plain pairwise `RankNet` logistic loss.
    PairwiseRanknet,
}

impl RankLossKind {
    /// The stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TargetRankIcWeightedRanknet => "target_rank_ic_weighted_ranknet",
            Self::PairwiseRanknet => "pairwise_ranknet",
        }
    }
}

/// Simplex optimizer used by the weighted-factor trainer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrainingOptimizerKind {
    /// Deterministic coordinate search only (production default).
    #[default]
    CoordinateSearch,
    /// Coordinate search base plus `argmin` refinement (requires `optimize` feature).
    Argmin,
}

/// A dimension a factor can be neutralized (residualized) against before the
/// cross-sectional normalization, to remove structural exposure (e.g. category).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NeutralizeDimension {
    /// Regress the factor on market-category one-hot dummies; keep the residual.
    Category,
}

/// Report delivery policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportDeliveryPolicy {
    StoreAndNotify,
    StoreOnly,
}

/// How often a report schedule fires.
///
/// Tagged on `kind`. The cron variant is parsed/scheduled by the runner;
/// validation only checks structural validity.
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
    /// Finalized execution-derived price, flow, intensity, and participant features.
    Trade,
    /// Order-flow microstructure (quote rate, churn, queue depletion, …).
    Microstructure,
    /// Prediction-market structural signals (neg-risk full-leg aggregates,
    /// shock/realized-vol windows, resolution-proximity, maker concentration).
    /// Platform-computable from existing facts — no external data source.
    Structural,
    /// Category-mapped external vertical slice (crypto underlying price, …).
    /// Built from `quant_domain_observation` + frozen market linkages; a market
    /// whose category maps to no vertical carries no domain slice at all.
    Domain,
}

impl FeatureFamily {
    /// Every feature family in declaration order.
    pub const ALL: [Self; 7] = [
        Self::MarketMetadata,
        Self::PriceBook,
        Self::TimeSeries,
        Self::Trade,
        Self::Microstructure,
        Self::Structural,
        Self::Domain,
    ];

    /// The stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketMetadata => "market_metadata",
            Self::PriceBook => "price_book",
            Self::TimeSeries => "time_series",
            Self::Trade => "trade",
            Self::Microstructure => "microstructure",
            Self::Structural => "structural",
            Self::Domain => "domain",
        }
    }
}

/// Entry order policy for recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryOrderPolicy {
    /// Maximum allowed entry-order slippage in basis points.
    pub max_slippage_bps: u32,
    /// Minimum visible book depth (USD) required at entry. Frozen onto every
    /// recommendation's `EntryPlan.min_depth_usd` at report build and enforced
    /// by execution admission (`LiquidityDepthCheck`): an intent is deferred
    /// when the fillable ask notional up to the limit price is below this
    /// floor. The value must be strictly positive; there is no disabled or
    /// unlimited sentinel.
    pub min_entry_book_depth_usd: DecimalValue,
}

impl Default for EntryOrderPolicy {
    fn default() -> Self {
        Self {
            max_slippage_bps: 50,
            min_entry_book_depth_usd: DecimalValue::new(rust_decimal_macros::dec!(100)),
        }
    }
}

/// Exit-monitor cadence and signal-degradation policy.
///
/// The monitor scans open position lots every `monitor_secs` for the price /
/// time / trailing / partial ladder; the heavier signal re-inference runs at
/// Model-backed thesis-invalidation re-inference policy.
///
/// When `enabled`, the exit monitor re-scores each lot's market via the
/// intent-frozen model and compares the fresh composite score against the entry
/// baseline. `shadow_mode` runs the full pipeline but suppresses
/// `ThesisInvalidated` exits (metrics + logs only) until operators disable it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

/// Opportunistic-Sell operational control.
///
/// Advisory model-driven scale-out when the thesis still holds but the Sell
/// scorer ranks exiting now as better than holding. Evaluated at the same
/// cadence as re-inference (`signal_recheck_secs`) and gated behind
/// re-inference being enabled (thesis validity must be checkable first).
///
/// All trading thresholds and cumulative/clip bounds live exclusively in the
/// immutable policy and intent. Runtime may only disable or shadow an already
/// frozen rule; it cannot change that rule's decision boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OpportunisticSellPolicy {
    /// Whether opportunistic-Sell evaluation is active at all.
    pub enabled: bool,
    /// When true, the scorer runs and is audited but never submits an exit
    /// (fail-safe hold; SL/time/trailing/invalidation still apply).
    pub shadow_mode: bool,
}

impl Default for OpportunisticSellPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            shadow_mode: true,
        }
    }
}

/// Exit-monitor cadence and re-inference policy.
///
/// Price/time/trailing tiers run every `monitor_secs`; model re-inference runs
/// at most every `signal_recheck_secs` per lot. Invalidation thresholds are
/// frozen in the intent's published trade-policy artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExitMonitorPolicy {
    /// Whether the exit-monitor worker actively evaluates open lots.
    pub enabled: bool,
    /// Seconds between exit-monitor scans (price / time / trailing cadence).
    pub monitor_secs: u64,
    /// Minimum seconds between signal re-inference checks for one lot.
    pub signal_recheck_secs: u64,
    /// Model-backed thesis-invalidation re-inference.
    pub signal_reinference: ExitSignalReinferencePolicy,
    /// Opportunistic-Sell advisory scale-out.
    pub opportunistic_sell: OpportunisticSellPolicy,
}

impl Default for ExitMonitorPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            monitor_secs: 10,
            signal_recheck_secs: 60,
            signal_reinference: ExitSignalReinferencePolicy::default(),
            opportunistic_sell: OpportunisticSellPolicy::default(),
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
#[serde(deny_unknown_fields)]
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

/// Execution kill-switch default policy.
///
/// Operational state lives in the `system_runtime_control` singleton. Runtime
/// config only carries the policy to apply when that state escalates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct KillSwitchPolicy {
    /// Emergency-exit behavior for kill-switch escalation.
    pub emergency_exit: EmergencyExitPolicy,
}

/// Execution reconciliation policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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

/// Runtime cadence and bounded work budget for outcome reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OutcomeReconciliationPolicy {
    /// Whether the outcome reconciliation worker is enabled.
    pub enabled: bool,
    /// Delay between reconciliation passes in seconds.
    pub sweep_secs: u64,
    /// Maximum recommendations or intents processed by each lane per pass.
    pub candidate_batch_size: u64,
    /// Maximum finalized source blocks scanned per resolution pass.
    pub source_block_span: u64,
}

impl OutcomeReconciliationPolicy {
    pub const MAX_CANDIDATE_BATCH_SIZE: u64 = 10_000;
    pub const MAX_SOURCE_BLOCK_SPAN: u64 = 10_000;
}

impl Default for OutcomeReconciliationPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            sweep_secs: 60,
            candidate_batch_size: 256,
            source_block_span: 256,
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

/// Frozen account-level policy for valuing delayed maker rebates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MakerRebatePolicy {
    /// Day-local minimum venue-calculated accrual required for payout.
    pub payout_threshold_usd: DecimalValue,
    /// Conservative lag from UTC program-day close while observed history is insufficient.
    pub fallback_lag_from_program_close_secs: u64,
    /// Complete venue-reported-accrual to wallet-credit program days required for p95.
    pub observed_p95_min_samples: u32,
}

impl Default for MakerRebatePolicy {
    fn default() -> Self {
        Self {
            payout_threshold_usd: DecimalValue::new(rust_decimal_macros::dec!(1)),
            fallback_lag_from_program_close_secs: 172_800,
            observed_p95_min_samples: 30,
        }
    }
}

/// Venue-dimension execution-breaker thresholds.
///
/// Drives the stateful execution breaker that watches venue submit/cancel
/// outcomes and publishes a `VenueHealth` seam for admission `#18` while
/// auto-tripping the operational kill-switch on sustained failure. Transient
/// degradation defers (admission retries); sustained failure halts and latches
/// the kill-switch (`execution_halted`, operator ack required to clear).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// `≥` the cap trips the kill-switch (`execution_halted`, latched). The
    /// cap must be strictly positive; there is no disabled sentinel.
    pub daily_realized_loss_cap_usd: DecimalValue,
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
            daily_realized_loss_cap_usd: DecimalValue::new(rust_decimal_macros::dec!(1_000)),
        }
    }
}

/// Notification routing policy flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotificationPolicies {
    /// Notify operators when a recommendation report is published.
    pub report_published: bool,
}

impl Default for NotificationPolicies {
    fn default() -> Self {
        Self {
            report_published: true,
        }
    }
}
