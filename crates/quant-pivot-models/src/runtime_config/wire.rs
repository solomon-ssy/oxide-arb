//! Shared wire types and nested policy documents for runtime config.

use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{enums::quant::PortfolioSolverKind, types::ModelVersionId};

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

/// Missing factor handling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MissingFactorPolicy {
    ZeroWeight,
    RejectCandidate,
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
/// λ-gradient rankers. `RankIcWeightedRanknet` weights `RankNet` pairs by the
/// closed-form `RankIC` swap delta; it is not a GBDT `LambdaRankIC` implementation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RankLossKind {
    /// RankIC-swap-weighted pairwise `RankNet` logistic loss (simplex surrogate).
    #[default]
    RankIcWeightedRanknet,
    /// Plain pairwise `RankNet` logistic loss.
    PairwiseRanknet,
}

impl RankLossKind {
    /// The stable `snake_case` wire name (matches the serde representation).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RankIcWeightedRanknet => "rank_ic_weighted_ranknet",
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
/// never stands in for `q` (Phase 11.3 §4 redesign): for a `Calibrated` return
/// model, `q` is the calibrator's `P(win)` directly (`f* = (q - p) / (1 - p)`,
/// `p` = market price) — never re-derived from a second, TP/SL-shaped bet
/// structure. `confidence` enters only as an estimation-uncertainty shrinkage
/// on the Kelly fraction (`confidence_weighting`) — the production-standard
/// mitigation for Kelly's sensitivity to edge mis-estimation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SizingModelConfig {
    /// Fraction of full Kelly to apply, in `(0, 1]` (half-Kelly ≈ `0.5`).
    pub kelly_fraction: DecimalValue,
    /// Maximum single-position size as a fraction of equity (`(0, 1]`).
    pub max_position_pct: DecimalValue,
    /// Confidence-driven shrinkage of the Kelly fraction (estimation
    /// uncertainty): `confidence` high → near fractional Kelly, low → compressed.
    pub confidence_weighting: ConfidenceSizeCurve,
    /// Drawdown-driven scaling policy.
    pub drawdown_scaling: DrawdownMultiplierPolicy,
}

impl Default for SizingModelConfig {
    fn default() -> Self {
        Self {
            kelly_fraction: DecimalValue::new(rust_decimal_macros::dec!(0.5)),
            max_position_pct: DecimalValue::new(rust_decimal_macros::dec!(0.1)),
            confidence_weighting: ConfidenceSizeCurve::Linear,
            drawdown_scaling: DrawdownMultiplierPolicy::Conservative,
        }
    }
}

/// Portfolio optimizer (`good_lp` LP/MILP) configuration.
///
/// The optimizer is the **single** allocation path (greedy has been removed):
/// the production primary is an exact binary-inclusion MILP, and on any solver
/// failure it falls back to the continuous LP relaxation with deterministic
/// integer recovery, then ultimately to an empty plan — so a report is always
/// produced. `microlp` (pure Rust) is the default backend and ships in every
/// build; `HiGHS` is an optional native performance backend gated behind the
/// `lp-solver-highs` feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PortfolioOptimizerConfig {
    /// LP solver backend. `highs` requires the `lp-solver-highs` build feature;
    /// when that feature is absent the planner transparently downgrades to
    /// `microlp` (recorded in the plan's optimizer metadata).
    pub solver: PortfolioSolverKind,
    /// `true` ⇒ solve the exact binary-inclusion MILP (production primary);
    /// `false` ⇒ solve the continuous LP relaxation with deterministic integer
    /// recovery (cheaper, fully deterministic — also the fallback / backtest mode).
    pub integer_inclusion: bool,
    /// `λ ≥ 0`: weight on normalized expected return in the per-dollar objective
    /// weight `wᵢ = scoreᵢ · (1 + λ · ret_normᵢ)`. `0` ⇒ pure conviction weighting
    /// (semantically equivalent to the former greedy fill order).
    pub objective_return_weight: DecimalValue,
}

impl Default for PortfolioOptimizerConfig {
    fn default() -> Self {
        Self {
            solver: PortfolioSolverKind::Microlp,
            integer_inclusion: true,
            objective_return_weight: DecimalValue::new(rust_decimal_macros::dec!(0)),
        }
    }
}

/// Correlation-cluster estimation configuration for the correlated-exposure cap.
///
/// Drives whether `portfolio.constraints.max_correlated_exposure_usd` actually
/// binds. When disabled, the cap is snapshot-only (Phase 4 behavior).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CorrelationConfig {
    /// Whether the correlated-exposure cap is enforced. `false` ⇒ no clustering.
    pub enabled: bool,
    /// Historical mid-price lookback window for co-movement estimation, in days.
    pub lookback_days: u32,
    /// Minimum paired observations before historical estimation is trusted;
    /// below this the estimator falls back to event/category proxy clusters.
    pub min_observations: u32,
    /// Absolute Pearson correlation at or above which two markets are clustered.
    pub cluster_threshold: DecimalValue,
}

impl Default for CorrelationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lookback_days: 30,
            min_observations: 20,
            cluster_threshold: DecimalValue::new(rust_decimal_macros::dec!(0.7)),
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
    pub const ALL: [Self; 6] = [
        Self::MarketMetadata,
        Self::PriceBook,
        Self::TimeSeries,
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
            Self::Microstructure => "microstructure",
            Self::Structural => "structural",
            Self::Domain => "domain",
        }
    }
}

/// Factor weights keyed by factor name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct FactorWeights {
    pub weights: BTreeMap<String, DecimalValue>,
}

/// Entry order policy for recommendations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EntryOrderPolicy {
    /// Maximum allowed entry-order slippage in basis points.
    pub max_slippage_bps: u32,
    /// Minimum visible book depth (USD) required at entry. Frozen onto every
    /// recommendation's `EntryPlan.min_depth_usd` at report build and enforced
    /// by execution admission (`LiquidityDepthCheck`): an intent is deferred
    /// when the fillable ask notional up to the limit price is below this
    /// floor. `0` disables the depth floor.
    pub min_entry_book_depth_usd: DecimalValue,
}

impl Default for EntryOrderPolicy {
    fn default() -> Self {
        Self {
            max_slippage_bps: 50,
            min_entry_book_depth_usd: DecimalValue::new(rust_decimal_macros::dec!(0)),
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

/// Opportunistic-Sell operational control (Phase 06.1 / 11.7.2).
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
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
pub struct ExitMonitorPolicy {
    /// Whether the exit-monitor worker actively evaluates open lots.
    pub enabled: bool,
    /// Seconds between exit-monitor scans (price / time / trailing cadence).
    pub monitor_secs: u64,
    /// Minimum seconds between signal re-inference checks for one lot.
    pub signal_recheck_secs: u64,
    /// Model-backed thesis-invalidation re-inference (Phase 06.0).
    pub signal_reinference: ExitSignalReinferencePolicy,
    /// Opportunistic-Sell advisory scale-out (Phase 06.1).
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
///
/// Both caps gate order-intent admission (checks `#21` / `#22`). A value of
/// `0` **disables** that dimension (no cap) — matching the other opt-in USD
/// governance knobs. When `> 0` the cap is enforced hard (`Deny`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CapitalPolicy {
    /// Maximum USD that may be reserved across all open execution intents.
    /// `0` disables the reserved-capital cap. Enforced by admission `#22`.
    pub max_reserved_usd: DecimalValue,
    /// Maximum number of concurrently open execution intents. `0` disables the
    /// open-intent count cap. Enforced by admission `#21`.
    pub max_open_intents: u32,
}

impl Default for CapitalPolicy {
    fn default() -> Self {
        Self {
            max_reserved_usd: DecimalValue::new(rust_decimal_macros::dec!(0)),
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

/// On-chain settlement redemption worker policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct SettlementRedeemPolicy {
    /// Whether the worker may submit standard CTF redeem transactions.
    pub enabled: bool,
    /// Sweep interval in seconds.
    pub interval_secs: u64,
    /// Maximum condition-level redeem batches processed per sweep.
    pub batch_size: u64,
    /// Maximum failed submit/confirm attempts before manual escalation.
    pub max_attempts: u32,
    /// Base retry backoff in seconds.
    pub retry_backoff_secs: u64,
    /// Polygon confirmations required before closing the strategy lots.
    pub confirmation_blocks: u64,
    /// Whether automatic redeem may sign new transactions in emergency halt.
    pub allow_during_emergency: bool,
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

impl Default for SettlementRedeemPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 300,
            batch_size: 32,
            max_attempts: 5,
            retry_backoff_secs: 300,
            confirmation_blocks: 3,
            allow_during_emergency: false,
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
            daily_realized_loss_cap_usd: DecimalValue::new(rust_decimal_macros::dec!(0)),
        }
    }
}

/// Notification routing policy flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
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
