//! Individual risk check implementations.
//!
//! Each check is a standalone struct implementing [`RiskCheck`]. Checks are
//! registered in the pipeline in a fixed canonical order. They read exclusively
//! from [`PreTradeContext`] and never perform I/O or lock subsystems.

use crate::{
    context::PreTradeContext,
    pipeline::RiskCheck,
    types::{DrawdownAction, RiskCheckId, RiskCheckKind, RiskCheckResult},
};
use oxide_arb_models::{
    enums::common::{ExecutionMode, StalenessLevel},
    runtime_config::RiskConfig,
    types::Usd,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// elapsed_us is set by the pipeline after evaluate() returns; checks use RiskCheckResult helpers.

// ═══════════════════════════════════════════════════════════════════════════════
// #1 ManualHalt
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ManualHaltCheck;

impl RiskCheck for ManualHaltCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::ManualHalt
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.manual_halt().allows_trading() {
            return RiskCheckResult::passed(RiskCheckId::ManualHalt);
        }
        RiskCheckResult::failed(
            RiskCheckId::ManualHalt,
            ctx.manual_halt()
                .denial_detail()
                .unwrap_or_else(|| "engine manually halted".into()),
            "not halted".into(),
            "halted".into(),
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #2 CircuitBreaker
// ═══════════════════════════════════════════════════════════════════════════════

pub struct CircuitBreakerCheck;

impl RiskCheck for CircuitBreakerCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::CircuitBreaker
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.circuit_breaker().allows_trading {
            return RiskCheckResult::passed(RiskCheckId::CircuitBreaker);
        }
        RiskCheckResult::failed(
            RiskCheckId::CircuitBreaker,
            "circuit breaker is open".into(),
            "closed or half-open".into(),
            "open".into(),
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #3 BlacklistTradingPath
// ═══════════════════════════════════════════════════════════════════════════════

pub struct BlacklistCheck;

impl RiskCheck for BlacklistCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::BlacklistTradingPath
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if let Some(detail) = ctx.is_market_blacklisted_trading_path() {
            return RiskCheckResult::failed(
                RiskCheckId::BlacklistTradingPath,
                detail,
                "not blacklisted".into(),
                "blacklisted".into(),
            );
        }
        RiskCheckResult::passed(RiskCheckId::BlacklistTradingPath)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #4 TokenBlacklist
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TokenBlacklistCheck;

impl RiskCheck for TokenBlacklistCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::TokenBlacklist
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if !ctx.is_token_blacklisted() {
            return RiskCheckResult::passed(RiskCheckId::TokenBlacklist);
        }
        RiskCheckResult::failed(
            RiskCheckId::TokenBlacklist,
            "token is blacklisted".into(),
            "not blacklisted".into(),
            "blacklisted".into(),
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #5 MarketAnomalyBlock (control factor)
// ═══════════════════════════════════════════════════════════════════════════════

/// Hard-rejects when a published `MarketAnomalyFactor` blocks this market/event.
///
/// Reads only the execution-time [`FactorDecisionContext`]; neutral (passes)
/// when no publication is active. Surfaces as a named denial, not an anonymous
/// string.
pub struct MarketAnomalyBlockCheck;

impl RiskCheck for MarketAnomalyBlockCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::MarketAnomalyBlock
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let Some(factor_context) = ctx.factor_context() else {
            return RiskCheckResult::passed(RiskCheckId::MarketAnomalyBlock);
        };
        let anomaly = &factor_context.market_anomaly;
        if anomaly.is_blocking() {
            let detail = anomaly
                .reason_code
                .clone()
                .unwrap_or_else(|| "market anomaly block".into());
            return RiskCheckResult::failed(
                RiskCheckId::MarketAnomalyBlock,
                detail,
                "no active anomaly block".into(),
                "blocked".into(),
            );
        }
        RiskCheckResult::passed(RiskCheckId::MarketAnomalyBlock)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #6 ReconciliationMaintenance (control factor)
// ═══════════════════════════════════════════════════════════════════════════════

/// Hard-rejects when a published `ReconciliationHealthFactor` forces maintenance
/// mode. Reads only the execution-time [`FactorDecisionContext`].
pub struct ReconciliationMaintenanceCheck;

impl RiskCheck for ReconciliationMaintenanceCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::ReconciliationMaintenance
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let Some(factor_context) = ctx.factor_context() else {
            return RiskCheckResult::passed(RiskCheckId::ReconciliationMaintenance);
        };
        if factor_context.reconciliation_health.force_maintenance_mode {
            return RiskCheckResult::failed(
                RiskCheckId::ReconciliationMaintenance,
                "reconciliation health forced maintenance mode".into(),
                "trading healthy".into(),
                "maintenance".into(),
            );
        }
        RiskCheckResult::passed(RiskCheckId::ReconciliationMaintenance)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #6b ControlFactorManualAckRequired (control factor)
// ═══════════════════════════════════════════════════════════════════════════════

/// Hard-rejects when a published control factor requires operator acknowledgement
/// before admitting new entries (reconciliation health or market anomaly).
pub struct ControlFactorManualAckRequiredCheck;

impl RiskCheck for ControlFactorManualAckRequiredCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::ControlFactorManualAckRequired
    }

    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }

    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let Some(factor_context) = ctx.factor_context() else {
            return RiskCheckResult::passed(RiskCheckId::ControlFactorManualAckRequired);
        };
        if factor_context.reconciliation_health.require_manual_ack {
            return RiskCheckResult::failed(
                RiskCheckId::ControlFactorManualAckRequired,
                "reconciliation health requires manual acknowledgement".into(),
                "acknowledged".into(),
                "pending".into(),
            );
        }
        if factor_context.market_anomaly.manual_ack_required {
            return RiskCheckResult::failed(
                RiskCheckId::ControlFactorManualAckRequired,
                factor_context
                    .market_anomaly
                    .reason_code
                    .clone()
                    .map_or_else(
                        || "market anomaly requires manual acknowledgement".to_owned(),
                        |code| format!("market anomaly {code} requires manual acknowledgement"),
                    ),
                "acknowledged".into(),
                "pending".into(),
            );
        }
        RiskCheckResult::passed(RiskCheckId::ControlFactorManualAckRequired)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #7 ControlFactorSnapshotExpired (control factor)
// ═══════════════════════════════════════════════════════════════════════════════

/// Hard-rejects when the active publication TTL has elapsed and Live policy
/// requires fail-closed behavior (retaining a stale snapshot after refresh
/// failure must not admit new entries).
pub struct ControlFactorSnapshotExpiredCheck;

impl RiskCheck for ControlFactorSnapshotExpiredCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::ControlFactorSnapshotExpired
    }

    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }

    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let Some(factor_context) = ctx.factor_context() else {
            return RiskCheckResult::passed(RiskCheckId::ControlFactorSnapshotExpired);
        };
        if factor_context.is_snapshot_stale_fail_closed() {
            return RiskCheckResult::failed(
                RiskCheckId::ControlFactorSnapshotExpired,
                "control-factor publication TTL expired".into(),
                "publication active".into(),
                "expired".into(),
            );
        }
        RiskCheckResult::passed(RiskCheckId::ControlFactorSnapshotExpired)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #8 RedeemRouteResolvable (Live settlement)
// ═══════════════════════════════════════════════════════════════════════════════

/// Fail-closed Live gate: registry must expose `neg_risk` and policy must resolve.
pub struct RedeemRouteResolvableCheck;

impl RiskCheck for RedeemRouteResolvableCheck {
    fn requires_metrics(&self) -> bool {
        false
    }

    fn id(&self) -> RiskCheckId {
        RiskCheckId::RedeemRouteResolvable
    }

    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }

    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.settlement_gate.mode != ExecutionMode::Live {
            return RiskCheckResult::passed(RiskCheckId::RedeemRouteResolvable);
        }
        let Some(neg_risk) = ctx.settlement_gate.market_neg_risk else {
            return RiskCheckResult::failed(
                RiskCheckId::RedeemRouteResolvable,
                format!("market {} not in registry", ctx.opportunity.market_id),
                "registry neg_risk".into(),
                "missing".into(),
            );
        };
        let Some(policy) = ctx.settlement_gate.redeem_policy else {
            return RiskCheckResult::failed(
                RiskCheckId::RedeemRouteResolvable,
                "settlement.redeem policy unavailable".into(),
                "policy present".into(),
                "missing".into(),
            );
        };
        if policy
            .resolve(&ctx.opportunity.market_id, neg_risk)
            .is_some()
        {
            RiskCheckResult::passed(RiskCheckId::RedeemRouteResolvable)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::RedeemRouteResolvable,
                format!(
                    "settlement.redeem: no route for market {} (neg_risk={neg_risk})",
                    ctx.opportunity.market_id
                ),
                "resolvable route".into(),
                "none".into(),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #9 MetricsFreshness
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MetricsFreshnessCheck {
    max_age_secs: u64,
}

impl MetricsFreshnessCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_age_secs: config.max_metrics_staleness_secs,
        }
    }
}

impl RiskCheck for MetricsFreshnessCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MetricsFreshness
    }

    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }

    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if !ctx.is_authoritative() {
            return RiskCheckResult::failed(
                RiskCheckId::MetricsFreshness,
                "risk metrics source is not authoritative".into(),
                "authoritative".into(),
                "non-authoritative".into(),
            );
        }

        if ctx.is_metrics_stale() {
            return RiskCheckResult::failed(
                RiskCheckId::MetricsFreshness,
                "risk metrics snapshot has been marked stale".into(),
                "not stale".into(),
                "stale".into(),
            );
        }

        let actual = ctx.metrics_age_secs();
        if actual <= self.max_age_secs {
            RiskCheckResult::passed(RiskCheckId::MetricsFreshness)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MetricsFreshness,
                format!("risk metrics snapshot is stale: {actual}s"),
                format!("≤ {}s", self.max_age_secs),
                format!("{actual}s"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #5 MinDepth
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MinDepthCheck {
    min_depth_usd: Decimal,
}

impl MinDepthCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            min_depth_usd: config.min_depth_usd,
        }
    }
}

impl RiskCheck for MinDepthCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MinDepth
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let depth_pct = ctx.opportunity.depth_used_pct;

        if depth_pct.is_zero() {
            return RiskCheckResult::failed(
                RiskCheckId::MinDepth,
                "depth_used_pct is zero — cannot infer available depth".into(),
                format!("≥ ${}", self.min_depth_usd),
                "unknown".into(),
            );
        }

        let available_depth = ctx.opportunity.total_cost.inner() * dec!(100) / depth_pct;

        if available_depth >= self.min_depth_usd {
            RiskCheckResult::passed(RiskCheckId::MinDepth)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MinDepth,
                format!(
                    "available depth ${available_depth:.2} below min ${}",
                    self.min_depth_usd
                ),
                format!("≥ ${}", self.min_depth_usd),
                format!("${available_depth:.2}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #5 MaxDepthUsage
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MaxDepthUsageCheck {
    max_depth_usage_pct: Decimal,
}

impl MaxDepthUsageCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_depth_usage_pct: config.max_depth_usage_pct,
        }
    }
}

impl RiskCheck for MaxDepthUsageCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MaxDepthUsage
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.opportunity.depth_used_pct;

        if actual <= self.max_depth_usage_pct {
            RiskCheckResult::passed(RiskCheckId::MaxDepthUsage)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MaxDepthUsage,
                format!(
                    "depth usage {actual}% exceeds max {}%",
                    self.max_depth_usage_pct
                ),
                format!("≤ {}%", self.max_depth_usage_pct),
                format!("{actual}%"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #6 Staleness
// ═══════════════════════════════════════════════════════════════════════════════

pub struct StalenessCheck;

impl RiskCheck for StalenessCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::Staleness
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.opportunity.staleness < StalenessLevel::Stale {
            RiskCheckResult::passed(RiskCheckId::Staleness)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::Staleness,
                format!(
                    "data staleness '{}' is unacceptable",
                    ctx.opportunity.staleness
                ),
                "< stale".into(),
                ctx.opportunity.staleness.as_str().into(),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #7 DailyBudget
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DailyBudgetCheck;

impl RiskCheck for DailyBudgetCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DailyBudget
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.daily_budget_remaining() >= ctx.intended_cost_usd() {
            RiskCheckResult::passed(RiskCheckId::DailyBudget)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::DailyBudget,
                format!(
                    "budget exhausted (remaining: {})",
                    ctx.daily_budget_remaining()
                ),
                "> $0".into(),
                format!("${}", ctx.daily_budget_remaining()),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #9 DailyLossCap
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DailyLossCapCheck {
    max_daily_loss_usd: Decimal,
}

impl DailyLossCapCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_daily_loss_usd: config.max_daily_loss_usd,
        }
    }
}

impl RiskCheck for DailyLossCapCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DailyLossCap
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.daily_loss().inner() < self.max_daily_loss_usd {
            RiskCheckResult::passed(RiskCheckId::DailyLossCap)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::DailyLossCap,
                format!(
                    "daily loss ${} ≥ cap ${}",
                    ctx.daily_loss(),
                    self.max_daily_loss_usd
                ),
                format!("< ${}", self.max_daily_loss_usd),
                format!("${}", ctx.daily_loss()),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #10 WeeklyLossCap
// ═══════════════════════════════════════════════════════════════════════════════

pub struct WeeklyLossCapCheck {
    max_weekly_loss_usd: Decimal,
}

impl WeeklyLossCapCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_weekly_loss_usd: config.max_weekly_loss_usd,
        }
    }
}

impl RiskCheck for WeeklyLossCapCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::WeeklyLossCap
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.weekly_loss().inner() < self.max_weekly_loss_usd {
            RiskCheckResult::passed(RiskCheckId::WeeklyLossCap)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::WeeklyLossCap,
                format!(
                    "weekly loss ${} ≥ cap ${}",
                    ctx.weekly_loss(),
                    self.max_weekly_loss_usd
                ),
                format!("< ${}", self.max_weekly_loss_usd),
                format!("${}", ctx.weekly_loss()),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #11 HourlyLossCap
// ═══════════════════════════════════════════════════════════════════════════════

pub struct HourlyLossCapCheck {
    max_hourly_loss_usd: Decimal,
}

impl HourlyLossCapCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_hourly_loss_usd: config.max_hourly_loss_usd,
        }
    }
}

impl RiskCheck for HourlyLossCapCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::HourlyLossCap
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.hourly_loss().inner() < self.max_hourly_loss_usd {
            RiskCheckResult::passed(RiskCheckId::HourlyLossCap)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::HourlyLossCap,
                format!(
                    "hourly loss ${} ≥ cap ${}",
                    ctx.hourly_loss(),
                    self.max_hourly_loss_usd
                ),
                format!("< ${}", self.max_hourly_loss_usd),
                format!("${}", ctx.hourly_loss()),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #12 FeeSpend
// ═══════════════════════════════════════════════════════════════════════════════

pub struct FeeSpendCheck {
    max_daily_fee_spend_usd: Decimal,
    max_hourly_fee_spend_usd: Decimal,
}

impl FeeSpendCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_daily_fee_spend_usd: config.max_daily_fee_spend_usd,
            max_hourly_fee_spend_usd: config.max_hourly_fee_spend_usd,
        }
    }
}

impl RiskCheck for FeeSpendCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::FeeSpend
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn requires_metrics(&self) -> bool {
        false
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.daily_fee().inner() >= self.max_daily_fee_spend_usd {
            return RiskCheckResult::failed(
                RiskCheckId::FeeSpend,
                format!(
                    "daily fees ${} ≥ cap ${}",
                    ctx.daily_fee(),
                    self.max_daily_fee_spend_usd
                ),
                format!("< ${}", self.max_daily_fee_spend_usd),
                format!("${}", ctx.daily_fee()),
            );
        }

        if ctx.hourly_fee().inner() >= self.max_hourly_fee_spend_usd {
            return RiskCheckResult::failed(
                RiskCheckId::FeeSpend,
                format!(
                    "hourly fees ${} ≥ cap ${}",
                    ctx.hourly_fee(),
                    self.max_hourly_fee_spend_usd
                ),
                format!("< ${}", self.max_hourly_fee_spend_usd),
                format!("${}", ctx.hourly_fee()),
            );
        }

        RiskCheckResult::passed(RiskCheckId::FeeSpend)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #13 MaxSingleBet
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MaxSingleBetCheck {
    max_single_bet_usd: Usd,
}

impl MaxSingleBetCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_single_bet_usd: Usd::new(config.max_single_bet_usd),
        }
    }
}

impl RiskCheck for MaxSingleBetCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MaxSingleBet
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.intended_cost_usd();

        if actual <= self.max_single_bet_usd {
            RiskCheckResult::passed(RiskCheckId::MaxSingleBet)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MaxSingleBet,
                format!("bet ${actual} exceeds max ${}", self.max_single_bet_usd),
                format!("≤ ${}", self.max_single_bet_usd),
                format!("${actual}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #12 MarketExposure
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MarketExposureCheck {
    max_single_market_exposure_usd: Usd,
}

impl MarketExposureCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_single_market_exposure_usd: Usd::new(config.max_single_market_exposure_usd),
        }
    }
}

impl RiskCheck for MarketExposureCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MarketExposure
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let post_trade = ctx.market_exposure_before() + ctx.intended_cost_usd();

        if post_trade <= self.max_single_market_exposure_usd {
            RiskCheckResult::passed(RiskCheckId::MarketExposure)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MarketExposure,
                format!(
                    "post-trade market exposure ${post_trade} exceeds max ${}",
                    self.max_single_market_exposure_usd
                ),
                format!("≤ ${}", self.max_single_market_exposure_usd),
                format!("${post_trade}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #13 TotalExposure
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TotalExposureCheck {
    max_total_exposure_usd: Usd,
}

impl TotalExposureCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_total_exposure_usd: Usd::new(config.max_total_exposure_usd),
        }
    }
}

impl RiskCheck for TotalExposureCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::TotalExposure
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let post_trade = ctx.total_exposure_before() + ctx.intended_cost_usd();

        if post_trade <= self.max_total_exposure_usd {
            RiskCheckResult::passed(RiskCheckId::TotalExposure)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::TotalExposure,
                format!(
                    "post-trade total exposure ${post_trade} exceeds max ${}",
                    self.max_total_exposure_usd
                ),
                format!("≤ ${}", self.max_total_exposure_usd),
                format!("${post_trade}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #14 ExposurePct
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ExposurePctCheck {
    max_total_exposure_pct: Decimal,
}

impl ExposurePctCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_total_exposure_pct: config.max_total_exposure_pct,
        }
    }
}

impl RiskCheck for ExposurePctCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::ExposurePct
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.cash_balance() <= Usd::ZERO {
            return RiskCheckResult::failed(
                RiskCheckId::ExposurePct,
                "cash balance is zero or negative — cannot compute exposure %".into(),
                format!("≤ {}%", self.max_total_exposure_pct),
                "N/A".into(),
            );
        }

        let post_trade = ctx.total_exposure_before().inner() + ctx.intended_cost_usd().inner();
        let pct = post_trade / ctx.cash_balance().inner() * dec!(100);

        if pct <= self.max_total_exposure_pct {
            RiskCheckResult::passed(RiskCheckId::ExposurePct)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::ExposurePct,
                format!(
                    "exposure {pct:.1}% exceeds max {}%",
                    self.max_total_exposure_pct
                ),
                format!("≤ {}%", self.max_total_exposure_pct),
                format!("{pct:.1}%"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #16 PotentialLossCap
// ═══════════════════════════════════════════════════════════════════════════════

pub struct PotentialLossCapCheck;

impl RiskCheck for PotentialLossCapCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::PotentialLossCap
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let post_trade_potential =
            ctx.total_potential_loss() + ctx.intended_cost_usd() + ctx.intended_fee_usd();

        if post_trade_potential <= ctx.cash_balance() {
            RiskCheckResult::passed(RiskCheckId::PotentialLossCap)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::PotentialLossCap,
                format!(
                    "potential loss ${post_trade_potential} exceeds balance ${}",
                    ctx.cash_balance()
                ),
                format!("≤ ${}", ctx.cash_balance()),
                format!("${post_trade_potential}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #17 MaxPositions
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MaxPositionsCheck {
    max_open_positions: usize,
}

impl MaxPositionsCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_open_positions: config.max_open_positions,
        }
    }
}

impl RiskCheck for MaxPositionsCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MaxPositions
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.open_position_count();

        if actual < self.max_open_positions {
            RiskCheckResult::passed(RiskCheckId::MaxPositions)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MaxPositions,
                format!("open positions {actual} ≥ max {}", self.max_open_positions),
                format!("< {}", self.max_open_positions),
                format!("{actual}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #16 WsConnectivity
// ═══════════════════════════════════════════════════════════════════════════════

pub struct WsConnectivityCheck {
    ws_disconnect_threshold_secs: u64,
}

impl WsConnectivityCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            ws_disconnect_threshold_secs: config.ws_disconnect_threshold_secs,
        }
    }
}

impl RiskCheck for WsConnectivityCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::WsConnectivity
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.ws_disconnect_secs();

        if actual < self.ws_disconnect_threshold_secs {
            RiskCheckResult::passed(RiskCheckId::WsConnectivity)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::WsConnectivity,
                format!("WS disconnected for {actual}s"),
                format!("< {}s", self.ws_disconnect_threshold_secs),
                format!("{actual}s"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #17 MinBalance
// ═══════════════════════════════════════════════════════════════════════════════

pub struct MinBalanceCheck {
    min_balance_usd: Usd,
}

impl MinBalanceCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            min_balance_usd: Usd::new(config.min_balance_usd),
        }
    }
}

impl RiskCheck for MinBalanceCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::MinBalance
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let balance = ctx.cash_balance();

        if balance >= self.min_balance_usd {
            RiskCheckResult::passed(RiskCheckId::MinBalance)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::MinBalance,
                format!("balance ${balance} below min ${}", self.min_balance_usd),
                format!("≥ ${}", self.min_balance_usd),
                format!("${balance}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #18 DirectionalConcentration
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DirectionalConcentrationCheck {
    max_concurrent_directional: usize,
}

impl DirectionalConcentrationCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            max_concurrent_directional: config.max_concurrent_directional,
        }
    }
}

impl RiskCheck for DirectionalConcentrationCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DirectionalConcentration
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.open_directional_count_same_side();

        if actual < self.max_concurrent_directional {
            RiskCheckResult::passed(RiskCheckId::DirectionalConcentration)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::DirectionalConcentration,
                format!(
                    "directional concentration {actual} ≥ max {}",
                    self.max_concurrent_directional
                ),
                format!("< {}", self.max_concurrent_directional),
                format!("{actual}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #19 DailyDirectionalBudget
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DailyDirectionalBudgetCheck {
    daily_directional_budget: u32,
}

impl DailyDirectionalBudgetCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            daily_directional_budget: config.daily_directional_budget,
        }
    }
}

impl RiskCheck for DailyDirectionalBudgetCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DailyDirectionalBudget
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        let actual = ctx.daily_directional_trades_same_side();

        if actual < self.daily_directional_budget {
            RiskCheckResult::passed(RiskCheckId::DailyDirectionalBudget)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::DailyDirectionalBudget,
                format!(
                    "daily directional trades {actual} ≥ budget {}",
                    self.daily_directional_budget
                ),
                format!("< {}", self.daily_directional_budget),
                format!("{actual}"),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #22 DuplicateMarket
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DuplicateMarketCheck;

impl RiskCheck for DuplicateMarketCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DuplicateMarket
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.market_exposure_before() <= Usd::ZERO {
            RiskCheckResult::passed(RiskCheckId::DuplicateMarket)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::DuplicateMarket,
                format!(
                    "market already has exposure ${}",
                    ctx.market_exposure_before()
                ),
                "$0".into(),
                format!("${}", ctx.market_exposure_before()),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #23 DrawdownGuard
// ═══════════════════════════════════════════════════════════════════════════════

pub struct DrawdownGuardCheck;

impl RiskCheck for DrawdownGuardCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::DrawdownGuard
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.drawdown_action() == DrawdownAction::Halt {
            RiskCheckResult::failed(
                RiskCheckId::DrawdownGuard,
                format!("drawdown halt (factor: {})", ctx.drawdown_factor()),
                "!= Halt".into(),
                format!("{:?}", ctx.drawdown_action()),
            )
        } else {
            RiskCheckResult::passed(RiskCheckId::DrawdownGuard)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #24 ApiErrorRate
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ApiErrorRateCheck {
    threshold: Decimal,
}

impl ApiErrorRateCheck {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            threshold: config.api_error_rate_threshold,
        }
    }
}

impl RiskCheck for ApiErrorRateCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::ApiErrorRate
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &PreTradeContext<'_>) -> RiskCheckResult {
        if ctx.api_request_count() == 0 {
            return RiskCheckResult::passed(RiskCheckId::ApiErrorRate);
        }
        let error_rate =
            Decimal::from(ctx.api_error_count()) / Decimal::from(ctx.api_request_count());
        if error_rate < self.threshold {
            RiskCheckResult::passed(RiskCheckId::ApiErrorRate)
        } else {
            RiskCheckResult::failed(
                RiskCheckId::ApiErrorRate,
                format!(
                    "API error rate {error_rate:.2} >= threshold {}",
                    self.threshold
                ),
                format!("< {}", self.threshold),
                format!("{error_rate:.2}"),
            )
        }
    }
}
