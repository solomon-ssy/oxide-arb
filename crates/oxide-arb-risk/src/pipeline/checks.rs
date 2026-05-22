//! Individual risk check implementations.
//!
//! Each check is a standalone struct implementing [`RiskCheck`]. Checks are
//! registered in the pipeline in a fixed canonical order. They read exclusively
//! from [`RiskContext`] and never perform I/O or lock subsystems.

use crate::context::RiskContext;
use crate::pipeline::RiskCheck;
use crate::types::{DrawdownAction, RiskCheckId, RiskCheckKind, RiskCheckResult};
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::enums::common::StalenessLevel;
use oxide_arb_models::types::Usd;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ── Helpers ─────────────────────────────────────────────────────────────────
// elapsed_us is set by the pipeline after evaluate() returns; checks set 0.

const fn pass(check_id: RiskCheckId) -> RiskCheckResult {
    RiskCheckResult {
        check_id,
        passed: true,
        detail: None,
        threshold: None,
        actual: None,
        elapsed_us: 0,
    }
}

const fn fail(
    check_id: RiskCheckId,
    detail: String,
    threshold: String,
    actual: String,
) -> RiskCheckResult {
    RiskCheckResult {
        check_id,
        passed: false,
        detail: Some(detail),
        threshold: Some(threshold),
        actual: Some(actual),
        elapsed_us: 0,
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #1 ManualHalt
// ═══════════════════════════════════════════════════════════════════════════════

pub struct ManualHaltCheck;

impl RiskCheck for ManualHaltCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::ManualHalt
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.manual_halt.allows_trading() {
            return pass(RiskCheckId::ManualHalt);
        }
        fail(
            RiskCheckId::ManualHalt,
            ctx.manual_halt
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
    fn id(&self) -> RiskCheckId {
        RiskCheckId::CircuitBreaker
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.circuit_breaker.allows_trading {
            return pass(RiskCheckId::CircuitBreaker);
        }
        fail(
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
    fn id(&self) -> RiskCheckId {
        RiskCheckId::BlacklistTradingPath
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.blacklist.allows_trading() {
            return pass(RiskCheckId::BlacklistTradingPath);
        }
        fail(
            RiskCheckId::BlacklistTradingPath,
            ctx.blacklist
                .denial_detail()
                .unwrap_or_else(|| "market is blacklisted".into()),
            "not blacklisted".into(),
            "blacklisted".into(),
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #4 TokenBlacklist
// ═══════════════════════════════════════════════════════════════════════════════

pub struct TokenBlacklistCheck;

impl RiskCheck for TokenBlacklistCheck {
    fn id(&self) -> RiskCheckId {
        RiskCheckId::TokenBlacklist
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if !ctx.token_blacklisted {
            return pass(RiskCheckId::TokenBlacklist);
        }
        fail(
            RiskCheckId::TokenBlacklist,
            "token is blacklisted".into(),
            "not blacklisted".into(),
            "blacklisted".into(),
        )
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let depth_pct = ctx.opportunity.depth_used_pct;

        if depth_pct.is_zero() {
            return fail(
                RiskCheckId::MinDepth,
                "depth_used_pct is zero — cannot infer available depth".into(),
                format!("≥ ${}", self.min_depth_usd),
                "unknown".into(),
            );
        }

        let available_depth = ctx.opportunity.total_cost.inner() * dec!(100) / depth_pct;

        if available_depth >= self.min_depth_usd {
            pass(RiskCheckId::MinDepth)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.opportunity.depth_used_pct;

        if actual <= self.max_depth_usage_pct {
            pass(RiskCheckId::MaxDepthUsage)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.opportunity.staleness < StalenessLevel::Stale {
            pass(RiskCheckId::Staleness)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.daily_budget_remaining > Usd::ZERO {
            pass(RiskCheckId::DailyBudget)
        } else {
            fail(
                RiskCheckId::DailyBudget,
                format!(
                    "budget exhausted (remaining: {})",
                    ctx.daily_budget_remaining
                ),
                "> $0".into(),
                format!("${}", ctx.daily_budget_remaining),
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.daily_loss.inner() < self.max_daily_loss_usd {
            pass(RiskCheckId::DailyLossCap)
        } else {
            fail(
                RiskCheckId::DailyLossCap,
                format!(
                    "daily loss ${} ≥ cap ${}",
                    ctx.daily_loss, self.max_daily_loss_usd
                ),
                format!("< ${}", self.max_daily_loss_usd),
                format!("${}", ctx.daily_loss),
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.weekly_loss.inner() < self.max_weekly_loss_usd {
            pass(RiskCheckId::WeeklyLossCap)
        } else {
            fail(
                RiskCheckId::WeeklyLossCap,
                format!(
                    "weekly loss ${} ≥ cap ${}",
                    ctx.weekly_loss, self.max_weekly_loss_usd
                ),
                format!("< ${}", self.max_weekly_loss_usd),
                format!("${}", ctx.weekly_loss),
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.hourly_loss.inner() < self.max_hourly_loss_usd {
            pass(RiskCheckId::HourlyLossCap)
        } else {
            fail(
                RiskCheckId::HourlyLossCap,
                format!(
                    "hourly loss ${} ≥ cap ${}",
                    ctx.hourly_loss, self.max_hourly_loss_usd
                ),
                format!("< ${}", self.max_hourly_loss_usd),
                format!("${}", ctx.hourly_loss),
            )
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// #12 MaxSingleBet
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.opportunity.total_cost;

        if actual <= self.max_single_bet_usd {
            pass(RiskCheckId::MaxSingleBet)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let post_trade = ctx.market_exposure_before + ctx.opportunity.total_cost;

        if post_trade <= self.max_single_market_exposure_usd {
            pass(RiskCheckId::MarketExposure)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let post_trade = ctx.total_exposure_before + ctx.opportunity.total_cost;

        if post_trade <= self.max_total_exposure_usd {
            pass(RiskCheckId::TotalExposure)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.cached_balance <= Usd::ZERO {
            return fail(
                RiskCheckId::ExposurePct,
                "cached balance is zero or negative — cannot compute exposure %".into(),
                format!("≤ {}%", self.max_total_exposure_pct),
                "N/A".into(),
            );
        }

        let post_trade = ctx.total_exposure_before.inner() + ctx.opportunity.total_cost.inner();
        let pct = post_trade / ctx.cached_balance.inner() * dec!(100);

        if pct <= self.max_total_exposure_pct {
            pass(RiskCheckId::ExposurePct)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let post_trade_potential = ctx.total_potential_loss + ctx.opportunity.total_cost;

        if post_trade_potential <= ctx.cached_balance {
            pass(RiskCheckId::PotentialLossCap)
        } else {
            fail(
                RiskCheckId::PotentialLossCap,
                format!(
                    "potential loss ${post_trade_potential} exceeds balance ${}",
                    ctx.cached_balance
                ),
                format!("≤ ${}", ctx.cached_balance),
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.open_position_count;

        if actual < self.max_open_positions {
            pass(RiskCheckId::MaxPositions)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.ws_disconnect_secs;

        if actual < self.ws_disconnect_threshold_secs {
            pass(RiskCheckId::WsConnectivity)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let balance = ctx.cached_balance;

        if balance >= self.min_balance_usd {
            pass(RiskCheckId::MinBalance)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.open_directional_count_same_side;

        if actual < self.max_concurrent_directional {
            pass(RiskCheckId::DirectionalConcentration)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        let actual = ctx.daily_directional_trades_same_side;

        if actual < self.daily_directional_budget {
            pass(RiskCheckId::DailyDirectionalBudget)
        } else {
            fail(
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.market_exposure_before <= Usd::ZERO {
            pass(RiskCheckId::DuplicateMarket)
        } else {
            fail(
                RiskCheckId::DuplicateMarket,
                format!(
                    "market already has exposure ${}",
                    ctx.market_exposure_before
                ),
                "$0".into(),
                format!("${}", ctx.market_exposure_before),
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.drawdown_action == DrawdownAction::Halt {
            fail(
                RiskCheckId::DrawdownGuard,
                format!("drawdown halt (factor: {})", ctx.drawdown_factor),
                "!= Halt".into(),
                format!("{:?}", ctx.drawdown_action),
            )
        } else {
            pass(RiskCheckId::DrawdownGuard)
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
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult {
        if ctx.api_request_count == 0 {
            return pass(RiskCheckId::ApiErrorRate);
        }
        let error_rate = Decimal::from(ctx.api_error_count) / Decimal::from(ctx.api_request_count);
        if error_rate < self.threshold {
            pass(RiskCheckId::ApiErrorRate)
        } else {
            fail(
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
