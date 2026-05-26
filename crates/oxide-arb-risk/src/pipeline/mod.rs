//! Unified pre-trade check pipeline with static registration.
//!
//! All gate checks are registered at construction time in a deterministic
//! order. The pipeline supports two modes: `ShortCircuit` (stop on first
//! hard gate failure) and `FullReport` (evaluate all checks for diagnostics).

pub mod checks;

use crate::context::RiskContext;
use crate::pipeline::checks::{
    ApiErrorRateCheck, BlacklistCheck, CircuitBreakerCheck, DailyBudgetCheck,
    DailyDirectionalBudgetCheck, DailyLossCapCheck, DirectionalConcentrationCheck,
    DrawdownGuardCheck, DuplicateMarketCheck, ExposurePctCheck, HourlyLossCapCheck,
    ManualHaltCheck, MarketExposureCheck, MaxDepthUsageCheck, MaxPositionsCheck, MaxSingleBetCheck,
    MinBalanceCheck, MinDepthCheck, PotentialLossCapCheck, StalenessCheck, TokenBlacklistCheck,
    TotalExposureCheck, WeeklyLossCapCheck, WsConnectivityCheck,
};
use crate::types::{PipelineReport, ReportMode, RiskCheckId, RiskCheckKind, RiskCheckResult};
use num_traits::ToPrimitive;
use oxide_arb_models::config::RiskConfig;
use std::time::Instant;

/// Trait implemented by each individual risk check.
pub trait RiskCheck: Send + Sync {
    fn id(&self) -> RiskCheckId;
    fn kind(&self) -> RiskCheckKind;
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult;
}

/// Statically registered pipeline — no dynamic dispatch on the hot path.
pub struct StaticRiskPipeline {
    manual_halt: ManualHaltCheck,
    circuit_breaker: CircuitBreakerCheck,
    blacklist: BlacklistCheck,
    token_blacklist: TokenBlacklistCheck,
    min_depth: MinDepthCheck,
    max_depth_usage: MaxDepthUsageCheck,
    staleness: StalenessCheck,
    daily_budget: DailyBudgetCheck,
    daily_loss_cap: DailyLossCapCheck,
    weekly_loss_cap: WeeklyLossCapCheck,
    hourly_loss_cap: HourlyLossCapCheck,
    max_single_bet: MaxSingleBetCheck,
    market_exposure: MarketExposureCheck,
    total_exposure: TotalExposureCheck,
    exposure_pct: ExposurePctCheck,
    potential_loss_cap: PotentialLossCapCheck,
    max_positions: MaxPositionsCheck,
    ws_connectivity: WsConnectivityCheck,
    api_error_rate: ApiErrorRateCheck,
    min_balance: MinBalanceCheck,
    directional_concentration: DirectionalConcentrationCheck,
    daily_directional_budget: DailyDirectionalBudgetCheck,
    duplicate_market: DuplicateMarketCheck,
    drawdown_guard: DrawdownGuardCheck,
}

impl StaticRiskPipeline {
    #[must_use]
    pub const fn len(&self) -> usize {
        24
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Ordered list of check IDs (for golden tests).
    #[must_use]
    pub fn check_order(&self) -> Vec<RiskCheckId> {
        vec![
            RiskCheckId::ManualHalt,
            RiskCheckId::CircuitBreaker,
            RiskCheckId::BlacklistTradingPath,
            RiskCheckId::TokenBlacklist,
            RiskCheckId::MinDepth,
            RiskCheckId::MaxDepthUsage,
            RiskCheckId::Staleness,
            RiskCheckId::DailyBudget,
            RiskCheckId::DailyLossCap,
            RiskCheckId::WeeklyLossCap,
            RiskCheckId::HourlyLossCap,
            RiskCheckId::MaxSingleBet,
            RiskCheckId::MarketExposure,
            RiskCheckId::TotalExposure,
            RiskCheckId::ExposurePct,
            RiskCheckId::PotentialLossCap,
            RiskCheckId::MaxPositions,
            RiskCheckId::WsConnectivity,
            RiskCheckId::ApiErrorRate,
            RiskCheckId::MinBalance,
            RiskCheckId::DirectionalConcentration,
            RiskCheckId::DailyDirectionalBudget,
            RiskCheckId::DuplicateMarket,
            RiskCheckId::DrawdownGuard,
        ]
    }

    #[must_use]
    pub fn evaluate(&self, ctx: &RiskContext, mode: ReportMode) -> PipelineReport {
        self.evaluate_range(ctx, mode, 0, self.len())
    }

    /// Evaluate gates `[start, end)` — used for lazy phase-1 / phase-2 split.
    #[must_use]
    pub fn evaluate_range(
        &self,
        ctx: &RiskContext,
        mode: ReportMode,
        start: usize,
        end: usize,
    ) -> PipelineReport {
        let pipeline_start = Instant::now();
        let mut results = Vec::with_capacity(end.saturating_sub(start));
        let mut has_failed_hard_gate = false;
        let mut first_failure: Option<RiskCheckId> = None;

        macro_rules! run_gate {
            ($check:expr) => {
                run_check(
                    &$check,
                    RiskCheckKind::Gate,
                    ctx,
                    mode,
                    &mut results,
                    &mut has_failed_hard_gate,
                    &mut first_failure,
                )
            };
        }

        let mut idx = 0usize;
        'evaluate: {
            macro_rules! gate {
                ($check:expr) => {{
                    if idx >= end {
                        break 'evaluate;
                    }
                    if idx >= start && !run_gate!($check) {
                        break 'evaluate;
                    }
                    idx += 1;
                }};
            }

            gate!(self.manual_halt);
            gate!(self.circuit_breaker);
            gate!(self.blacklist);
            gate!(self.token_blacklist);
            gate!(self.min_depth);
            gate!(self.max_depth_usage);
            gate!(self.staleness);
            gate!(self.daily_budget);
            gate!(self.daily_loss_cap);
            gate!(self.weekly_loss_cap);
            gate!(self.hourly_loss_cap);
            gate!(self.max_single_bet);
            gate!(self.market_exposure);
            gate!(self.total_exposure);
            gate!(self.exposure_pct);
            gate!(self.potential_loss_cap);
            gate!(self.max_positions);
            gate!(self.ws_connectivity);
            gate!(self.api_error_rate);
            gate!(self.min_balance);
            gate!(self.directional_concentration);
            gate!(self.daily_directional_budget);
            gate!(self.duplicate_market);
            if idx >= end {
                break 'evaluate;
            }
            if idx >= start {
                let _ = run_gate!(self.drawdown_guard);
            }
        }

        PipelineReport {
            results,
            has_failed_hard_gate,
            first_failure,
            total_elapsed_us: ToPrimitive::to_u64(&pipeline_start.elapsed().as_micros())
                .unwrap_or(u64::MAX),
        }
    }
}

#[inline]
fn run_check<C: RiskCheck>(
    check: &C,
    kind: RiskCheckKind,
    ctx: &RiskContext,
    mode: ReportMode,
    results: &mut Vec<RiskCheckResult>,
    has_failed_hard_gate: &mut bool,
    first_failure: &mut Option<RiskCheckId>,
) -> bool {
    let check_start = Instant::now();
    let mut result = check.evaluate(ctx);
    result.elapsed_us = ToPrimitive::to_u64(&check_start.elapsed().as_micros()).unwrap_or(u64::MAX);

    if !result.passed && kind == RiskCheckKind::Gate {
        *has_failed_hard_gate = true;
        if first_failure.is_none() {
            *first_failure = Some(check.id());
        }
    }

    results.push(result);

    !(*has_failed_hard_gate && mode == ReportMode::ShortCircuit)
}

/// Build the canonical pipeline in the fixed evaluation order.
#[must_use]
pub const fn build_default_pipeline(config: &RiskConfig) -> StaticRiskPipeline {
    StaticRiskPipeline {
        manual_halt: ManualHaltCheck,
        circuit_breaker: CircuitBreakerCheck,
        blacklist: BlacklistCheck,
        token_blacklist: TokenBlacklistCheck,
        min_depth: MinDepthCheck::new(config),
        max_depth_usage: MaxDepthUsageCheck::new(config),
        staleness: StalenessCheck,
        daily_budget: DailyBudgetCheck,
        daily_loss_cap: DailyLossCapCheck::new(config),
        weekly_loss_cap: WeeklyLossCapCheck::new(config),
        hourly_loss_cap: HourlyLossCapCheck::new(config),
        max_single_bet: MaxSingleBetCheck::new(config),
        market_exposure: MarketExposureCheck::new(config),
        total_exposure: TotalExposureCheck::new(config),
        exposure_pct: ExposurePctCheck::new(config),
        potential_loss_cap: PotentialLossCapCheck,
        max_positions: MaxPositionsCheck::new(config),
        ws_connectivity: WsConnectivityCheck::new(config),
        api_error_rate: ApiErrorRateCheck::new(config),
        min_balance: MinBalanceCheck::new(config),
        directional_concentration: DirectionalConcentrationCheck::new(config),
        daily_directional_budget: DailyDirectionalBudgetCheck::new(config),
        duplicate_market: DuplicateMarketCheck,
        drawdown_guard: DrawdownGuardCheck,
    }
}
