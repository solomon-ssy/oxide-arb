//! Unified pre-trade check pipeline with static registration.
//!
//! All gate checks are registered at construction time in a deterministic
//! order. The pipeline supports two modes: `ShortCircuit` (stop on first
//! hard gate failure) and `FullReport` (evaluate all checks for diagnostics).

pub mod checks;

use crate::context::RiskContext;
use crate::types::{PipelineReport, ReportMode, RiskCheckId, RiskCheckKind, RiskCheckResult};
use num_traits::ToPrimitive;
use oxide_arb_models::config::RiskConfig;
use std::time::Instant;

/// Trait implemented by each individual risk check.
///
/// Checks receive an immutable `RiskContext` and produce a `RiskCheckResult`.
/// They must not mutate any state or perform I/O.
pub trait RiskCheck: Send + Sync {
    fn id(&self) -> RiskCheckId;
    fn kind(&self) -> RiskCheckKind;
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult;
}

/// Ordered collection of risk checks executed during `pre_trade_check`.
///
/// Registration order is locked at construction time. Tests verify this
/// order via golden tests to prevent accidental reordering.
pub struct RiskPipeline {
    checks: Vec<Box<dyn RiskCheck>>,
}

impl RiskPipeline {
    #[must_use]
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    pub fn register(&mut self, check: Box<dyn RiskCheck>) {
        self.checks.push(check);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.checks.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    /// Ordered list of check IDs (for golden tests).
    #[must_use]
    pub fn check_order(&self) -> Vec<RiskCheckId> {
        self.checks.iter().map(|c| c.id()).collect()
    }

    /// Evaluate all registered checks against the given context.
    #[must_use]
    pub fn evaluate(&self, ctx: &RiskContext, mode: ReportMode) -> PipelineReport {
        let pipeline_start = Instant::now();
        let mut results = Vec::with_capacity(self.checks.len());
        let mut has_failed_hard_gate = false;
        let mut first_failure: Option<RiskCheckId> = None;

        for check in &self.checks {
            let check_start = Instant::now();
            let mut result = check.evaluate(ctx);
            result.elapsed_us =
                ToPrimitive::to_u64(&check_start.elapsed().as_micros()).unwrap_or(u64::MAX);

            if !result.passed && check.kind() == RiskCheckKind::Gate {
                has_failed_hard_gate = true;
                if first_failure.is_none() {
                    first_failure = Some(check.id());
                }
            }

            results.push(result);

            if has_failed_hard_gate && mode == ReportMode::ShortCircuit {
                break;
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

impl Default for RiskPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the canonical pipeline in the fixed evaluation order.
///
/// This is the single source of truth for check ordering. Tests verify
/// this order via golden tests.
#[must_use]
pub fn build_default_pipeline(config: &RiskConfig) -> RiskPipeline {
    use checks::{
        ApiErrorRateCheck, BlacklistCheck, CircuitBreakerCheck, DailyBudgetCheck,
        DailyDirectionalBudgetCheck, DailyLossCapCheck, DirectionalConcentrationCheck,
        DrawdownGuardCheck, DuplicateMarketCheck, ExposurePctCheck, HourlyLossCapCheck,
        ManualHaltCheck, MarketExposureCheck, MaxDepthUsageCheck, MaxPositionsCheck,
        MaxSingleBetCheck, MinBalanceCheck, MinDepthCheck, PotentialLossCapCheck, StalenessCheck,
        TokenBlacklistCheck, TotalExposureCheck, WeeklyLossCapCheck, WsConnectivityCheck,
    };

    let mut pipeline = RiskPipeline::new();

    // 1-4: Hard gates (manual, breaker, blacklist market, blacklist token)
    pipeline.register(Box::new(ManualHaltCheck));
    pipeline.register(Box::new(CircuitBreakerCheck));
    pipeline.register(Box::new(BlacklistCheck));
    pipeline.register(Box::new(TokenBlacklistCheck));

    // 5-7: Static limits (opportunity quality)
    pipeline.register(Box::new(MinDepthCheck::new(config)));
    pipeline.register(Box::new(MaxDepthUsageCheck::new(config)));
    pipeline.register(Box::new(StalenessCheck));

    // 8-11: Accounting caps
    pipeline.register(Box::new(DailyBudgetCheck));
    pipeline.register(Box::new(DailyLossCapCheck::new(config)));
    pipeline.register(Box::new(WeeklyLossCapCheck::new(config)));
    pipeline.register(Box::new(HourlyLossCapCheck::new(config)));

    // 12-17: Exposure limits
    pipeline.register(Box::new(MaxSingleBetCheck::new(config)));
    pipeline.register(Box::new(MarketExposureCheck::new(config)));
    pipeline.register(Box::new(TotalExposureCheck::new(config)));
    pipeline.register(Box::new(ExposurePctCheck::new(config)));
    pipeline.register(Box::new(PotentialLossCapCheck));
    pipeline.register(Box::new(MaxPositionsCheck::new(config)));

    // 18-20: System health
    pipeline.register(Box::new(WsConnectivityCheck::new(config)));
    pipeline.register(Box::new(ApiErrorRateCheck::new(config)));
    pipeline.register(Box::new(MinBalanceCheck::new(config)));

    // 21-23: Endgame-specific
    pipeline.register(Box::new(DirectionalConcentrationCheck::new(config)));
    pipeline.register(Box::new(DailyDirectionalBudgetCheck::new(config)));
    pipeline.register(Box::new(DuplicateMarketCheck));

    // 24: Drawdown guard
    pipeline.register(Box::new(DrawdownGuardCheck));

    pipeline
}
