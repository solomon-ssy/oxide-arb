//! Immutable pre-trade snapshot of internal risk engine state.
//!
//! Published via `ArcSwap` so `build_context` reads all subsystems in a
//! single atomic load instead of six separate `RwLock` acquisitions.

use crate::context::{CircuitBreakerGate, ManualHaltGate};
use crate::sizing::DrawdownGuard;
use crate::types::DrawdownAction;
use oxide_arb_models::types::Usd;
use rust_decimal::Decimal;

/// Circuit breaker + manual halt gates frozen at snapshot time.
#[derive(Debug, Clone)]
pub struct CircuitBreakerSnapshot {
    pub circuit_breaker: CircuitBreakerGate,
    pub manual_halt: ManualHaltGate,
}

/// Daily accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct DailyAccountingSnapshot {
    pub daily_loss: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_remaining: Usd,
}

/// Weekly accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct WeeklyAccountingSnapshot {
    pub weekly_loss: Usd,
}

/// Hourly accounting fields used by pre-trade checks.
#[derive(Debug, Clone, Copy)]
pub struct HourlyAccountingSnapshot {
    pub hourly_loss: Usd,
}

/// Drawdown guard parameters frozen at snapshot time.
///
/// Sizing factor is computed at check time using live equity from metrics.
#[derive(Debug, Clone, Copy)]
pub struct DrawdownSnapshot {
    pub hwm: Usd,
    pub max_drawdown_pct: Decimal,
    pub reduction_factor: Decimal,
}

impl DrawdownSnapshot {
    #[must_use]
    pub fn evaluate(&self, current_equity: Usd) -> (Decimal, DrawdownAction) {
        let guard =
            DrawdownGuard::from_snapshot(self.hwm, self.max_drawdown_pct, self.reduction_factor);
        guard.evaluate(current_equity)
    }

    #[must_use]
    pub fn sizing_factor(&self, current_equity: Usd) -> Decimal {
        let guard =
            DrawdownGuard::from_snapshot(self.hwm, self.max_drawdown_pct, self.reduction_factor);
        guard.sizing_factor(current_equity)
    }
}

/// Immutable copy of all internal state needed for a single pre-trade decision.
#[derive(Debug, Clone)]
pub struct RiskSnapshot {
    pub circuit_breaker: CircuitBreakerSnapshot,
    pub daily: DailyAccountingSnapshot,
    pub weekly: WeeklyAccountingSnapshot,
    pub hourly: HourlyAccountingSnapshot,
    pub drawdown: DrawdownSnapshot,
    pub total_potential_loss: Usd,
}

impl RiskSnapshot {
    #[must_use]
    pub const fn zeroed() -> Self {
        Self {
            circuit_breaker: CircuitBreakerSnapshot {
                circuit_breaker: CircuitBreakerGate {
                    allows_trading: true,
                    is_probe: false,
                },
                manual_halt: ManualHaltGate::Clear,
            },
            daily: DailyAccountingSnapshot {
                daily_loss: Usd::ZERO,
                daily_pnl: Usd::ZERO,
                daily_budget_remaining: Usd::ZERO,
            },
            weekly: WeeklyAccountingSnapshot {
                weekly_loss: Usd::ZERO,
            },
            hourly: HourlyAccountingSnapshot {
                hourly_loss: Usd::ZERO,
            },
            drawdown: DrawdownSnapshot {
                hwm: Usd::ZERO,
                max_drawdown_pct: Decimal::ZERO,
                reduction_factor: Decimal::ONE,
            },
            total_potential_loss: Usd::ZERO,
        }
    }
}
