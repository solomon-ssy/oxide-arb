//! Position sizing — Quarter-Kelly with multi-constraint clipping.

use crate::context::PreTradeContext;
use crate::types::{DrawdownAction, KellyResult, SizeBreakdown, SizeConstraint, SizeResult};
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::types::Usd;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub struct QuarterKellyCalculator {
    kelly_fraction: Decimal,
    min_edge_bps: Decimal,
    max_kelly_fraction: Decimal,
    min_probability_confidence: Decimal,
    min_calibration_samples: u32,
    max_probability_staleness_secs: u64,
}

impl QuarterKellyCalculator {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            kelly_fraction: config.kelly_fraction,
            min_edge_bps: config.kelly.min_edge_bps,
            max_kelly_fraction: config.kelly.max_kelly,
            min_probability_confidence: config.kelly.min_probability_confidence,
            min_calibration_samples: config.kelly.min_calibration_samples,
            max_probability_staleness_secs: config.kelly.max_probability_staleness_secs,
        }
    }
    #[must_use]
    pub fn calculate(
        &self,
        prob: &ProbabilityInput,
        entry_price: Decimal,
        bankroll: Usd,
    ) -> KellyResult {
        let zero = || KellyResult {
            bet_usd: Usd::ZERO,
            kelly_raw: Decimal::ZERO,
            kelly_fractional: Decimal::ZERO,
            edge_bps: Decimal::ZERO,
            effective_win_prob: Decimal::ZERO,
            net_odds: Decimal::ZERO,
            binding_reason: "zero",
        };

        if entry_price <= Decimal::ZERO || entry_price >= Decimal::ONE || bankroll <= Usd::ZERO {
            return KellyResult {
                binding_reason: "invalid_input",
                ..zero()
            };
        }
        if prob.calibration_confidence < self.min_probability_confidence {
            return KellyResult {
                binding_reason: "low_confidence",
                ..zero()
            };
        }
        if prob.sample_size < self.min_calibration_samples {
            return KellyResult {
                binding_reason: "insufficient_samples",
                ..zero()
            };
        }
        if prob.model_staleness_secs > self.max_probability_staleness_secs {
            return KellyResult {
                binding_reason: "stale_model",
                ..zero()
            };
        }

        let confidence_haircut = prob.calibration_confidence;
        let staleness_ratio = Decimal::from(prob.model_staleness_secs)
            / Decimal::from(self.max_probability_staleness_secs);
        let staleness_haircut = (Decimal::ONE - staleness_ratio).max(Decimal::ZERO);

        let effective_p =
            prob.calibrated_win_prob * prob.fill_prob * confidence_haircut * staleness_haircut;
        let gross_odds = (Decimal::ONE - entry_price) / entry_price;
        let net_odds = gross_odds - prob.expected_slippage_pct - prob.expected_failure_cost_pct;

        if net_odds <= Decimal::ZERO {
            return KellyResult {
                effective_win_prob: effective_p,
                net_odds,
                binding_reason: "negative_odds_after_costs",
                ..zero()
            };
        }

        let edge_bps = if entry_price > Decimal::ZERO {
            (effective_p - entry_price) / entry_price * dec!(10000)
        } else {
            Decimal::ZERO
        };
        if edge_bps < self.min_edge_bps {
            return KellyResult {
                edge_bps,
                effective_win_prob: effective_p,
                net_odds,
                binding_reason: "below_min_edge",
                ..zero()
            };
        }

        let q = Decimal::ONE - effective_p;
        let kelly_raw = ((effective_p * net_odds - q) / net_odds).max(Decimal::ZERO);
        let kelly_fractional = (kelly_raw * self.kelly_fraction).min(self.max_kelly_fraction);
        let bet = (bankroll.inner() * kelly_fractional).round_dp(2);

        KellyResult {
            bet_usd: Usd::new(bet),
            kelly_raw,
            kelly_fractional,
            edge_bps,
            effective_win_prob: effective_p,
            net_odds,
            binding_reason: "kelly",
        }
    }
}

/// Multi-constraint position sizer. Stores only the fields it needs
/// from `RiskConfig` to avoid cloning the entire config.
pub struct MultiConstraintSizer {
    kelly: QuarterKellyCalculator,
    max_single_bet_usd: Decimal,
    max_single_loss_usd: Decimal,
    max_single_market_exposure_usd: Decimal,
    max_total_exposure_usd: Decimal,
    max_weekly_loss_usd: Decimal,
    reserve_balance_usd: Decimal,
    min_trade_usd: Decimal,
}

impl MultiConstraintSizer {
    #[must_use]
    pub const fn new(config: &RiskConfig) -> Self {
        Self {
            kelly: QuarterKellyCalculator::new(config),
            max_single_bet_usd: config.max_single_bet_usd,
            max_single_loss_usd: config.max_single_loss_usd,
            max_single_market_exposure_usd: config.max_single_market_exposure_usd,
            max_total_exposure_usd: config.max_total_exposure_usd,
            max_weekly_loss_usd: config.max_weekly_loss_usd,
            reserve_balance_usd: config.reserve_balance_usd,
            min_trade_usd: config.min_trade_usd,
        }
    }

    #[must_use]
    pub fn size(
        &self,
        ctx: &PreTradeContext<'_>,
        bankroll: Usd,
        drawdown_factor: Decimal,
    ) -> SizeResult {
        let kelly = self.kelly.calculate(
            &ctx.probability,
            ctx.opportunity.entry_price.inner(),
            bankroll,
        );

        let constraints = [
            SizeConstraint {
                name: "kelly_upper_bound",
                max_usd: kelly.bet_usd,
            },
            SizeConstraint {
                name: "max_single_bet",
                max_usd: Usd::new(self.max_single_bet_usd),
            },
            SizeConstraint {
                name: "max_single_loss",
                max_usd: Usd::new(self.max_single_loss_usd),
            },
            SizeConstraint {
                name: "market_exposure_headroom",
                max_usd: (Usd::new(self.max_single_market_exposure_usd)
                    - ctx.market_exposure_before())
                .max(Usd::ZERO),
            },
            SizeConstraint {
                name: "portfolio_exposure_headroom",
                max_usd: (Usd::new(self.max_total_exposure_usd) - ctx.total_exposure_before())
                    .max(Usd::ZERO),
            },
            SizeConstraint {
                name: "daily_budget_remaining",
                max_usd: ctx.daily_budget_remaining().max(Usd::ZERO),
            },
            SizeConstraint {
                name: "weekly_loss_headroom",
                max_usd: (Usd::new(self.max_weekly_loss_usd) - ctx.weekly_loss()).max(Usd::ZERO),
            },
            SizeConstraint {
                name: "available_balance",
                max_usd: (ctx.cached_balance()
                    - Usd::new(self.reserve_balance_usd)
                    - ctx.total_exposure_before())
                .max(Usd::ZERO),
            },
            SizeConstraint {
                name: "drawdown_scaled",
                max_usd: if drawdown_factor <= Decimal::ZERO {
                    Usd::ZERO
                } else {
                    bankroll * drawdown_factor
                },
            },
        ];

        let binding = constraints
            .iter()
            .min_by(|a, b| a.max_usd.inner().cmp(&b.max_usd.inner()))
            .expect("constraints is non-empty");
        let mut final_usd = binding.max_usd.max(Usd::ZERO);

        if final_usd < Usd::new(self.min_trade_usd) {
            final_usd = Usd::ZERO;
        }
        let final_usd = Usd::new(final_usd.inner().round_dp(2));

        SizeResult {
            bet_usd: final_usd,
            kelly_result: kelly,
            binding_constraint: binding.name,
            breakdown: SizeBreakdown {
                constraints: constraints.into_iter().collect(),
            },
        }
    }
}

pub struct DrawdownGuard {
    hwm: Usd,
    max_drawdown_pct: Decimal,
    reduction_factor: Decimal,
}

impl DrawdownGuard {
    #[must_use]
    pub const fn new(
        initial_equity: Usd,
        max_drawdown_pct: Decimal,
        reduction_factor: Decimal,
    ) -> Self {
        Self {
            hwm: initial_equity,
            max_drawdown_pct,
            reduction_factor,
        }
    }
    #[must_use]
    pub const fn from_snapshot(
        hwm: Usd,
        max_drawdown_pct: Decimal,
        reduction_factor: Decimal,
    ) -> Self {
        Self {
            hwm,
            max_drawdown_pct,
            reduction_factor,
        }
    }
    pub fn update_equity(&mut self, current_equity: Usd) {
        if current_equity > self.hwm {
            self.hwm = current_equity;
        }
    }
    #[must_use]
    pub fn evaluate(&self, current_equity: Usd) -> (Decimal, DrawdownAction) {
        if self.hwm <= Usd::ZERO {
            return (Decimal::ZERO, DrawdownAction::Normal);
        }
        let drawdown = self.hwm - current_equity;
        let drawdown_pct = if drawdown.is_positive() {
            drawdown.inner() * dec!(100) / self.hwm.inner()
        } else {
            Decimal::ZERO
        };
        if drawdown_pct >= self.max_drawdown_pct {
            (drawdown_pct, DrawdownAction::Halt)
        } else if drawdown_pct > Decimal::ZERO {
            (drawdown_pct, DrawdownAction::Reduce)
        } else {
            (drawdown_pct, DrawdownAction::Normal)
        }
    }
    #[must_use]
    pub fn sizing_factor(&self, current_equity: Usd) -> Decimal {
        let (drawdown_pct, action) = self.evaluate(current_equity);
        match action {
            DrawdownAction::Normal => Decimal::ONE,
            DrawdownAction::Reduce => {
                let ratio = drawdown_pct / self.max_drawdown_pct;
                Decimal::ONE - ratio * (Decimal::ONE - self.reduction_factor)
            }
            DrawdownAction::Halt => Decimal::ZERO,
        }
    }
    #[must_use]
    #[inline]
    pub const fn hwm(&self) -> Usd {
        self.hwm
    }
}
