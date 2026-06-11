//! Runtime-config semantic validation and activation preflight.
//!
//! Three layers, all fail-closed:
//!
//! 1. [`validate_runtime_config`] — mode-agnostic invariants (ranges, cross
//!    field consistency). Runs on every `create_version` and `activate`.
//! 2. [`validate_runtime_for_mode`] — invariants that depend on the execution
//!    mode that will actually run (Live notification credentials, Live redeem
//!    route completeness). Runs on `activate` and on mode transitions.
//! 3. [`preflight_runtime_config`] — money-state preflight against the *live*
//!    system (in-flight reservations); rejects activations that would tighten
//!    exposure limits below currently committed capital.

use super::{RiskConfig, RuntimeConfig};
use crate::enums::common::{ExecutionMode, RedeemRoute};
use oxide_arb_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashSet;

/// Live money-state inputs for the activation preflight.
///
/// Collected by the core layer from the exposure-reservation system at the
/// moment of activation; pure data so the check itself stays side-effect free.
#[derive(Debug, Clone, Copy)]
pub struct RuntimePreflightContext {
    /// Execution mode that will run under the candidate config.
    pub mode: ExecutionMode,
    /// Sum of all active capital reservations (USD).
    pub reserved_total_usd: Decimal,
    /// Largest single-market in-flight exposure (USD).
    pub max_market_reserved_usd: Decimal,
}

/// Mode-agnostic runtime-config invariants.
#[must_use]
pub fn validate_runtime_config(config: &RuntimeConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_market_data(config, &mut report);
    validate_detection(config, &mut report);
    validate_execution(config, &mut report);
    validate_risk(config, &mut report);
    validate_settlement(config, &mut report);
    validate_notification(config, &mut report);
    report
}

/// Mode-aware runtime-config invariants (Live is strict, simulated modes warn).
#[must_use]
pub fn validate_runtime_for_mode(
    config: &RuntimeConfig,
    mode: ExecutionMode,
) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_settlement_mode(config, mode, &mut report);
    validate_notification_mode(config, mode, &mut report);
    report
}

/// Money-state activation preflight (fail-closed).
///
/// Rejects a candidate config whose exposure ceilings fall below capital that
/// is already committed: shrinking `max_total_exposure_usd` under the live
/// reserved total (or `max_single_market_exposure_usd` under any in-flight
/// market) would make the running system instantly out-of-policy with no safe
/// way to unwind.
#[must_use]
pub fn preflight_runtime_config(
    config: &RuntimeConfig,
    ctx: &RuntimePreflightContext,
) -> ConfigValidationReport {
    let mut report = validate_runtime_for_mode(config, ctx.mode);

    if config.risk.max_total_exposure_usd < ctx.reserved_total_usd {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_total_exposure_usd",
            detail: format!(
                "candidate limit {} is below the currently reserved total {}",
                config.risk.max_total_exposure_usd, ctx.reserved_total_usd
            ),
        });
    }
    if config.risk.max_single_market_exposure_usd < ctx.max_market_reserved_usd {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_single_market_exposure_usd",
            detail: format!(
                "candidate limit {} is below an in-flight market exposure of {}",
                config.risk.max_single_market_exposure_usd, ctx.max_market_reserved_usd
            ),
        });
    }

    report
}

// ── Section validators ───────────────────────────────────────────────────────

fn validate_market_data(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let md = &config.market_data;
    let ladder = [
        ("market_data.staleness_fresh_ms", md.staleness_fresh_ms),
        (
            "market_data.staleness_acceptable_ms",
            md.staleness_acceptable_ms,
        ),
        ("market_data.staleness_stale_ms", md.staleness_stale_ms),
        ("market_data.staleness_expired_ms", md.staleness_expired_ms),
    ];
    for (field, value) in ladder {
        if value == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }
    for window in ladder.windows(2) {
        let (low_field, low) = window[0];
        let (_, high) = window[1];
        if low >= high {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: low_field,
                detail: "staleness ladder must be strictly increasing \
                         (fresh < acceptable < stale < expired)"
                    .into(),
            });
        }
    }
    let mut seen = HashSet::new();
    for category in &md.enabled_categories {
        if !seen.insert(*category) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "market_data.enabled_categories",
                detail: format!("duplicate category: {category}"),
            });
        }
    }
}

fn validate_detection(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let detection = &config.detection;
    if detection.min_profit_threshold_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.min_profit_threshold_usd",
            detail: "must be > 0".into(),
        });
    }
    validate_detection_endgame(&detection.endgame, report);
    validate_detection_calibration(&detection.calibration, report);
}

fn validate_detection_endgame(
    endgame: &super::EndgameDetectionConfig,
    report: &mut ConfigValidationReport,
) {
    if endgame.high_threshold <= dec!(0.5) || endgame.high_threshold >= Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.high_threshold",
            detail: "must be in (0.5, 1)".into(),
        });
    }
    if endgame.min_profit_per_share <= Decimal::ZERO || endgame.min_profit_per_share >= Decimal::ONE
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.min_profit_per_share",
            detail: "must be in (0, 1)".into(),
        });
    }
    if endgame.max_investment_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.max_investment_usd",
            detail: "must be > 0".into(),
        });
    }
    if endgame.settlement_window_hours == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.settlement_window_hours",
            detail: "must be > 0".into(),
        });
    }
    if endgame.min_convergence_duration_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.min_convergence_duration_secs",
            detail: "must be > 0 (zero disables the transient-spike guard)".into(),
        });
    }

    let scorer = &endgame.scorer;
    if scorer.min_score < Decimal::ZERO || scorer.min_score > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.scorer.min_score",
            detail: "must be in [0, 1]".into(),
        });
    }
    if scorer.max_depth_usage_pct <= Decimal::ZERO || scorer.max_depth_usage_pct > dec!(100) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.scorer.max_depth_usage_pct",
            detail: "must be in (0, 100]".into(),
        });
    }
    for weight in scorer.category_weights.values() {
        if *weight < Decimal::ZERO || *weight > dec!(10) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "detection.endgame.scorer.category_weights",
                detail: format!("weight {weight} out of range [0, 10]"),
            });
        }
    }

    validate_fill_probability(&endgame.fill_probability, report);

    let cooldown = &endgame.emission_cooldown;
    if cooldown.base_cooldown_secs == 0 || cooldown.max_capacity == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.emission_cooldown",
            detail: "base_cooldown_secs and max_capacity must be > 0".into(),
        });
    }
    if cooldown.max_multiplier < Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.emission_cooldown.max_multiplier",
            detail: "must be >= 1".into(),
        });
    }

    let tracker = &endgame.convergence_tracker;
    if tracker.max_idle_secs == 0 || tracker.max_capacity == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.convergence_tracker",
            detail: "max_idle_secs and max_capacity must be > 0".into(),
        });
    }
}

/// Fill-probability model parameters: every penalty/bonus adjusts a
/// probability, so each must stay within `[0, 1]` and the depth threshold
/// must be a usable percentage.
fn validate_fill_probability(
    fill: &super::FillProbabilityConfig,
    report: &mut ConfigValidationReport,
) {
    if fill.base_fill_prob <= Decimal::ZERO || fill.base_fill_prob > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.fill_probability.base_fill_prob",
            detail: "must be in (0, 1]".into(),
        });
    }
    if fill.depth_penalty_threshold_pct <= Decimal::ZERO
        || fill.depth_penalty_threshold_pct > dec!(100)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.endgame.fill_probability.depth_penalty_threshold_pct",
            detail: "must be in (0, 100]".into(),
        });
    }
    for (field, value) in [
        (
            "detection.endgame.fill_probability.depth_penalty_per_pct",
            fill.depth_penalty_per_pct,
        ),
        (
            "detection.endgame.fill_probability.staleness_penalty_per_level",
            fill.staleness_penalty_per_level,
        ),
        (
            "detection.endgame.fill_probability.resolution_proximity_bonus",
            fill.resolution_proximity_bonus,
        ),
    ] {
        if value < Decimal::ZERO || value > Decimal::ONE {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be in [0, 1] (it adjusts a probability)".into(),
            });
        }
    }
}

fn validate_detection_calibration(
    calibration: &super::CalibrationConfig,
    report: &mut ConfigValidationReport,
) {
    if calibration.fused_p_floor >= calibration.fused_p_ceiling {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "detection.calibration.fused_p_floor",
            value_low: calibration.fused_p_floor,
            field_high: "detection.calibration.fused_p_ceiling",
            value_high: calibration.fused_p_ceiling,
        });
    }
    if calibration.fused_p_floor < Decimal::ZERO || calibration.fused_p_ceiling > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.calibration",
            detail: "fused_p_floor/fused_p_ceiling must be within [0, 1]".into(),
        });
    }
    if calibration.refresh_interval_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.calibration.refresh_interval_secs",
            detail: "must be > 0".into(),
        });
    }
    if calibration.min_sample_size == 0 || calibration.fusion_prior_strength == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.calibration",
            detail: "min_sample_size and fusion_prior_strength must be >= 1".into(),
        });
    }
    if calibration.bootstrap_alpha <= Decimal::ZERO || calibration.bootstrap_beta <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.calibration",
            detail: "bootstrap_alpha and bootstrap_beta must be > 0".into(),
        });
    }
}

fn validate_execution(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let execution = &config.execution;
    if execution.timeout.max_validation_slippage_bps < Decimal::ZERO
        || execution.timeout.max_validation_slippage_bps > dec!(10_000)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.timeout.max_validation_slippage_bps",
            detail: "must be in [0, 10000] basis points".into(),
        });
    }
    for (field, value) in [
        (
            "execution.timeout.dispatcher_timeout_ms",
            execution.timeout.dispatcher_timeout_ms,
        ),
        (
            "execution.timeout.trade_confirm_timeout_secs",
            execution.timeout.trade_confirm_timeout_secs,
        ),
        (
            "execution.timeout.trade_confirm_poll_interval_secs",
            execution.timeout.trade_confirm_poll_interval_secs,
        ),
        (
            "execution.funnel.min_dispatch_interval_ms",
            execution.funnel.min_dispatch_interval_ms,
        ),
        (
            "execution.coalescer.coalesce_window_ms",
            execution.coalescer.coalesce_window_ms,
        ),
        (
            "execution.endgame_latency.max_book_to_order_ms",
            execution.endgame_latency.max_book_to_order_ms,
        ),
    ] {
        if value == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }
    if execution.funnel.max_queue_size == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.funnel.max_queue_size",
            detail: "must be > 0".into(),
        });
    }
    if execution.timeout.trade_confirm_poll_interval_secs
        > execution.timeout.trade_confirm_timeout_secs
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.timeout.trade_confirm_poll_interval_secs",
            detail: "must be <= trade_confirm_timeout_secs \
                     (a poll slower than the confirm budget never observes it)"
                .into(),
        });
    }
    let threshold = execution.endgame_latency.dispatch_immediate_threshold;
    if threshold < Decimal::ZERO || threshold > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.endgame_latency.dispatch_immediate_threshold",
            detail: "must be in [0, 1]".into(),
        });
    }
}

fn validate_risk(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    validate_risk_limits(&config.risk, report);
    validate_risk_sizing(&config.risk, report);
    validate_risk_operations(&config.risk, report);
    validate_risk_blacklist(&config.risk, report);
}

/// Positivity for every cap that bounds money at risk: a zero or negative
/// cap silently disables the guard it is supposed to provide.
fn validate_risk_money_caps(r: &RiskConfig, report: &mut ConfigValidationReport) {
    for (field, value) in [
        ("risk.min_depth_usd", r.min_depth_usd),
        ("risk.max_hourly_loss_usd", r.max_hourly_loss_usd),
        ("risk.max_daily_loss_usd", r.max_daily_loss_usd),
        ("risk.max_single_loss_usd", r.max_single_loss_usd),
        ("risk.max_weekly_loss_usd", r.max_weekly_loss_usd),
        ("risk.max_total_exposure_usd", r.max_total_exposure_usd),
        (
            "risk.max_single_market_exposure_usd",
            r.max_single_market_exposure_usd,
        ),
        ("risk.max_single_bet_usd", r.max_single_bet_usd),
        ("risk.min_trade_usd", r.min_trade_usd),
        ("risk.min_balance_usd", r.min_balance_usd),
        ("risk.bankroll_usd", r.bankroll_usd),
    ] {
        if value <= Decimal::ZERO {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }
    if r.reserve_balance_usd < Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.reserve_balance_usd",
            detail: "must be >= 0".into(),
        });
    }
}

fn validate_risk_limits(r: &RiskConfig, report: &mut ConfigValidationReport) {
    validate_risk_money_caps(r, report);

    if r.max_depth_usage_pct <= Decimal::ZERO || r.max_depth_usage_pct > dec!(100) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_depth_usage_pct",
            detail: "must be in (0, 100]".into(),
        });
    }
    if r.max_total_exposure_pct <= Decimal::ZERO || r.max_total_exposure_pct > dec!(100) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_total_exposure_pct",
            detail: "must be in (0, 100]".into(),
        });
    }

    if r.max_single_bet_usd > Decimal::ZERO && r.min_trade_usd > r.max_single_bet_usd {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.min_trade_usd",
            value_low: r.min_trade_usd,
            field_high: "risk.max_single_bet_usd",
            value_high: r.max_single_bet_usd,
        });
    }

    if r.max_hourly_loss_usd > r.max_daily_loss_usd {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.max_hourly_loss_usd",
            value_low: r.max_hourly_loss_usd,
            field_high: "risk.max_daily_loss_usd",
            value_high: r.max_daily_loss_usd,
        });
    }
    if r.max_daily_loss_usd > r.max_weekly_loss_usd {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.max_daily_loss_usd",
            value_low: r.max_daily_loss_usd,
            field_high: "risk.max_weekly_loss_usd",
            value_high: r.max_weekly_loss_usd,
        });
    }

    for (field, value) in [
        ("risk.max_hourly_fee_spend_usd", r.max_hourly_fee_spend_usd),
        ("risk.max_daily_fee_spend_usd", r.max_daily_fee_spend_usd),
    ] {
        if value <= Decimal::ZERO {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }
    if r.max_hourly_fee_spend_usd > r.max_daily_fee_spend_usd {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.max_hourly_fee_spend_usd",
            value_low: r.max_hourly_fee_spend_usd,
            field_high: "risk.max_daily_fee_spend_usd",
            value_high: r.max_daily_fee_spend_usd,
        });
    }

    if r.max_single_bet_usd > Decimal::ZERO
        && r.max_single_market_exposure_usd > Decimal::ZERO
        && r.max_single_bet_usd > r.max_single_market_exposure_usd
    {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.max_single_bet_usd",
            value_low: r.max_single_bet_usd,
            field_high: "risk.max_single_market_exposure_usd",
            value_high: r.max_single_market_exposure_usd,
        });
    }
    if r.max_single_market_exposure_usd > Decimal::ZERO
        && r.max_total_exposure_usd > Decimal::ZERO
        && r.max_single_market_exposure_usd > r.max_total_exposure_usd
    {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.max_single_market_exposure_usd",
            value_low: r.max_single_market_exposure_usd,
            field_high: "risk.max_total_exposure_usd",
            value_high: r.max_total_exposure_usd,
        });
    }

    if r.reserve_balance_usd >= r.bankroll_usd && r.bankroll_usd > Decimal::ZERO {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.reserve_balance_usd",
            value_low: r.reserve_balance_usd,
            field_high: "risk.bankroll_usd",
            value_high: r.bankroll_usd,
        });
    }
}

/// Position-sizing invariants (Kelly fraction, Kelly guards, drawdown).
fn validate_risk_sizing(r: &RiskConfig, report: &mut ConfigValidationReport) {
    let kelly = r.kelly_fraction;
    if kelly <= Decimal::ZERO || kelly > Decimal::ONE {
        report
            .errors
            .push(ConfigValidationError::InvalidKellyFraction(kelly));
    } else if kelly > dec!(0.5) {
        report
            .warnings
            .push(ConfigWarning::LargeKellyFraction(kelly));
    }

    let guards = &r.kelly;
    if guards.max_kelly <= Decimal::ZERO || guards.max_kelly > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.kelly.max_kelly",
            detail: "must be in (0, 1]".into(),
        });
    }
    if guards.min_edge_bps < Decimal::ZERO || guards.min_edge_bps > dec!(10_000) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.kelly.min_edge_bps",
            detail: "must be in [0, 10000] basis points".into(),
        });
    }
    if guards.min_probability_confidence < Decimal::ZERO
        || guards.min_probability_confidence > Decimal::ONE
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.kelly.min_probability_confidence",
            detail: "must be in [0, 1]".into(),
        });
    }
    if guards.min_calibration_samples == 0 || guards.max_probability_staleness_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.kelly",
            detail: "min_calibration_samples and max_probability_staleness_secs must be >= 1"
                .into(),
        });
    }
}

fn validate_risk_operations(r: &RiskConfig, report: &mut ConfigValidationReport) {
    if r.daily_budget_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.daily_budget_usd",
            detail: "must be > 0".into(),
        });
    }

    if r.metrics_refresh_interval_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.metrics_refresh_interval_secs",
            detail: "must be > 0".into(),
        });
    }
    if r.max_metrics_staleness_secs < r.metrics_refresh_interval_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_metrics_staleness_secs",
            detail: "must be >= risk.metrics_refresh_interval_secs".into(),
        });
    }
    if r.reconciliation_interval_secs == 0 || r.reservation_gc_interval_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk",
            detail: "reconciliation_interval_secs and reservation_gc_interval_secs must be > 0"
                .into(),
        });
    }
    if r.reconciliation_tolerance_usd < Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.reconciliation_tolerance_usd",
            detail: "must be >= 0".into(),
        });
    }

    // Adaptive miss cooldown: zero/inverted values break the backoff ladder.
    if r.base_cooldown_secs == 0 || r.max_cooldown_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk",
            detail: "base_cooldown_secs and max_cooldown_secs must be > 0".into(),
        });
    } else if r.base_cooldown_secs > r.max_cooldown_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.base_cooldown_secs",
            detail: "must be <= risk.max_cooldown_secs".into(),
        });
    }
    if r.cooldown_multiplier < Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.cooldown_multiplier",
            detail: "must be >= 1".into(),
        });
    }

    if r.api_error_rate_threshold < Decimal::ZERO || r.api_error_rate_threshold > Decimal::ONE {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.api_error_rate_threshold",
            detail: "must be in [0, 1]".into(),
        });
    }

    // Concurrency / counter guards: zero would deadlock or disable the path.
    if r.max_open_positions == 0
        || r.max_concurrent_directional == 0
        || r.daily_directional_budget == 0
        || r.max_consecutive_misses == 0
        || r.heartbeat_max_failures == 0
        || r.reservation_ttl_secs == 0
        || r.ws_disconnect_threshold_secs == 0
        || r.potential_loss_escalation_secs == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk",
            detail: "position/counter/threshold limits (max_open_positions, \
                     max_concurrent_directional, daily_directional_budget, \
                     max_consecutive_misses, heartbeat_max_failures, \
                     reservation_ttl_secs, ws_disconnect_threshold_secs, \
                     potential_loss_escalation_secs) must be >= 1"
                .into(),
        });
    }

    validate_circuit_breaker(&r.circuit_breaker, report);

    let dd = &r.drawdown;
    if dd.max_drawdown_pct <= Decimal::ZERO || dd.max_drawdown_pct > dec!(100) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.drawdown.max_drawdown_pct",
            detail: "must be in (0, 100]".into(),
        });
    }
    if dd.drawdown_reduction_factor <= Decimal::ZERO || dd.drawdown_reduction_factor > Decimal::ONE
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.drawdown.drawdown_reduction_factor",
            detail: "must be in (0, 1]".into(),
        });
    }
}

/// Circuit-breaker recovery FSM invariants: every cooldown level positive, at
/// least one `HalfOpen` probe, and the L2 back-off ceiling above its base.
fn validate_circuit_breaker(cb: &super::CircuitBreakerConfig, report: &mut ConfigValidationReport) {
    for (field, val) in [
        ("risk.circuit_breaker.l1_cooldown_secs", cb.l1_cooldown_secs),
        ("risk.circuit_breaker.l2_cooldown_secs", cb.l2_cooldown_secs),
        ("risk.circuit_breaker.l3_cooldown_secs", cb.l3_cooldown_secs),
        ("risk.circuit_breaker.l4_cooldown_secs", cb.l4_cooldown_secs),
        (
            "risk.circuit_breaker.recovery_observation_secs",
            cb.recovery_observation_secs,
        ),
        (
            "risk.circuit_breaker.max_cooldown_secs",
            cb.max_cooldown_secs,
        ),
    ] {
        if val == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }
    if cb.half_open_probes == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.circuit_breaker.half_open_probes",
            detail: "must be >= 1 (the recovery FSM needs at least one probe)".into(),
        });
    }
    if cb.max_cooldown_secs < cb.l2_cooldown_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.circuit_breaker.max_cooldown_secs",
            detail: "must be >= l2_cooldown_secs (it caps the L2 exponential back-off)".into(),
        });
    }
}

/// Permanent blacklist entries must be well-formed identifiers: a typo here
/// would silently fail to block the intended market/token.
fn validate_risk_blacklist(r: &RiskConfig, report: &mut ConfigValidationReport) {
    if r.market_miss_blacklist_count == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.market_miss_blacklist_count",
            detail: "must be >= 1 (zero would auto-blacklist every market instantly)".into(),
        });
    }
    if r.market_miss_blacklist_duration_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.market_miss_blacklist_duration_secs",
            detail: "must be > 0".into(),
        });
    }
    for market in &r.permanent_blacklist_markets {
        if !is_condition_id(market.trim()) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "risk.permanent_blacklist_markets",
                detail: format!("'{market}' is not a condition id (0x + 64 hex chars expected)"),
            });
        }
    }
    for token in &r.permanent_blacklist_tokens {
        let token = token.trim();
        if token.is_empty() || !token.chars().all(|c| c.is_ascii_digit()) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "risk.permanent_blacklist_tokens",
                detail: format!("'{token}' is not a decimal CLOB token id"),
            });
        }
    }
}

fn validate_notification(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.notification.alert_cooldown_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "notification.alert_cooldown_secs",
            detail: "must be > 0 (zero floods every channel)".into(),
        });
    }
}

fn validate_settlement(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let lifecycle = &config.settlement.lifecycle;
    let redeem = &config.settlement.redeem;

    if lifecycle.retry_interval_secs < 10 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.retry_interval_secs",
            detail: "must be >= 10".into(),
        });
    }
    if lifecycle.max_redeem_attempts == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.max_redeem_attempts",
            detail: "must be >= 1".into(),
        });
    }
    if lifecycle.dedup_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.dedup_window_secs",
            detail: "must be > 0".into(),
        });
    }
    if redeem.gas_limit == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.redeem.gas_limit",
            detail: "must be > 0".into(),
        });
    }

    for (field, value) in [
        (
            "settlement.redeem.holder_address",
            redeem.holder_address.as_deref(),
        ),
        (
            "settlement.redeem.proxy_safe_address",
            redeem.proxy_safe_address.as_deref(),
        ),
    ] {
        if let Some(address) = value {
            validate_address(field, address, report);
        }
    }

    let oracle = &config.settlement.oracle;
    if oracle.voting_quorum == 0 || oracle.voting_quorum > 3 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.oracle.voting_quorum",
            detail: "must be in [1, 3] (Gamma / CTF / UMA)".into(),
        });
    }
    if oracle.uma_timeout_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.oracle.uma_timeout_secs",
            detail: "must be > 0".into(),
        });
    }
    if oracle.uma_endpoint.trim().is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.oracle.uma_endpoint",
            detail: "must be a non-empty URL".into(),
        });
    }
}

fn validate_settlement_mode(
    config: &RuntimeConfig,
    mode: ExecutionMode,
    report: &mut ConfigValidationReport,
) {
    if mode != ExecutionMode::Live {
        return;
    }
    let redeem = &config.settlement.redeem;
    if redeem.route == RedeemRoute::Disabled {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.redeem.route",
            detail: "Live mode requires an explicit redeem route".into(),
        });
    }
    if redeem.route == RedeemRoute::ProxySafe && redeem.proxy_safe_address.is_none() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.redeem.proxy_safe_address",
            detail: "required when settlement.redeem.route=proxy_safe".into(),
        });
    }
}

fn validate_notification_mode(
    config: &RuntimeConfig,
    mode: ExecutionMode,
    report: &mut ConfigValidationReport,
) {
    let telegram = &config.notification.telegram;
    let telegram_incomplete = telegram.enabled
        && (telegram.bot_token.trim().is_empty() || telegram.chat_id.trim().is_empty());
    let webhook = &config.notification.webhook;
    let webhook_incomplete = webhook.enabled && webhook.url.trim().is_empty();

    if mode == ExecutionMode::Live {
        // Live trades real money: an enabled-but-unreachable alert channel
        // means breaker trips and settlement failures go unseen. Fail closed.
        if telegram_incomplete {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "notification.telegram",
                detail: "enabled in Live but bot_token/chat_id is empty".into(),
            });
        }
        if webhook_incomplete {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "notification.webhook.url",
                detail: "enabled in Live but url is empty".into(),
            });
        }
    } else if telegram_incomplete || webhook_incomplete {
        tracing::warn!(
            mode = %mode,
            "notification channel enabled without credentials — alerts will be dropped"
        );
    }
}

fn is_hex_address(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 40 && hex.chars().all(|c| c.is_ascii_hexdigit()))
}

/// Polymarket condition id: `0x` followed by 64 hex characters.
fn is_condition_id(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()))
}

fn validate_address(field: &'static str, value: &str, report: &mut ConfigValidationReport) {
    if !is_hex_address(value.trim()) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: "must be a 20-byte hex address with 0x prefix".into(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn default_runtime_config_validates_clean() {
        let report = validate_runtime_config(&RuntimeConfig::default());
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn invalid_kelly_fraction_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.risk.kelly_fraction = dec!(1.5);
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn inverted_staleness_ladder_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.market_data.staleness_fresh_ms = 10_000;
        config.market_data.staleness_acceptable_ms = 5_000;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn live_requires_explicit_redeem_route() {
        let config = RuntimeConfig::default();
        let report = validate_runtime_for_mode(&config, ExecutionMode::Live);
        assert!(report.has_errors(), "disabled route must fail Live");
        assert!(
            !validate_runtime_for_mode(&config, ExecutionMode::DryRun).has_errors(),
            "DryRun tolerates a disabled route"
        );
    }

    #[test]
    fn live_rejects_enabled_telegram_without_token() {
        let mut config = RuntimeConfig::default();
        config.notification.telegram.enabled = true;
        let report = validate_runtime_for_mode(&config, ExecutionMode::Live);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.to_string().contains("notification.telegram"))
        );
        assert!(!validate_runtime_for_mode(&config, ExecutionMode::Paper).has_errors());
    }

    #[test]
    fn preflight_rejects_exposure_tightening_below_reserved() {
        let mut config = RuntimeConfig::default();
        config.risk.max_total_exposure_usd = dec!(100);
        let ctx = RuntimePreflightContext {
            mode: ExecutionMode::Paper,
            reserved_total_usd: dec!(250),
            max_market_reserved_usd: dec!(50),
        };
        let report = preflight_runtime_config(&config, &ctx);
        assert!(report.has_errors());
    }

    #[test]
    fn preflight_accepts_limits_above_reserved() {
        let config = RuntimeConfig::default();
        let ctx = RuntimePreflightContext {
            mode: ExecutionMode::Paper,
            reserved_total_usd: dec!(250),
            max_market_reserved_usd: dec!(50),
        };
        let report = preflight_runtime_config(&config, &ctx);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn zero_loss_caps_are_fatal() {
        for mutate in [
            |c: &mut RuntimeConfig| c.risk.max_daily_loss_usd = Decimal::ZERO,
            |c: &mut RuntimeConfig| c.risk.max_total_exposure_usd = dec!(-1),
            |c: &mut RuntimeConfig| c.risk.bankroll_usd = Decimal::ZERO,
            |c: &mut RuntimeConfig| c.risk.max_single_market_exposure_usd = Decimal::ZERO,
        ] {
            let mut config = RuntimeConfig::default();
            mutate(&mut config);
            assert!(
                validate_runtime_config(&config).has_errors(),
                "zero/negative money cap must be rejected"
            );
        }
    }

    #[test]
    fn kelly_guards_are_validated() {
        let mut config = RuntimeConfig::default();
        config.risk.kelly.max_kelly = dec!(1.5);
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.kelly.min_probability_confidence = dec!(-0.1);
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn circuit_breaker_recovery_invariants() {
        let mut config = RuntimeConfig::default();
        config.risk.circuit_breaker.half_open_probes = 0;
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.circuit_breaker.max_cooldown_secs = 1;
        assert!(
            validate_runtime_config(&config).has_errors(),
            "max_cooldown below l2_cooldown must be rejected"
        );
    }

    #[test]
    fn adaptive_cooldown_invariants() {
        let mut config = RuntimeConfig::default();
        config.risk.cooldown_multiplier = dec!(0.5);
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.base_cooldown_secs = 9000;
        assert!(
            validate_runtime_config(&config).has_errors(),
            "base cooldown above max must be rejected"
        );
    }

    #[test]
    fn zero_coalescer_window_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.execution.coalescer.coalesce_window_ms = 0;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn zero_alert_cooldown_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.notification.alert_cooldown_secs = 0;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn malformed_blacklist_entries_are_fatal() {
        let mut config = RuntimeConfig::default();
        config.risk.permanent_blacklist_markets = vec!["0x123".into()];
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.permanent_blacklist_tokens = vec!["0xdeadbeef".into()];
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.permanent_blacklist_markets = vec![format!("0x{}", "a".repeat(64))];
        config.risk.permanent_blacklist_tokens = vec!["12345678901234567890".into()];
        assert!(
            !validate_runtime_config(&config).has_errors(),
            "well-formed ids must pass"
        );
    }

    #[test]
    fn out_of_range_rates_are_fatal() {
        let mut config = RuntimeConfig::default();
        config.risk.api_error_rate_threshold = dec!(1.5);
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.max_total_exposure_pct = dec!(0);
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn fill_probability_penalties_are_range_checked() {
        for mutate in [
            |c: &mut RuntimeConfig| {
                c.detection
                    .endgame
                    .fill_probability
                    .depth_penalty_threshold_pct = dec!(0);
            },
            |c: &mut RuntimeConfig| {
                c.detection.endgame.fill_probability.depth_penalty_per_pct = dec!(-0.01);
            },
            |c: &mut RuntimeConfig| {
                c.detection
                    .endgame
                    .fill_probability
                    .staleness_penalty_per_level = dec!(1.5);
            },
            |c: &mut RuntimeConfig| {
                c.detection
                    .endgame
                    .fill_probability
                    .resolution_proximity_bonus = dec!(2);
            },
        ] {
            let mut config = RuntimeConfig::default();
            mutate(&mut config);
            assert!(
                validate_runtime_config(&config).has_errors(),
                "out-of-range fill-probability parameter must be rejected"
            );
        }
    }

    #[test]
    fn zero_min_convergence_duration_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.detection.endgame.min_convergence_duration_secs = 0;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn auto_blacklist_thresholds_are_fatal_at_zero() {
        let mut config = RuntimeConfig::default();
        config.risk.market_miss_blacklist_count = 0;
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.market_miss_blacklist_duration_secs = 0;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn negative_reconciliation_tolerance_is_fatal() {
        let mut config = RuntimeConfig::default();
        config.risk.reconciliation_tolerance_usd = dec!(-0.01);
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn trade_confirm_poll_must_fit_in_confirm_budget() {
        let mut config = RuntimeConfig::default();
        config.execution.timeout.trade_confirm_timeout_secs = 10;
        config.execution.timeout.trade_confirm_poll_interval_secs = 30;
        assert!(validate_runtime_config(&config).has_errors());
    }

    #[test]
    fn basis_point_fields_are_capped_at_10000() {
        let mut config = RuntimeConfig::default();
        config.execution.timeout.max_validation_slippage_bps = dec!(20_000);
        assert!(validate_runtime_config(&config).has_errors());

        let mut config = RuntimeConfig::default();
        config.risk.kelly.min_edge_bps = dec!(10_001);
        assert!(validate_runtime_config(&config).has_errors());
    }
}
