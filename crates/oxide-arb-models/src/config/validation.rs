//! Startup config validation: detects infeasible ranges, contradictions, and
//! cross-section inconsistencies that would cause silent failure at runtime.
//!
//! Validation is split into **mode-agnostic** and **mode-aware** halves.
//! The mode-agnostic half is called during [`super::Settings::new`]
//! (see [`validate_settings_common`]); errors there abort startup
//! regardless of which subcommand the operator invoked. The mode-aware
//! half is called from the CLI runner once the final [`ExecutionMode`]
//! has been determined (see [`validate_settings_mode`]).
//!
//! # Mode → severity matrix
//!
//! | Polymarket creds   | `DryRun` | `Paper` | `Live` |
//! |--------------------|----------|---------|--------|
//! | all populated      | pass     | pass    | pass   |
//! | all empty          | pass     | warn    | fatal  |
//! | partial (1-3 set)  | warn     | fatal   | fatal  |

use super::Inner;
use crate::{
    constants::POLYGON_CHAIN_ID,
    enums::common::{ExecutionMode, RedeemRoute},
};
use oxide_arb_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn validate_risk_cross_constraints(inner: &Inner, report: &mut ConfigValidationReport) {
    let r = &inner.risk;

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

    if r.daily_budget_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.daily_budget_usd",
            detail: "must be > 0".into(),
        });
    }

    let cb = &r.circuit_breaker;
    for (field, val) in [
        ("risk.circuit_breaker.l1_cooldown_secs", cb.l1_cooldown_secs),
        ("risk.circuit_breaker.l2_cooldown_secs", cb.l2_cooldown_secs),
        ("risk.circuit_breaker.l3_cooldown_secs", cb.l3_cooldown_secs),
        ("risk.circuit_breaker.l4_cooldown_secs", cb.l4_cooldown_secs),
    ] {
        if val == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field,
                detail: "must be > 0".into(),
            });
        }
    }

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

fn is_hex_address(value: &str) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 40 && hex.chars().all(|c| c.is_ascii_hexdigit()))
}

fn validate_address(field: &'static str, value: &str, report: &mut ConfigValidationReport) {
    if !is_hex_address(value.trim()) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: "must be a 20-byte hex address with 0x prefix".into(),
        });
    }
}

fn validate_settlement_common(inner: &Inner, report: &mut ConfigValidationReport) {
    let lifecycle = &inner.settlement.lifecycle;
    let contracts = &inner.settlement.contracts;
    let redeem = &inner.settlement.redeem;

    if lifecycle.channel_capacity < 16 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.channel_capacity",
            detail: "must be >= 16".into(),
        });
    }

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

    if redeem.gas_limit == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.redeem.gas_limit",
            detail: "must be > 0".into(),
        });
    }

    if lifecycle.dedup_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.dedup_window_secs",
            detail: "must be > 0".into(),
        });
    }

    validate_address(
        "settlement.contracts.ctf_address",
        &contracts.ctf_address,
        report,
    );
    validate_address(
        "settlement.contracts.usdc_e_address",
        &contracts.usdc_e_address,
        report,
    );
    validate_address(
        "settlement.contracts.standard_ctf_exchange",
        &contracts.standard_ctf_exchange,
        report,
    );
    validate_address(
        "settlement.contracts.neg_risk_ctf_exchange",
        &contracts.neg_risk_ctf_exchange,
        report,
    );

    for (field, value) in [
        (
            "settlement.contracts.neg_risk_adapter",
            contracts.neg_risk_adapter.as_deref(),
        ),
        (
            "settlement.contracts.ctf_collateral_adapter",
            contracts.ctf_collateral_adapter.as_deref(),
        ),
        (
            "settlement.contracts.neg_risk_collateral_adapter",
            contracts.neg_risk_collateral_adapter.as_deref(),
        ),
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
}

fn validate_settlement_mode(
    inner: &Inner,
    mode: ExecutionMode,
    report: &mut ConfigValidationReport,
) {
    if inner.polymarket.chain_id != POLYGON_CHAIN_ID {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.chain_id",
            detail: format!("must be Polygon chain id {POLYGON_CHAIN_ID}"),
        });
    }

    if mode != ExecutionMode::Live {
        return;
    }

    let contracts = &inner.settlement.contracts;
    let redeem = &inner.settlement.redeem;

    if redeem.route == RedeemRoute::Disabled {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.redeem.route",
            detail: "Live mode requires an explicit redeem route".into(),
        });
    }

    match redeem.route {
        RedeemRoute::NegRiskLegacyAdapter if contracts.neg_risk_adapter.is_none() => {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "settlement.contracts.neg_risk_adapter",
                detail: "required when settlement.redeem.route=neg_risk_legacy_adapter".into(),
            });
        }
        RedeemRoute::CtfCollateralAdapter if contracts.ctf_collateral_adapter.is_none() => {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "settlement.contracts.ctf_collateral_adapter",
                detail: "required when settlement.redeem.route=ctf_collateral_adapter".into(),
            });
        }
        RedeemRoute::NegRiskCollateralAdapter
            if contracts.neg_risk_collateral_adapter.is_none() =>
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "settlement.contracts.neg_risk_collateral_adapter",
                detail: "required when settlement.redeem.route=neg_risk_collateral_adapter".into(),
            });
        }
        RedeemRoute::ProxySafe if redeem.proxy_safe_address.is_none() => {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "settlement.redeem.proxy_safe_address",
                detail: "required when settlement.redeem.route=proxy_safe".into(),
            });
        }
        _ => {}
    }
}

/// Mode-agnostic validation: checks mathematical invariants that must hold
/// regardless of execution mode.
pub fn validate_settings_common(inner: &Inner) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    let kelly = inner.risk.kelly_fraction;
    if kelly <= Decimal::ZERO || kelly > Decimal::ONE {
        report
            .errors
            .push(ConfigValidationError::InvalidKellyFraction(kelly));
    } else if kelly > dec!(0.5) {
        report
            .warnings
            .push(ConfigWarning::LargeKellyFraction(kelly));
    }

    let high = inner.detection.endgame.high_threshold;
    let low = inner.detection.endgame.low_threshold;
    if high <= low {
        report
            .errors
            .push(ConfigValidationError::InvertedEndgameThresholds { high, low });
    }

    if inner.risk.max_single_bet_usd > Decimal::ZERO
        && inner.risk.min_trade_usd > inner.risk.max_single_bet_usd
    {
        report.errors.push(ConfigValidationError::InfeasibleRange {
            field_low: "risk.min_trade_usd",
            value_low: inner.risk.min_trade_usd,
            field_high: "risk.max_single_bet_usd",
            value_high: inner.risk.max_single_bet_usd,
        });
    }

    if inner.detection.min_profit_threshold_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "detection.min_profit_threshold_usd",
            detail: "must be > 0".into(),
        });
    }

    if inner.risk.min_depth_usd <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.min_depth_usd",
            detail: "must be > 0".into(),
        });
    }

    if inner.risk.max_depth_usage_pct <= Decimal::ZERO || inner.risk.max_depth_usage_pct > dec!(100)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "risk.max_depth_usage_pct",
            detail: "must be in (0, 100]".into(),
        });
    }

    // Depth check intentionally removed: min_depth_usd (order book depth
    // requirement) is not directly comparable to max_single_bet_usd (bet size cap).
    // The old validation compared against the deleted max_single_trade_usd.

    validate_risk_cross_constraints(inner, &mut report);
    validate_settlement_common(inner, &mut report);

    if inner.polymarket.fees.exponent <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.exponent",
            detail: "must be > 0".into(),
        });
    }

    for (category, rate) in &inner.polymarket.fees.category_rates {
        if *rate < Decimal::ZERO || *rate > dec!(1) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.fees.category_rates",
                detail: format!("{category} rate {rate} out of range [0, 1]"),
            });
        }
    }

    if inner.polymarket.fees.unknown_category_rate < Decimal::ZERO
        || inner.polymarket.fees.unknown_category_rate > dec!(1)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.unknown_category_rate",
            detail: "must be in [0, 1]".into(),
        });
    }

    report
}

/// Mode-aware validation: enforces credential policies based on the
/// execution mode that will actually run.
pub fn validate_settings_mode(inner: &Inner, mode: ExecutionMode) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    let keys = &inner.keys;
    let mut present = Vec::new();
    let mut missing = Vec::new();

    if keys.polymarket_api_key.is_some() {
        present.push("api_key");
    } else {
        missing.push("api_key");
    }
    if keys.polymarket_api_secret.is_some() {
        present.push("api_secret");
    } else {
        missing.push("api_secret");
    }
    if keys.polymarket_passphrase.is_some() {
        present.push("passphrase");
    } else {
        missing.push("passphrase");
    }
    if keys.private_key.is_some() {
        present.push("private_key");
    } else {
        missing.push("private_key");
    }

    let all_present = missing.is_empty();
    let all_missing = present.is_empty();
    let mode_label = mode.to_string();

    match mode {
        ExecutionMode::DryRun => {
            if !all_present && !all_missing {
                report
                    .warnings
                    .push(ConfigWarning::PartialCredentialsDryRun {
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
        ExecutionMode::Paper => {
            if all_missing {
                report.warnings.push(ConfigWarning::NoCredentialsPaper);
            } else if !all_present {
                report
                    .errors
                    .push(ConfigValidationError::PartialCredentials {
                        mode: mode_label,
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
        ExecutionMode::Live => {
            if all_missing {
                report
                    .errors
                    .push(ConfigValidationError::MissingCredentials {
                        mode: mode_label,
                        missing: missing.clone(),
                    });
            } else if !all_present {
                report
                    .errors
                    .push(ConfigValidationError::PartialCredentials {
                        mode: mode_label,
                        present: present.clone(),
                        missing: missing.clone(),
                    });
            }
        }
    }

    validate_settlement_mode(inner, mode, &mut report);

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_validation_passes_for_defaults() {
        let inner = Inner::default();
        let report = validate_settings_common(&inner);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn inverted_kelly_fraction_is_fatal() {
        let mut inner = Inner::default();
        inner.risk.kelly_fraction = dec!(1.5);
        let report = validate_settings_common(&inner);
        assert!(report.has_errors());
    }

    #[test]
    fn inverted_thresholds_is_fatal() {
        let mut inner = Inner::default();
        inner.detection.endgame.high_threshold = dec!(0.03);
        inner.detection.endgame.low_threshold = dec!(0.95);
        let report = validate_settings_common(&inner);
        assert!(report.has_errors());
    }

    #[test]
    fn live_mode_requires_all_credentials() {
        let inner = Inner::default();
        let report = validate_settings_mode(&inner, ExecutionMode::Live);
        assert!(report.has_errors());
    }

    #[test]
    fn dry_run_permits_empty_credentials() {
        let inner = Inner::default();
        let report = validate_settings_mode(&inner, ExecutionMode::DryRun);
        assert!(!report.has_errors());
    }
}
