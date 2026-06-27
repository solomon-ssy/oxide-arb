//! Runtime-config v5 semantic validation.
//!
//! This module validates only the quant-pivot v5 document. Legacy Endgame
//! Deleted pre-quant configuration paths are not accepted in Phase 1 clean-break
//! runtime configuration.

use super::{
    DecimalString, RuntimeConfig, ScheduleCadence, SizingModelConfig, sections::FeaturesConfig,
};
use linkme::distributed_slice;
use quant_pivot_error::config_validation::{ConfigValidationError, ConfigValidationReport};
use rust_decimal::Decimal;
use std::collections::HashSet;

/// Extension hook for feature-config validation that requires research-plane schema knowledge.
///
/// Crates such as `quant-pivot-research` register validators here at link time so
/// [`validate_features`] remains the single features entry point inside
/// [`validate_runtime_config`].
pub type FeaturesConfigValidator = fn(&FeaturesConfig, &mut ConfigValidationReport);

#[allow(unsafe_code)]
#[distributed_slice]
pub static FEATURES_CONFIG_VALIDATORS: [FeaturesConfigValidator] = [..];

/// Mode-agnostic runtime-config v5 invariants.
#[must_use]
pub fn validate_runtime_config(config: &RuntimeConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_selection(config, &mut report);
    validate_data_quality(config, &mut report);
    validate_features(config, &mut report);
    validate_factors(config, &mut report);
    validate_model(config, &mut report);
    validate_quality_gate(config, &mut report);
    validate_training(config, &mut report);
    validate_reports(config, &mut report);
    validate_portfolio(config, &mut report);
    validate_execution(config, &mut report);
    validate_notification(config, &mut report);
    report
}

fn validate_selection(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "selection.min_liquidity_usd",
        &config.selection.min_liquidity_usd,
        report,
    );
    decimal(
        "selection.min_volume_24h_usd",
        &config.selection.min_volume_24h_usd,
        report,
    );
    if config.selection.min_time_to_resolution_secs > config.selection.max_time_to_resolution_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "selection.min_time_to_resolution_secs",
            detail: "must be <= selection.max_time_to_resolution_secs".to_owned(),
        });
    }
    if config.selection.max_selection_size == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "selection.max_selection_size",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_data_quality(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "data_quality.min_book_depth_usd",
        &config.data_quality.min_book_depth_usd,
        report,
    );
    if config.data_quality.max_book_age_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_book_age_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
    if config.data_quality.max_stale_book_ratio_bps > 10_000 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "data_quality.max_stale_book_ratio_bps",
            detail: "must be <= 10000 (100%)".to_owned(),
        });
    }
}

fn validate_features(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.features.feature_schema_version.get() <= 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "features.feature_schema_version",
            detail: "must be positive".to_owned(),
        });
    }
    non_empty_numbers(
        "features.bar_windows_secs",
        &config.features.bar_windows_secs,
        report,
    );
    non_empty_numbers(
        "features.momentum_windows_secs",
        &config.features.momentum_windows_secs,
        report,
    );
    non_empty_numbers(
        "features.volatility_windows_secs",
        &config.features.volatility_windows_secs,
        report,
    );
    let mut seen = HashSet::new();
    for feature_ref in &config.features.required_features {
        let label = feature_ref.name.trim();
        if label.is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.required_features",
                detail: "feature names must not be empty".to_owned(),
            });
            continue;
        }
        if !seen.insert(label.to_owned()) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "features.required_features",
                detail: format!("duplicate feature `{label}`"),
            });
        }
    }
    for validator in FEATURES_CONFIG_VALIDATORS {
        validator(&config.features, report);
    }
}

fn validate_factors(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "factors.min_factor_confidence",
        &config.factors.min_factor_confidence,
        report,
    );
    if config.factors.enabled_factor_families.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "factors.enabled_factor_families",
            detail: "must contain at least one generic factor family".to_owned(),
        });
    }
    let mut seen = HashSet::new();
    for family in &config.factors.enabled_factor_families {
        if !family.is_generic() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.enabled_factor_families",
                detail: format!(
                    "domain factor family `{family}` must not appear in config; vertical factors are routed by market category"
                ),
            });
        }
        if !seen.insert(*family) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.enabled_factor_families",
                detail: format!("duplicate family `{family}`"),
            });
        }
    }
    for (name, weight) in &config.factors.factor_weights.weights {
        if name.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "factors.factor_weights",
                detail: "factor names must not be empty".to_owned(),
            });
        }
        decimal("factors.factor_weights", weight, report);
    }
}

fn validate_model(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "model.min_model_confidence",
        &config.model.min_model_confidence,
        report,
    );
    decimal(
        "model.candidate_score_floor",
        &config.model.candidate_score_floor,
        report,
    );
    decimal(
        "model.shadow_diff_threshold",
        &config.model.shadow_diff_threshold,
        report,
    );
    if config.model.prediction_horizon_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "model.prediction_horizon_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_quality_gate(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let gate = &config.quality_gate;
    unit_ratio(
        "quality_gate.min_label_coverage",
        &gate.min_label_coverage,
        report,
    );
    unit_ratio(
        "quality_gate.min_critical_feature_coverage",
        &gate.min_critical_feature_coverage,
        report,
    );
    unit_ratio("quality_gate.max_drawdown", &gate.max_drawdown, report);
    unit_ratio(
        "quality_gate.min_liquidity_exit_feasibility",
        &gate.min_liquidity_exit_feasibility,
        report,
    );
    unit_ratio(
        "quality_gate.min_shadow_overlap_stability",
        &gate.min_shadow_overlap_stability,
        report,
    );
    unit_ratio(
        "quality_gate.max_category_concentration",
        &gate.max_category_concentration,
        report,
    );
    // rank IC is a correlation in [-1, 1]; only validate it parses.
    decimal("quality_gate.min_rank_ic", &gate.min_rank_ic, report);
    if gate.required_shadow_window_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quality_gate.required_shadow_window_secs",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_training(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "training.min_exit_depth_usd",
        &config.training.min_exit_depth_usd,
        report,
    );
    if config.training.max_book_staleness_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "training.max_book_staleness_ms",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_reports(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.reports.default_top_n == 0 || config.reports.default_top_n > config.reports.max_top_n
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "reports.default_top_n",
            detail: "must be in 1..=reports.max_top_n".to_owned(),
        });
    }
    for schedule in &config.reports.schedules {
        if schedule.schedule_id.trim().is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.schedule_id",
                detail: "must not be empty".to_owned(),
            });
        }
        if schedule.enabled {
            validate_cadence(&schedule.cadence, report);
        }
        if schedule.top_n == 0 || schedule.top_n > config.reports.max_top_n {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.top_n",
                detail: "must be in 1..=reports.max_top_n".to_owned(),
            });
        }
    }
}

/// Validate a schedule cadence. Cron expressions get a structural check only;
/// full parsing happens in the 04.3 scheduler runner.
fn validate_cadence(cadence: &ScheduleCadence, report: &mut ConfigValidationReport) {
    match cadence {
        ScheduleCadence::Interval { interval_secs } => {
            if *interval_secs == 0 {
                report.errors.push(ConfigValidationError::InvalidValue {
                    field: "reports.schedules.cadence.interval_secs",
                    detail: "enabled interval schedules must have a positive interval".to_owned(),
                });
            }
        }
        ScheduleCadence::Cron { expr, .. } => {
            if expr.split_whitespace().count() != 6 {
                report.errors.push(ConfigValidationError::InvalidValue {
                    field: "reports.schedules.cadence.expr",
                    detail: "cron expression must have 6 whitespace-separated fields".to_owned(),
                });
            }
        }
    }
}

fn validate_portfolio(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    let budget = &config.portfolio.budget;
    non_negative_decimal(
        "portfolio.budget.total_budget_usd",
        &budget.total_budget_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.budget.min_recommendation_usd",
        &budget.min_recommendation_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.budget.max_single_recommendation_usd",
        &budget.max_single_recommendation_usd,
        report,
    );

    let constraints = &config.portfolio.constraints;
    non_negative_decimal(
        "portfolio.constraints.max_market_exposure_usd",
        &constraints.max_market_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_event_exposure_usd",
        &constraints.max_event_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_category_exposure_usd",
        &constraints.max_category_exposure_usd,
        report,
    );
    non_negative_decimal(
        "portfolio.constraints.max_correlated_exposure_usd",
        &constraints.max_correlated_exposure_usd,
        report,
    );
    unit_ratio(
        "portfolio.constraints.liquidity_usage_cap_pct",
        &constraints.liquidity_usage_cap_pct,
        report,
    );

    validate_sizing(&config.portfolio.sizing, report);
}

/// Validate the sizing model parameters.
fn validate_sizing(sizing: &SizingModelConfig, report: &mut ConfigValidationReport) {
    half_open_unit(
        "portfolio.sizing.kelly_fraction",
        &sizing.kelly_fraction,
        report,
    );
    half_open_unit(
        "portfolio.sizing.max_position_pct",
        &sizing.max_position_pct,
        report,
    );
    match sizing.target_reward_multiple.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "portfolio.sizing.target_reward_multiple",
            detail: format!("`{parsed}` must be > 0"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field: "portfolio.sizing.target_reward_multiple",
            detail: format!(
                "`{}` is not a valid decimal string",
                sizing.target_reward_multiple.value
            ),
        }),
    }
}

fn validate_execution(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.execution.runtime_mode.allows_auto_execution()
        && !config.execution.auto_execution.enabled
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.auto_execution.enabled",
            detail: "must be true when runtime_mode is auto_execution".to_owned(),
        });
    }
    decimal(
        "execution.auto_execution.max_total_usd_per_report",
        &config.execution.auto_execution.max_total_usd_per_report,
        report,
    );
    decimal(
        "execution.auto_execution.min_score",
        &config.execution.auto_execution.min_score,
        report,
    );
    decimal(
        "execution.auto_execution.min_confidence",
        &config.execution.auto_execution.min_confidence,
        report,
    );
    decimal(
        "execution.admission.min_score",
        &config.execution.admission.min_score,
        report,
    );
    decimal(
        "execution.admission.min_confidence",
        &config.execution.admission.min_confidence,
        report,
    );
    decimal(
        "execution.capital.max_reserved_usd",
        &config.execution.capital.max_reserved_usd,
        report,
    );
    if config.execution.kill_switch.emergency_exit.max_slippage_bps == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.kill_switch.emergency_exit.max_slippage_bps",
            detail: "must be greater than zero".to_owned(),
        });
    }
}

fn validate_notification(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.execution.runtime_mode.allows_order_submission()
        && config.notification.telegram.bot_token.trim().is_empty()
        && config.notification.webhook.url.trim().is_empty()
    {
        report
            .errors
            .push(ConfigValidationError::MissingCredentials {
                mode: config.execution.runtime_mode.as_str().to_owned(),
                missing: vec![
                    "notification.telegram.bot_token",
                    "notification.webhook.url",
                ],
            });
    }
}

fn decimal(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    if value.value.parse::<Decimal>().is_err() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        });
    }
}

/// Validate a non-negative decimal (parses and is `>= 0`).
fn non_negative_decimal(
    field: &'static str,
    value: &DecimalString,
    report: &mut ConfigValidationReport,
) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed >= Decimal::ZERO => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be >= 0"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a `[0, 1]` ratio: parses as a decimal and lies within the unit range.
fn unit_ratio(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if (Decimal::ZERO..=Decimal::ONE).contains(&parsed) => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be within [0, 1]"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

/// Validate a half-open unit ratio in `(0, 1]` (the value must be strictly
/// positive — a zero fraction would size nothing — and at most full).
fn half_open_unit(field: &'static str, value: &DecimalString, report: &mut ConfigValidationReport) {
    match value.value.parse::<Decimal>() {
        Ok(parsed) if parsed > Decimal::ZERO && parsed <= Decimal::ONE => {}
        Ok(parsed) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{parsed}` must be within (0, 1]"),
        }),
        Err(_) => report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: format!("`{}` is not a valid decimal string", value.value),
        }),
    }
}

fn non_empty_numbers(field: &'static str, values: &[u64], report: &mut ConfigValidationReport) {
    if values.is_empty() || values.contains(&0) {
        report.errors.push(ConfigValidationError::InvalidValue {
            field,
            detail: "must contain at least one positive value".to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeConfig, validate_runtime_config};
    use crate::{
        enums::{factor::FactorFamily, quant::QuantRuntimeMode},
        runtime_config::{DecimalString, FeatureNameRef, RUNTIME_CONFIG_SCHEMA_VERSION},
    };

    #[test]
    fn default_runtime_config_is_valid() {
        let report = validate_runtime_config(&RuntimeConfig::default());
        assert!(!report.has_errors());
    }

    #[test]
    fn invalid_decimal_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.portfolio.budget.total_budget_usd = DecimalString::new("not-a-decimal");
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn auto_execution_requires_enabled_policy() {
        let mut config = RuntimeConfig::default();
        config.execution.runtime_mode = QuantRuntimeMode::AutoExecution;
        config.execution.auto_execution.enabled = false;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn kill_switch_emergency_exit_slippage_must_be_positive() {
        let mut config = RuntimeConfig::default();
        config.execution.kill_switch.emergency_exit.max_slippage_bps = 0;
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn schema_version_matches_constant() {
        assert_eq!(
            RuntimeConfig::default().schema_version,
            RUNTIME_CONFIG_SCHEMA_VERSION
        );
    }

    #[test]
    fn domain_family_in_config_is_rejected() {
        let mut config = RuntimeConfig::default();
        config
            .factors
            .enabled_factor_families
            .push(FactorFamily::DomainSports);
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn duplicate_factor_family_is_rejected() {
        let mut config = RuntimeConfig::default();
        config
            .factors
            .enabled_factor_families
            .push(FactorFamily::Liquidity);
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn empty_factor_families_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.factors.enabled_factor_families.clear();
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn empty_required_feature_name_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![FeatureNameRef::new("   ")];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }

    #[test]
    fn duplicate_required_feature_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.features.required_features = vec![
            FeatureNameRef::new("book.spread_bps"),
            FeatureNameRef::new("book.spread_bps"),
        ];
        let report = validate_runtime_config(&config);
        assert!(report.has_errors());
    }
}
