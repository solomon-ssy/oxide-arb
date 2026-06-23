//! Runtime-config v3 semantic validation.
//!
//! This module validates only the quant-pivot v3 document. Legacy Endgame
//! Deleted pre-quant configuration paths are not accepted in Phase 1 clean-break
//! runtime configuration.

use super::{DecimalString, RuntimeConfig};
use quant_pivot_error::config_validation::{ConfigValidationError, ConfigValidationReport};
use rust_decimal::Decimal;

/// Mode-agnostic runtime-config v3 invariants.
#[must_use]
pub fn validate_runtime_config(config: &RuntimeConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_selection(config, &mut report);
    validate_data_quality(config, &mut report);
    validate_features(config, &mut report);
    validate_factors(config, &mut report);
    validate_model(config, &mut report);
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
}

fn validate_features(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    if config.features.feature_schema_version <= 0 {
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
}

fn validate_factors(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "factors.min_factor_confidence",
        &config.factors.min_factor_confidence,
        report,
    );
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
        if schedule.enabled && schedule.interval_secs == 0 {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "reports.schedules.interval_secs",
                detail: "enabled schedules must have a positive interval".to_owned(),
            });
        }
    }
}

fn validate_portfolio(config: &RuntimeConfig, report: &mut ConfigValidationReport) {
    decimal(
        "portfolio.total_budget_usd",
        &config.portfolio.total_budget_usd,
        report,
    );
    decimal(
        "portfolio.max_single_recommendation_usd",
        &config.portfolio.max_single_recommendation_usd,
        report,
    );
    decimal(
        "portfolio.max_market_exposure_usd",
        &config.portfolio.max_market_exposure_usd,
        report,
    );
    decimal(
        "portfolio.liquidity_usage_cap_pct",
        &config.portfolio.liquidity_usage_cap_pct,
        report,
    );
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
        enums::quant::QuantRuntimeMode,
        runtime_config::{DecimalString, RUNTIME_CONFIG_SCHEMA_VERSION},
    };

    #[test]
    fn default_runtime_config_is_valid() {
        let report = validate_runtime_config(&RuntimeConfig::default());
        assert!(!report.has_errors());
    }

    #[test]
    fn invalid_decimal_is_rejected() {
        let mut config = RuntimeConfig::default();
        config.portfolio.total_budget_usd = DecimalString::new("not-a-decimal");
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
    fn schema_version_remains_v3() {
        assert_eq!(
            RuntimeConfig::default().schema_version,
            RUNTIME_CONFIG_SCHEMA_VERSION
        );
    }
}
