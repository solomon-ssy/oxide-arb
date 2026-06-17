//! Deploy-config validation: detects infeasible values and credential-policy
//! violations that would cause silent failure at runtime.
//!
//! Validation is split into **mode-agnostic** and **mode-aware** halves. The
//! mode-agnostic half runs during [`DeployConfig::load`]; errors there abort
//! startup regardless of which mode the operator will run. The mode-aware half
//! runs once the persisted operational [`ExecutionMode`] has been restored.
//!
//! Runtime-config validation lives in [`crate::runtime_config::validation`].
//!
//! # Mode → severity matrix
//!
//! | Polymarket creds   | `DryRun` | `Paper` | `Live` |
//! |--------------------|----------|---------|--------|
//! | all populated      | pass     | pass    | pass   |
//! | all empty          | pass     | warn    | fatal  |
//! | partial (1-3 set)  | warn     | fatal   | fatal  |

use super::DeployConfig;
use crate::{constants::POLYGON_CHAIN_ID, enums::common::ExecutionMode};
use oxide_arb_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Mode-agnostic deploy validation: structural and platform invariants that
/// must hold regardless of execution mode.
#[must_use]
pub fn validate_deploy_common(deploy: &DeployConfig) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();

    if deploy.polymarket.chain_id != POLYGON_CHAIN_ID {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.chain_id",
            detail: format!("must be Polygon chain id {POLYGON_CHAIN_ID}"),
        });
    }

    if deploy.polymarket.fees.exponent <= Decimal::ZERO {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.exponent",
            detail: "must be > 0".into(),
        });
    }
    for (category, rate) in &deploy.polymarket.fees.category_rates {
        if *rate < Decimal::ZERO || *rate > dec!(1) {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "polymarket.fees.category_rates",
                detail: format!("{category} rate {rate} out of range [0, 1]"),
            });
        }
    }
    if deploy.polymarket.fees.unknown_category_rate < Decimal::ZERO
        || deploy.polymarket.fees.unknown_category_rate > dec!(1)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.fees.unknown_category_rate",
            detail: "must be in [0, 1]".into(),
        });
    }

    if deploy.execution.book_apply.shard_count == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.book_apply.shard_count",
            detail: "must be >= 1".into(),
        });
    }
    if deploy.execution.book_apply.channel_capacity < 16 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "execution.book_apply.channel_capacity",
            detail: "must be >= 16".into(),
        });
    }
    if deploy.settlement.lifecycle.channel_capacity < 16 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "settlement.lifecycle.channel_capacity",
            detail: "must be >= 16".into(),
        });
    }

    if deploy.db.postgres.max_connections == 0
        || deploy.db.postgres.min_connections > deploy.db.postgres.max_connections
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.postgres",
            detail: "max_connections must be > 0 and >= min_connections".into(),
        });
    }
    if deploy.db.clickhouse.batch_size == 0
        || deploy.db.clickhouse.flush_interval_secs == 0
        || deploy.db.clickhouse.max_concurrent_inserts == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse",
            detail: "batch_size, flush_interval_secs, max_concurrent_inserts must be > 0".into(),
        });
    }

    validate_cache_redis(deploy, &mut report);

    if deploy
        .market_data
        .websocket
        .max_subscriptions_per_connection
        == 0
        || deploy.market_data.websocket.engine_max_subscription_tokens == 0
        || deploy.market_data.websocket.engine_endgame_window_hours == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.websocket",
            detail: "max_subscriptions_per_connection, engine_max_subscription_tokens, engine_endgame_window_hours must be > 0".into(),
        });
    }
    if deploy.market_data.gamma.page_size == 0
        || deploy.market_data.gamma.full_sync_interval_secs == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma",
            detail: "page_size and full_sync_interval_secs must be > 0".into(),
        });
    }
    if deploy.market_data.gamma.page_size > 500 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma.page_size",
            detail: "must be <= 500 (Gamma /events/keyset limit)".into(),
        });
    }

    report
}

fn validate_cache_redis(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let redis = &deploy.cache.redis;
    if redis.host.is_empty() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.host",
            detail: "must not be empty".into(),
        });
    }
    if redis.port == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.port",
            detail: "must be > 0".into(),
        });
    }
    if redis.pool_size == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis.pool_size",
            detail: "must be > 0".into(),
        });
    }
    if redis.try_connection_url().is_err() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "cache.redis",
            detail: "host/port/user/password produce an invalid connection URL".into(),
        });
    }
}

/// Mode-aware deploy validation: enforces credential and JWT policies based on
/// the execution mode that will actually run.
#[must_use]
pub fn validate_deploy_for_mode(
    deploy: &DeployConfig,
    mode: ExecutionMode,
) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_credentials_mode(deploy, mode, &mut report);
    validate_web_mode(deploy, mode, &mut report);
    report
}

fn validate_credentials_mode(
    deploy: &DeployConfig,
    mode: ExecutionMode,
    report: &mut ConfigValidationReport,
) {
    if deploy.keys.private_key_present() {
        return;
    }

    let mode_label = mode.to_string();
    match mode {
        ExecutionMode::DryRun => {}
        ExecutionMode::Paper => {
            report.warnings.push(ConfigWarning::NoCredentialsPaper);
        }
        ExecutionMode::Live => {
            report
                .errors
                .push(ConfigValidationError::MissingCredentials {
                    mode: mode_label,
                    missing: vec!["private_key"],
                });
        }
    }
}

/// Mode-aware web validation: a strong JWT secret is mandatory in `Live`.
///
/// In `Live` an empty/placeholder secret is fatal (fail-closed); in
/// `DryRun`/`Paper` it is only a warning so local development stays frictionless.
fn validate_web_mode(
    deploy: &DeployConfig,
    mode: ExecutionMode,
    report: &mut ConfigValidationReport,
) {
    if !deploy.web.jwt_secret_is_weak() {
        return;
    }
    match mode {
        ExecutionMode::Live => report.errors.push(ConfigValidationError::InvalidValue {
            field: "web.jwt.secret",
            detail: "Live mode requires a strong, non-placeholder JWT secret \
                     (set OXIDE_ARB__WEB__JWT__SECRET)"
                .to_owned(),
        }),
        ExecutionMode::DryRun | ExecutionMode::Paper => {
            report.warnings.push(ConfigWarning::WeakJwtSecret);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_validation_passes_for_defaults() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_common(&deploy);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn wrong_chain_id_is_fatal() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.chain_id = 1;
        assert!(validate_deploy_common(&deploy).has_errors());
    }

    #[test]
    fn live_mode_requires_all_credentials() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_for_mode(&deploy, ExecutionMode::Live);
        assert!(report.has_errors());
    }

    #[test]
    fn dry_run_permits_empty_credentials() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_for_mode(&deploy, ExecutionMode::DryRun);
        assert!(!report.has_errors());
    }

    #[test]
    fn live_mode_rejects_weak_jwt_secret() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        let report = validate_deploy_for_mode(&deploy, ExecutionMode::Live);
        assert!(
            report.has_errors(),
            "empty jwt secret must be fatal in Live"
        );
    }

    #[test]
    fn live_mode_accepts_strong_jwt_secret_and_credentials() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.web.jwt.secret = "a-strong-production-secret".to_owned();
        let report = validate_deploy_for_mode(&deploy, ExecutionMode::Live);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn dry_run_only_warns_on_weak_jwt_secret() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_for_mode(&deploy, ExecutionMode::DryRun);
        assert!(!report.has_errors());
        assert!(!report.warnings.is_empty(), "weak secret should warn");
    }
}
