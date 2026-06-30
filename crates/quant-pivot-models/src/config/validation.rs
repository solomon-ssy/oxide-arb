//! Deploy-config validation: detects infeasible values and credential-policy
//! violations that would cause silent failure at runtime.

use super::DeployConfig;
use crate::{
    constants::POLYGON_CHAIN_ID,
    enums::quant::{ExecutionWalletKind, QuantRuntimeMode},
};
use quant_pivot_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Mode-agnostic deploy validation: structural and platform invariants that
/// must hold regardless of quant runtime mode.
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

    if deploy.quant.workers.report_expire_sweep_secs == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "quant.workers.report_expire_sweep_secs",
            detail: "must be > 0".into(),
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
        || deploy
            .market_data
            .websocket
            .engine_subscription_window_hours
            == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.websocket",
            detail: "max_subscriptions_per_connection, engine_max_subscription_tokens, engine_subscription_window_hours must be > 0".into(),
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

/// Quant-mode-aware deploy validation: enforces credential and JWT policies.
#[must_use]
pub fn validate_deploy_for_quant_mode(
    deploy: &DeployConfig,
    mode: QuantRuntimeMode,
) -> ConfigValidationReport {
    let mut report = ConfigValidationReport::default();
    validate_credentials_quant_mode(deploy, mode, &mut report);
    validate_web_quant_mode(deploy, mode, &mut report);
    report
}

/// Credential policy (all modes): `report_only` is **not** dry-run — report
/// sizing is built on the real venue account, so the private key (CLOB
/// collateral / L2 read credential) and the funder (Data API position reads) are
/// required in every mode. Missing either fails closed. The private key is used
/// only for reads here; signing/submission gating stays mode-aware elsewhere.
/// Phase05.10 EOA auto-redeem additionally checks signer/funder equality during
/// CTF worker assembly, where the signer address is available.
fn validate_credentials_quant_mode(
    deploy: &DeployConfig,
    mode: QuantRuntimeMode,
    report: &mut ConfigValidationReport,
) {
    let mut missing = Vec::new();
    if !deploy.keys.private_key_present() {
        missing.push("private_key");
    }
    let funder_present = deploy
        .quant
        .account
        .funder
        .as_deref()
        .is_some_and(|funder| !funder.trim().is_empty());
    if !funder_present {
        missing.push("quant.account.funder");
    }
    if !missing.is_empty() {
        report
            .errors
            .push(ConfigValidationError::MissingCredentials {
                mode: mode.to_string(),
                missing,
            });
    }

    // Proxy / Gnosis Safe topologies move money via the gasless relayer, so the
    // relayer API credentials are mandatory once order submission is allowed
    // (SemiAuto / AutoExecution). ReportOnly never redeems, so it is exempt.
    if mode.allows_order_submission()
        && matches!(
            deploy.quant.account.wallet_kind,
            ExecutionWalletKind::Proxy | ExecutionWalletKind::GnosisSafe
        )
    {
        let mut relayer_missing = Vec::new();
        if deploy.polymarket.relayer.api_key().is_none() {
            relayer_missing.push("polymarket.relayer.api_key");
        }
        if deploy.polymarket.relayer.api_key_address().is_none() {
            relayer_missing.push("polymarket.relayer.api_key_address");
        }
        if !relayer_missing.is_empty() {
            report
                .errors
                .push(ConfigValidationError::MissingCredentials {
                    mode: mode.to_string(),
                    missing: relayer_missing,
                });
        }
    }
}

fn validate_web_quant_mode(
    deploy: &DeployConfig,
    mode: QuantRuntimeMode,
    report: &mut ConfigValidationReport,
) {
    if !deploy.web.jwt_secret_is_weak() {
        return;
    }
    match mode {
        QuantRuntimeMode::AutoExecution => {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "web.jwt.secret",
                detail: "AutoExecution mode requires a strong, non-placeholder JWT secret \
                         (set QUANT_PIVOT__WEB__JWT__SECRET)"
                    .to_owned(),
            });
        }
        QuantRuntimeMode::ReportOnly | QuantRuntimeMode::SemiAuto => {
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
    fn auto_execution_requires_credentials() {
        let deploy = DeployConfig::default();
        let report = validate_deploy_for_quant_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(report.has_errors());
    }

    #[test]
    fn report_only_requires_credentials() {
        // report_only is not dry-run: it needs a private key + funder to read
        // the real venue account, so missing credentials fail closed.
        let deploy = DeployConfig::default();
        let report = validate_deploy_for_quant_mode(&deploy, QuantRuntimeMode::ReportOnly);
        assert!(report.has_errors());
    }

    #[test]
    fn auto_execution_rejects_weak_jwt_secret() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        let report = validate_deploy_for_quant_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(
            report.has_errors(),
            "empty jwt secret must be fatal in AutoExecution"
        );
    }

    #[test]
    fn auto_execution_accepts_strong_jwt_secret_and_credentials() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        deploy.web.jwt.secret = "a-strong-production-secret".to_owned();
        let report = validate_deploy_for_quant_mode(&deploy, QuantRuntimeMode::AutoExecution);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
    }

    #[test]
    fn report_only_with_credentials_only_warns_on_weak_jwt_secret() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        let report = validate_deploy_for_quant_mode(&deploy, QuantRuntimeMode::ReportOnly);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
        assert!(!report.warnings.is_empty(), "weak secret should warn");
    }
}
