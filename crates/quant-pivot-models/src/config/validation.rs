//! Deploy-config validation: detects infeasible values and credential-policy
//! violations that would cause silent failure at runtime.

use super::{DeployConfig, WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS};
use crate::{
    constants::POLYGON_CHAIN_ID,
    enums::quant::{ExecutionWalletKind, QuantRuntimeMode},
    types::IcaoStation,
};
use quant_pivot_error::config_validation::{
    ConfigValidationError, ConfigValidationReport, ConfigWarning,
};
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
    if deploy.polymarket.order_post_timeout_ms == 0 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.order_post_timeout_ms",
            detail: "must be > 0 so an ambiguous order POST cannot hang indefinitely".into(),
        });
    }
    if deploy.polymarket.clob_market_info_refresh_secs < 60 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "polymarket.clob_market_info_refresh_secs",
            detail: "must be at least 60 seconds".to_owned(),
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
    let migration_credentials = &deploy.db.clickhouse.migration;
    if migration_credentials.user.is_some() != migration_credentials.password.is_some() {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse.migration",
            detail: "user and password must either both be configured or both be omitted".into(),
        });
    }
    if migration_credentials.auto_apply_online
        && migration_credentials.require_dedicated_identity
        && migration_credentials.user.is_none()
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse.migration",
            detail: "auto_apply_online with require_dedicated_identity requires dedicated user and password"
                .into(),
        });
    }
    if migration_credentials
        .user
        .as_deref()
        .is_some_and(|user| user.trim().is_empty())
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse.migration.user",
            detail: "must not be empty when a dedicated migration identity is configured".into(),
        });
    }
    if migration_credentials
        .password
        .as_deref()
        .is_some_and(str::is_empty)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "db.clickhouse.migration.password",
            detail: "must not be empty when a dedicated migration identity is configured".into(),
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
    validate_market_data(deploy, &mut report);
    validate_domain_sources(deploy, &mut report);

    report
}

fn validate_domain_sources(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    let sources = &deploy.domain_sources;
    if sources.binance.enabled
        && (sources.binance.request_timeout_ms == 0
            || sources.binance.websocket_rotation_secs == 0
            || sources.binance.batch_size == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.binance",
            detail: "request_timeout_ms, websocket_rotation_secs, and batch_size must be > 0"
                .into(),
        });
    }
    if sources.chainlink_data_streams.enabled {
        let credentials_present = sources.chainlink_data_streams.api_key.is_some()
            && sources.chainlink_data_streams.api_secret.is_some();
        if !credentials_present || sources.chainlink_data_streams.feeds.is_empty() {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.chainlink_data_streams",
                detail: "enabled source requires API credentials and at least one frozen V3 feed"
                    .into(),
            });
        }
        if sources.chainlink_data_streams.max_clock_skew_ms == 0
            || sources.chainlink_data_streams.rest_page_limit == 0
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.chainlink_data_streams",
                detail: "max_clock_skew_ms and rest_page_limit must be > 0".into(),
            });
        }
    }
    if sources.aviation_weather.enabled
        && (sources.aviation_weather.poll_secs == 0
            || sources.aviation_weather.request_timeout_ms == 0
            || sources.aviation_weather.day_close_grace_secs
                != WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.aviation_weather",
            detail: format!(
                "poll_secs and request_timeout_ms must be > 0; day_close_grace_secs must equal governed methodology value {WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS}"
            ),
        });
    }
    if sources.ghcnh.enabled
        && (sources.ghcnh.request_timeout_ms == 0
            || sources.ghcnh.refresh_secs == 0
            || sources.ghcnh.calibration_years == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.ghcnh",
            detail: "request_timeout_ms, refresh_secs, and calibration_years must be > 0".into(),
        });
    }
    if sources.gefs.enabled
        && (sources.gefs.request_timeout_ms == 0
            || sources.gefs.poll_secs == 0
            || sources.gefs.publication_lag_secs == 0
            || !(3..=240).contains(&sources.gefs.max_lead_hours)
            || !sources.gefs.max_lead_hours.is_multiple_of(3)
            || sources.gefs.max_concurrency == 0)
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "domain_sources.gefs",
            detail: "timeouts/cadences/concurrency must be > 0 and max_lead_hours must be a 3-hour step in 3..=240".into(),
        });
    }
    for (station, profile) in &sources.weather_stations {
        if IcaoStation::parse(station).is_err()
            || profile.timezone.parse::<chrono_tz::Tz>().is_err()
            || profile.latitude < dec!(-90)
            || profile.latitude > dec!(90)
            || profile.longitude < dec!(-180)
            || profile.longitude > dec!(180)
            || profile.ghcnh_station_id.len() != 11
            || !profile
                .ghcnh_station_id
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            report.errors.push(ConfigValidationError::InvalidValue {
                field: "domain_sources.weather_stations",
                detail: format!(
                    "station `{station}` must have a valid ICAO id, IANA timezone, coordinates, and GHCNh id"
                ),
            });
        }
    }
}

fn validate_market_data(deploy: &DeployConfig, report: &mut ConfigValidationReport) {
    if deploy.market_data.gamma.page_size == 0
        || deploy.market_data.gamma.full_sync_interval_secs == 0
        || deploy.market_data.gamma.catalog_visibility_guard_secs == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma",
            detail:
                "page_size, full_sync_interval_secs, and catalog_visibility_guard_secs must be > 0"
                    .into(),
        });
    }
    if deploy.market_data.gamma.page_size > 500 {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma.page_size",
            detail: "must be <= 500 (Gamma /events/keyset limit)".into(),
        });
    }
    let catalog_visibility_guard_ms = deploy
        .market_data
        .gamma
        .catalog_visibility_guard_secs
        .saturating_mul(1_000);
    if deploy.db.postgres.lock_timeout_ms > 0
        && catalog_visibility_guard_ms >= deploy.db.postgres.lock_timeout_ms
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.gamma.catalog_visibility_guard_secs",
            detail: "must be lower than db.postgres.lock_timeout_ms".into(),
        });
    }
    let trade_tape = &deploy.market_data.trade_tape_on_chain;
    if trade_tape.max_blocks_per_tick == 0
        || trade_tape.max_blocks_per_request == 0
        || trade_tape.batch_size == 0
        || trade_tape.reconciliation_match_window_ms == 0
        || trade_tape.reconciliation_terminal_age_secs == 0
        || trade_tape.reconciliation_max_rows == 0
    {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.trade_tape_on_chain",
            detail: "block, batch, match-window, terminal-age, and row limits must be > 0".into(),
        });
    }
    if trade_tape.reconciliation_lookback_secs <= trade_tape.reconciliation_terminal_age_secs {
        report.errors.push(ConfigValidationError::InvalidValue {
            field: "market_data.trade_tape_on_chain.reconciliation_lookback_secs",
            detail: "must exceed reconciliation_terminal_age_secs".into(),
        });
    }
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
    fn zero_order_post_timeout_is_fatal() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.order_post_timeout_ms = 0;
        assert!(validate_deploy_common(&deploy).has_errors());
    }

    #[test]
    fn zero_trade_tape_block_range_is_fatal() {
        let mut deploy = DeployConfig::default();
        deploy
            .market_data
            .trade_tape_on_chain
            .max_blocks_per_request = 0;
        assert!(validate_deploy_common(&deploy).has_errors());
    }

    #[test]
    fn catalog_visibility_guard_must_fit_postgres_lock_timeout() {
        let mut deploy = DeployConfig::default();
        deploy.market_data.gamma.catalog_visibility_guard_secs = 5;
        deploy.db.postgres.lock_timeout_ms = 5_000;
        assert!(validate_deploy_common(&deploy).has_errors());
    }

    #[test]
    fn production_schema_auto_apply_requires_dedicated_identity_when_governed() {
        let mut deploy = DeployConfig::default();
        deploy.db.clickhouse.migration.require_dedicated_identity = true;
        assert!(validate_deploy_common(&deploy).has_errors());

        deploy.db.clickhouse.migration.user = Some("quant_pivot_migrator".into());
        deploy.db.clickhouse.migration.password = Some("test-only-secret".into());
        let report = validate_deploy_common(&deploy);
        assert!(!report.has_errors(), "errors: {:?}", report.errors);
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
