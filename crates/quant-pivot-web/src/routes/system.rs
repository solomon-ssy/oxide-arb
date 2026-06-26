//! System status + quant runtime mode endpoints (Phase 0).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        HealthReport, KillSwitchView, QuantModeTransitionReport, QuantModeView,
        SetKillSwitchCommand, SetKillSwitchRequest, SwitchQuantModeRequest, SystemStatus,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/system/status",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            status,
        ),
        spec(
            Method::GET,
            "/system/health",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            health,
        ),
        spec(
            Method::GET,
            "/system/quant-mode",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            quant_mode,
        ),
        spec(
            Method::POST,
            "/system/quant-mode",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_quant_mode,
        ),
        spec(
            Method::GET,
            "/system/kill-switch",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            kill_switch_status,
        ),
        spec(
            Method::POST,
            "/system/kill-switch",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::Halt),
            set_kill_switch,
        ),
        spec(
            Method::GET,
            "/system/deploy-config",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            deploy_config,
        ),
    ]
}

pub async fn deploy_config(
    state: web::Data<AppState>,
) -> Result<WebResponse<serde_json::Value>, WebError> {
    Ok(WebResponse::ok(masked_deploy_view(&state.deploy)))
}

fn masked_deploy_view(deploy: &DeployConfig) -> serde_json::Value {
    serde_json::json!({
        "polymarket": {
            "clob_base_url": deploy.polymarket.clob_base_url,
            "clob_ws_url": deploy.polymarket.clob_ws_url,
            "chain_id": deploy.polymarket.chain_id,
            "onchain": {
                "rpc_url": deploy.polymarket.onchain.rpc_url,
                "rpc_timeout_ms": deploy.polymarket.onchain.rpc_timeout_ms,
            },
            "fees": {
                "exponent": deploy.polymarket.fees.exponent,
                "unknown_category_rate": deploy.polymarket.fees.unknown_category_rate,
                "category_rates": deploy.polymarket.fees.category_rates,
            },
        },
        "market_data": {
            "websocket": {
                "reconnect_delay_ms": deploy.market_data.websocket.reconnect_delay_ms,
                "max_reconnect_delay_ms": deploy.market_data.websocket.max_reconnect_delay_ms,
                "max_subscriptions_per_connection":
                    deploy.market_data.websocket.max_subscriptions_per_connection,
            },
            "gamma": {
                "base_url": deploy.market_data.gamma.base_url,
                "full_sync_interval_secs": deploy.market_data.gamma.full_sync_interval_secs,
                "page_size": deploy.market_data.gamma.page_size,
            },
        },
        "observability": {
            "log_level": deploy.observability.log_level,
            "log_json": deploy.observability.log_json,
        },
        "db": masked_db_view(deploy),
        "cache": masked_cache_view(deploy),
        "keys": {
            "source": format!("{:?}", deploy.keys.source),
            "private_key_present": deploy.keys.private_key_present(),
        },
        "web": masked_web_view(deploy),
    })
}

const fn mask_secret(value: &str) -> &'static str {
    if value.is_empty() { "" } else { "***" }
}

fn mask_url_credentials(url: &str) -> &str {
    let authority = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = authority.split(['/', '?', '#']).next().unwrap_or(authority);
    if authority.contains('@') { "***" } else { url }
}

fn masked_db_view(deploy: &DeployConfig) -> serde_json::Value {
    serde_json::json!({
        "postgres": {
            "host": deploy.db.postgres.host,
            "port": deploy.db.postgres.port,
            "user": deploy.db.postgres.user,
            "password": mask_secret(&deploy.db.postgres.password),
            "database": deploy.db.postgres.database,
            "schema": deploy.db.postgres.schema,
            "max_connections": deploy.db.postgres.max_connections,
            "min_connections": deploy.db.postgres.min_connections,
        },
        "clickhouse": {
            "url": mask_url_credentials(&deploy.db.clickhouse.url),
            "database": deploy.db.clickhouse.database,
            "user": deploy.db.clickhouse.user,
            "password": mask_secret(&deploy.db.clickhouse.password),
            "flush_interval_secs": deploy.db.clickhouse.flush_interval_secs,
            "batch_size": deploy.db.clickhouse.batch_size,
            "max_concurrent_inserts": deploy.db.clickhouse.max_concurrent_inserts,
        },
    })
}

fn masked_cache_view(deploy: &DeployConfig) -> serde_json::Value {
    serde_json::json!({
        "redis": {
            "host": deploy.cache.redis.host,
            "port": deploy.cache.redis.port,
            "user": deploy.cache.redis.user,
            "password": mask_secret(&deploy.cache.redis.password),
            "database": deploy.cache.redis.database,
            "pool_size": deploy.cache.redis.pool_size,
            "timeout_ms": deploy.cache.redis.timeout_ms,
            "key_prefix": deploy.cache.redis.key_prefix,
        },
        "moka": { "max_capacity": deploy.cache.moka.max_capacity },
        "operation_timeout_ms": deploy.cache.operation_timeout_ms,
        "fail_open": deploy.cache.fail_open,
        "disabled": deploy.cache.disabled,
    })
}

fn masked_web_view(deploy: &DeployConfig) -> serde_json::Value {
    serde_json::json!({
        "listen_host": deploy.web.listen_host,
        "listen_port": deploy.web.listen_port,
        "cors_allowed_origins": deploy.web.cors_allowed_origins,
        "serve_static_ui": deploy.web.serve_static_ui,
        "static_ui_dir": deploy.web.static_ui_dir,
        "jwt": {
            "secret": mask_secret(&deploy.web.jwt.secret),
            "issuer": deploy.web.jwt.issuer,
            "access_ttl_secs": deploy.web.jwt.access_ttl_secs,
            "refresh_ttl_secs": deploy.web.jwt.refresh_ttl_secs,
        },
    })
}

pub async fn status(state: web::Data<AppState>) -> Result<WebResponse<SystemStatus>, WebError> {
    Ok(WebResponse::ok(state.control.system_status()))
}

pub async fn health(state: web::Data<AppState>) -> Result<WebResponse<HealthReport>, WebError> {
    Ok(WebResponse::ok(state.control.health().await))
}

pub async fn quant_mode(
    state: web::Data<AppState>,
) -> Result<WebResponse<QuantModeView>, WebError> {
    Ok(WebResponse::ok(QuantModeView {
        mode: state.control.quant_runtime_mode(),
    }))
}

pub async fn switch_quant_mode(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchQuantModeRequest>,
) -> Result<WebResponse<QuantModeTransitionReport>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.switch_quant_mode");
    op_ctx.set_detail(serde_json::json!({
        "target_mode": body.mode.as_str(),
        "reason": body.reason,
    }));
    let report = state
        .control
        .switch_quant_mode(body.mode, &actor.claims.username, &body.reason)
        .await?;
    Ok(WebResponse::ok(report))
}

pub async fn kill_switch_status(
    state: web::Data<AppState>,
) -> Result<WebResponse<KillSwitchView>, WebError> {
    Ok(WebResponse::ok(state.kill_switch.view()))
}

pub async fn set_kill_switch(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SetKillSwitchRequest>,
) -> Result<WebResponse<KillSwitchView>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.set_kill_switch");
    op_ctx.set_detail(serde_json::json!({
        "target_state": body.state.as_str(),
        "reason": body.reason,
        "ack": body.ack,
    }));
    let view = state
        .kill_switch
        .set(SetKillSwitchCommand {
            target: body.state,
            actor: actor.claims.username.clone(),
            reason: body.reason,
            ack: body.ack,
        })
        .await?;
    Ok(WebResponse::ok(view))
}
