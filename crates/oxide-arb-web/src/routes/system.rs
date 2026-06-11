//! System status + control endpoints.
//!
//! Reads (`System:Read`) project the live aggregate status / health views.
//! Controls are money-critical: `halt` / `resume` engage the risk halt + the
//! execution kill switch, and the execution-mode hot-swap is **governed**
//! (`ActingRoleGoverned(System, SwitchMode)`) — entering `Live` is the highest
//! risk operator action, so it requires the strict acting-role authorization and
//! a mandatory reason, and is recorded on the operation log.

use actix_web::{http::Method, web};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        CoreEvent, HaltRequest, HealthReport, ModeTransitionReport, ResumeRequest,
        SwitchModeRequest, SystemStatus,
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

/// System status + control routes.
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
            Method::POST,
            "/system/halt",
            Rule::ResourceOp(ResourceType::System, Operation::Halt),
            halt,
        ),
        spec(
            Method::POST,
            "/system/resume",
            Rule::ResourceOp(ResourceType::System, Operation::Resume),
            resume,
        ),
        spec(
            Method::POST,
            "/system/mode",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_mode,
        ),
        spec(
            Method::GET,
            "/system/deploy-config",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            deploy_config,
        ),
    ]
}

/// `GET /api/system/deploy-config` — read-only deploy configuration, masked.
///
/// DB passwords and the JWT secret are masked; key material is never included
/// (only presence flags). Deploy values change only with a restart — runtime
/// tunables live under `/api/runtime-config`.
pub async fn deploy_config(
    state: web::Data<AppState>,
) -> Result<WebResponse<serde_json::Value>, WebError> {
    Ok(WebResponse::ok(masked_deploy_view(&state.deploy)))
}

/// Build the masked deploy-config projection.
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
        "execution": {
            "book_apply": {
                "shard_count": deploy.execution.book_apply.shard_count,
                "channel_capacity": deploy.execution.book_apply.channel_capacity,
            },
        },
        "settlement": {
            "lifecycle": {
                "channel_capacity": deploy.settlement.lifecycle.channel_capacity,
            },
        },
    })
}

/// Mask a secret: empty stays empty, anything else becomes `***`.
const fn mask_secret(value: &str) -> &'static str {
    if value.is_empty() { "" } else { "***" }
}

/// Mask a URL that embeds credentials in its authority (`user:pass@host`).
///
/// URLs without userinfo pass through unchanged so operators can verify the
/// endpoint; anything with an `@` in the authority is masked whole rather
/// than risking a partial credential leak.
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
            // The URL may embed credentials in its authority — masked if so.
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

/// `GET /api/system/status` — aggregate live system status.
pub async fn status(state: web::Data<AppState>) -> Result<WebResponse<SystemStatus>, WebError> {
    Ok(WebResponse::ok(state.control.system_status().await))
}

/// `GET /api/system/health` — subsystem health report.
pub async fn health(state: web::Data<AppState>) -> Result<WebResponse<HealthReport>, WebError> {
    Ok(WebResponse::ok(state.control.health().await))
}

/// `POST /api/system/halt` — halt trading (risk halt + execution kill switch).
pub async fn halt(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<HaltRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.halt");
    op_ctx.set_detail(serde_json::json!({ "reason": body.reason }));
    state.control.halt(body.reason).await;
    publish_status(&state).await;
    Ok(WebResponse::ok(()))
}

/// `POST /api/system/resume` — resume trading after operator acknowledgement.
pub async fn resume(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<ResumeRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.resume");
    state.control.resume(&body.operator_ack).await?;
    publish_status(&state).await;
    Ok(WebResponse::ok(()))
}

/// `POST /api/system/mode` — governed runtime execution-mode hot-swap.
///
/// `ActingRoleGoverned`, so authz has already resolved an [`ActingRole`]; the
/// operator's user id is used as the acknowledgement recorded on the risk audit.
pub async fn switch_mode(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchModeRequest>,
) -> Result<WebResponse<ModeTransitionReport>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.switch_mode");
    op_ctx.set_detail(serde_json::json!({
        "target_mode": body.mode.as_str(),
        "reason": body.reason,
    }));
    let report = state
        .control
        .switch_execution_mode(body.mode, &actor.claims.sub)
        .await?;
    publish_status(&state).await;
    Ok(WebResponse::ok(report))
}

/// Publish a fresh `SystemStatusChanged` event after a control action so live
/// WebSocket clients observe the new state immediately.
async fn publish_status(state: &AppState) {
    let status = state.control.system_status().await;
    state.events.publish(CoreEvent::SystemStatusChanged(status));
}

#[cfg(test)]
mod tests {
    use super::mask_url_credentials;

    #[test]
    fn url_without_userinfo_passes_through() {
        assert_eq!(
            mask_url_credentials("http://ch.internal:8123"),
            "http://ch.internal:8123"
        );
        // An `@` outside the authority (path/query) is not a credential.
        assert_eq!(
            mask_url_credentials("http://ch.internal:8123/db?owner=a@b"),
            "http://ch.internal:8123/db?owner=a@b"
        );
    }

    #[test]
    fn url_with_embedded_credentials_is_masked_whole() {
        assert_eq!(
            mask_url_credentials("http://user:pass@ch.internal:8123"),
            "***"
        );
        assert_eq!(mask_url_credentials("user:pass@ch.internal"), "***");
    }
}
