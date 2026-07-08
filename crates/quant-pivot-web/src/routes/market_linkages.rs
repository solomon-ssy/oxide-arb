//! Market-linkage governance endpoints (Phase 11.2.2).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/market-linkages` | `materialization:read` | Paginated linkage ledger |
//! | GET | `/research/market-linkages/{market_id}` | `materialization:read` | Latest linkage for a market |
//! | POST | `/research/market-linkages/resolve` | `materialization:create` | Trigger offline re-resolution |
//! | POST | `/research/market-linkages/{market_id}/override` | `materialization:create` | Audited operator override |

use actix_web::{http::Method, web};
use chrono::Utc;
use quant_pivot_models::{
    domain::{
        LinkageResolveSummaryView, MarketLinkageDetailView, MarketLinkageHistoryEntryView,
        MarketLinkageListQuery, MarketLinkageSummaryView, OverrideLinkageRequest, Paginated,
        ResolveLinkagesRequest,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::MarketId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Market-linkage governance routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/market-linkages",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/research/market-linkages/resolve",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            resolve,
        ),
        spec(
            Method::GET,
            "/research/market-linkages/{market_id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get,
        ),
        spec(
            Method::GET,
            "/research/market-linkages/{market_id}/history",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            history,
        ),
        spec(
            Method::POST,
            "/research/market-linkages/{market_id}/override",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            r#override,
        ),
    ]
}

/// `GET /api/research/market-linkages` — paginated linkage ledger catalog.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<MarketLinkageListQuery>,
) -> Result<WebResponse<Paginated<MarketLinkageSummaryView>>, WebError> {
    let page = state
        .market_linkages
        .page(query.into_inner())
        .await?
        .map(MarketLinkageSummaryView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/market-linkages/{market_id}` — latest PIT-valid linkage.
pub async fn get(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
) -> Result<WebResponse<MarketLinkageDetailView>, WebError> {
    let market_id = market_id.into_inner();
    let info = state
        .market_linkages
        .valid_at(&market_id, Utc::now())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("market linkage not found: {market_id}")))?;
    Ok(WebResponse::ok(MarketLinkageDetailView::from(info)))
}

/// `GET /api/research/market-linkages/{market_id}/history` — the full ledger.
///
/// History for one market, oldest first: every resolve pass and operator
/// override that ever produced a row, the audit trail the detail drawer
/// renders (R8 UI/UX closed loop).
pub async fn history(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
) -> Result<WebResponse<Vec<MarketLinkageHistoryEntryView>>, WebError> {
    let market_id = market_id.into_inner();
    let rows = state
        .market_linkages
        .ledger_for_markets(&[market_id], Utc::now())
        .await?;
    Ok(WebResponse::ok(
        rows.into_iter()
            .map(MarketLinkageHistoryEntryView::from)
            .collect::<Vec<_>>(),
    ))
}

/// `POST /api/research/market-linkages/resolve` — trigger offline re-resolution.
pub async fn resolve(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ResolveLinkagesRequest>,
) -> Result<WebResponse<LinkageResolveSummaryView>, WebError> {
    let request = body.into_inner();
    let summary = state
        .linkage_governance
        .resolve_changed_markets(&request.market_ids)
        .await?;
    op_ctx.set_action(OperationCategory::Other, "market_linkage.resolve");
    op_ctx.set_resource(ResourceType::Materialization, "market-linkages");
    op_ctx.set_detail(serde_json::json!({
        "market_ids": request.market_ids,
        "reason": request.reason,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "examined": summary.examined,
        "appended": summary.appended,
        "resolved": summary.resolved,
        "unresolved": summary.unresolved,
    }));
    Ok(WebResponse::ok(summary))
}

/// `POST /api/research/market-linkages/{market_id}/override` — audited override.
pub async fn r#override(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<OverrideLinkageRequest>,
) -> Result<WebResponse<MarketLinkageDetailView>, WebError> {
    let market_id = market_id.into_inner();
    let request = body.into_inner();
    let reason = request.reason.clone();
    let row = state
        .linkage_governance
        .apply_override(&market_id, request, actor.claims.username.clone())
        .await?;
    op_ctx.set_action(OperationCategory::Other, "market_linkage.override");
    op_ctx.set_resource(ResourceType::Materialization, market_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "market_id": market_id.to_string(),
        "linkage_id": row.linkage_id.to_string(),
        "reason": reason,
        "acting_role": acting_role.0,
        "actor": actor.claims.username,
        "request_id": request_id.0,
    }));
    Ok(WebResponse::ok(MarketLinkageDetailView::from(row)))
}
