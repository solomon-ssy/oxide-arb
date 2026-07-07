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
        LinkageResolveSummaryView, MarketLinkageDetailView, MarketLinkageListQuery,
        MarketLinkageSummaryView, OverrideLinkageRequest, Paginated, ResolveLinkagesRequest,
    },
    enums::rbac::{Operation, ResourceType},
    types::MarketId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, RequestId, ValidatedJson},
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

/// `POST /api/research/market-linkages/resolve` — trigger offline re-resolution.
pub async fn resolve(
    state: web::Data<AppState>,
    _acting_role: ActingRole,
    _request_id: RequestId,
    body: ValidatedJson<ResolveLinkagesRequest>,
) -> Result<WebResponse<LinkageResolveSummaryView>, WebError> {
    let summary = state
        .linkage_governance
        .resolve_changed_markets(&body.into_inner().market_ids)
        .await?;
    Ok(WebResponse::ok(summary))
}

/// `POST /api/research/market-linkages/{market_id}/override` — audited override.
pub async fn r#override(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
    _acting_role: ActingRole,
    _request_id: RequestId,
    body: ValidatedJson<OverrideLinkageRequest>,
) -> Result<WebResponse<MarketLinkageDetailView>, WebError> {
    let row = state
        .linkage_governance
        .apply_override(&market_id.into_inner(), body.into_inner())
        .await?;
    Ok(WebResponse::ok(MarketLinkageDetailView::from(row)))
}
