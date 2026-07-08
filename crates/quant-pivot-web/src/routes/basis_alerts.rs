//! Basis-cross-check exceedance alert feed (11.2.2 remediation R6).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/basis-alerts` | `materialization:read` | Paginated exceedance feed |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{BasisAlertListQuery, BasisAlertView, Paginated},
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Basis-alert feed routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/research/basis-alerts",
        Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
        list,
    )]
}

/// `GET /api/research/basis-alerts` — paginated basis-exceedance feed, newest first.
///
/// Filterable by `market_id` and `[from, to)` over `as_of`, so the linkage
/// detail page can cross-link "alerts for this market".
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<BasisAlertListQuery>,
) -> Result<WebResponse<Paginated<BasisAlertView>>, WebError> {
    let page = state
        .basis_alerts
        .page(query.into_inner())
        .await?
        .map(BasisAlertView::from);
    Ok(WebResponse::ok(page))
}
