//! Domain-source ingest cursor health endpoints (Phase 11.2.2).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/domain-sources` | `materialization:read` | All ingest cursors + lag |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::DomainSourceCursorView,
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Domain-source cursor health routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/research/domain-sources",
        Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
        list,
    )]
}

/// `GET /api/research/domain-sources` — ingest checkpoint health for every
/// `(source, instrument)` stream.
pub async fn list(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<DomainSourceCursorView>>, WebError> {
    let rows = state
        .domain_source_cursors
        .list_all()
        .await?
        .into_iter()
        .map(DomainSourceCursorView::from)
        .collect();
    Ok(WebResponse::ok(rows))
}
