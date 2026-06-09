//! Operation-log read endpoint (track-two forensic queries).
//!
//! The operation log is written asynchronously by the
//! [`operation_audit`](crate::middleware) middleware; this is its read side:
//! a paginated, filterable view of the append-only activity trail, gated by
//! `OperationLog:Read`. Rows are returned as their persistence projection — the
//! log carries no credentials, only redacted detail summaries.

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{OperationLogInfo, OperationLogQuery, OperationLogView, Paginated},
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Operation-log read routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/operation-logs",
        Rule::ResourceOp(ResourceType::OperationLog, Operation::Read),
        list,
    )]
}

/// `GET /api/operation-logs` — paginated, filtered operation-log query
/// (most recent first).
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<OperationLogQuery>,
) -> Result<WebResponse<Paginated<OperationLogView>>, WebError> {
    let result = state
        .operation_logs
        .page(query.into_inner().normalized())
        .await?;
    Ok(WebResponse::ok(project_page(result)))
}

/// Project a paginated [`OperationLogInfo`] page into its outbound view.
fn project_page(page: Paginated<OperationLogInfo>) -> Paginated<OperationLogView> {
    Paginated {
        items: page.items.into_iter().map(OperationLogView::from).collect(),
        total: page.total,
        page: page.page,
        size: page.size,
        has_next: page.has_next,
    }
}
