//! Operation-log read endpoint (track-two forensic queries).
//!
//! The operation log is written asynchronously by the
//! [`operation_audit`](crate::middleware) middleware; this is its read side:
//! a paginated, filterable view of the append-only activity trail, gated by
//! `OperationLog:Read`. Rows are returned as their persistence projection — the
//! log carries no credentials, only redacted detail summaries.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{OperationLogQuery, OperationLogView},
        governance::OperationLogInfo,
        pagination::Paginated,
    },
    enums::rbac::{Operation, ResourceType},
    types::OperationLogId,
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
    vec![
        spec(
            Method::GET,
            "/operation-logs",
            Rule::ResourceOp(ResourceType::OperationLog, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/operation-logs/{id}",
            Rule::ResourceOp(ResourceType::OperationLog, Operation::Read),
            get,
        ),
    ]
}

/// `GET /api/operation-logs` — paginated, filtered operation-log query
/// (most recent first).
pub async fn list(
    state: Data<AppState>,
    query: Query<OperationLogQuery>,
) -> Result<WebResponse<Paginated<OperationLogView>>, WebError> {
    let result = state.operation_logs.page(query.into_inner()).await?;
    Ok(WebResponse::ok(project_page(result)))
}

/// `GET /api/operation-logs/{id}` — one immutable redacted audit row.
pub async fn get(
    state: Data<AppState>,
    id: Path<OperationLogId>,
) -> Result<WebResponse<OperationLogView>, WebError> {
    let info = state
        .operation_logs
        .find_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("operation log not found: {id}")))?;
    Ok(WebResponse::ok(OperationLogView::from(info)))
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
