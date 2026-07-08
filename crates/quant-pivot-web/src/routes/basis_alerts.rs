//! Basis-cross-check exceedance alert feed (11.2.2 remediation R6).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/basis-alerts` | `materialization:read` | Paginated exceedance feed |
//! | POST | `/research/basis-alerts/{alert_id}/acknowledge` | `materialization:create` | Audited triage acknowledgement |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{AcknowledgeBasisAlertRequest, BasisAlertListQuery, BasisAlertView, Paginated},
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::BasisAlertId,
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

/// Basis-alert feed routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/basis-alerts",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/research/basis-alerts/{alert_id}/acknowledge",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            acknowledge,
        ),
    ]
}

/// `GET /api/research/basis-alerts` — paginated basis-exceedance feed, newest first.
///
/// Filterable by `market_id`, `[from, to)` over `as_of`, and `open_only`
/// (the review-queue default view), so the linkage detail page can
/// cross-link "alerts for this market".
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

/// `POST /api/research/basis-alerts/{alert_id}/acknowledge` — audited triage.
///
/// Records who acknowledged the alert and when on the ledger row itself
/// (idempotent — re-acknowledging is a no-op), and the operator's reason on
/// the operation log, mirroring the linkage `resolve`/`override` audit
/// pattern (R6 review-queue closed loop).
pub async fn acknowledge(
    state: web::Data<AppState>,
    alert_id: web::Path<BasisAlertId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<AcknowledgeBasisAlertRequest>,
) -> Result<WebResponse<BasisAlertView>, WebError> {
    let alert_id = alert_id.into_inner();
    let request = body.into_inner();
    let row = state
        .basis_alerts
        .acknowledge(&alert_id, actor.claims.username.clone())
        .await?;
    op_ctx.set_action(OperationCategory::Other, "basis_alert.acknowledge");
    op_ctx.set_resource(ResourceType::Materialization, alert_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "alert_id": alert_id.to_string(),
        "market_id": row.market_id.to_string(),
        "reason": request.reason,
        "acting_role": acting_role.0,
        "actor": actor.claims.username,
        "request_id": request_id.0,
    }));
    Ok(WebResponse::ok(BasisAlertView::from(row)))
}
