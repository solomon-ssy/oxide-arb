//! Reconciliation HTTP endpoints (Phase 05.5 closeout).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        ExecutionOrderView, Paginated, ReconciliationInfo, ReconciliationListQuery,
        ReconciliationView, ResolveReconciliationCommand, ResolveReconciliationRequest,
        ResolveReconciliationResponse,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::ReconciliationId,
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
            "/quant/reconciliations",
            Rule::ResourceOp(ResourceType::Reconciliation, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/quant/reconciliations/{id}",
            Rule::ResourceOp(ResourceType::Reconciliation, Operation::Read),
            get,
        ),
        spec(
            Method::POST,
            "/quant/reconciliations/{id}/resolve",
            Rule::ActingRoleGoverned(ResourceType::Reconciliation, Operation::Resolve),
            resolve,
        ),
    ]
}

async fn list(
    state: web::Data<AppState>,
    query: web::Query<ReconciliationListQuery>,
) -> Result<WebResponse<Paginated<ReconciliationView>>, WebError> {
    let page = state
        .execution_read
        .list_reconciliations(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(ReconciliationView::from)))
}

async fn get(
    state: web::Data<AppState>,
    id: web::Path<ReconciliationId>,
) -> Result<WebResponse<ReconciliationView>, WebError> {
    let info = state
        .execution_read
        .get_reconciliation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("reconciliation not found: {id}")))?;
    Ok(WebResponse::ok(ReconciliationView::from(info)))
}

async fn resolve(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    id: web::Path<ReconciliationId>,
    body: ValidatedJson<ResolveReconciliationRequest>,
) -> Result<WebResponse<ResolveReconciliationResponse>, WebError> {
    let reconciliation_id = id.into_inner();
    let before = state
        .execution_read
        .get_reconciliation(&reconciliation_id)
        .await?;
    let before_hash = before
        .as_ref()
        .map(canonical_reconciliation_hash)
        .transpose()?;

    let body = body.into_inner();
    op_ctx.set_action(
        OperationCategory::Governance,
        "quant.reconciliation.resolve",
    );
    op_ctx.set_resource(ResourceType::Reconciliation, reconciliation_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "result": body.result.as_str(),
        "reason": body.reason,
    }));

    let reconciliation_id_for_cmd = reconciliation_id.clone();
    let outcome = state
        .reconciliation
        .resolve_operator(ResolveReconciliationCommand {
            reconciliation_id: reconciliation_id_for_cmd,
            result: body.result,
            filled_shares: body.filled_shares,
            avg_price: body.avg_price,
            operator: actor.claims.username.clone(),
            reason: body.reason,
        })
        .await?;

    let after_hash = canonical_reconciliation_hash(
        &state
            .execution_read
            .get_reconciliation(&reconciliation_id)
            .await?
            .ok_or_else(|| WebError::NotFound("reconciliation missing after resolve".into()))?,
    )?;
    op_ctx.set_state_hashes(before_hash, Some(after_hash));

    Ok(WebResponse::ok(ResolveReconciliationResponse {
        execution_order: ExecutionOrderView::from(outcome.execution_order),
        recovery: outcome.recovery,
    }))
}

fn canonical_reconciliation_hash(info: &ReconciliationInfo) -> Result<String, WebError> {
    CanonicalDigest::content_hash_json(&ReconciliationView::from(info.clone()))
        .map(|hash| hash.as_str().to_owned())
        .map_err(|error| {
            WebError::Internal(format!("canonical reconciliation hash failed: {error}"))
        })
}
