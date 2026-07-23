//! Settlement readiness, ledger, and governed authorization control plane.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use chrono::Utc;
use quant_pivot_models::{
    domain::{
        api::settlement_redeem::{
            SettlementAuthorizationRequest, SettlementCanaryPreflightRequest,
            SettlementChainSubmissionView, SettlementGovernedActionApplyRequest,
            SettlementGovernedActionDetailView, SettlementGovernedActionListQuery,
            SettlementGovernedActionPreflightView, SettlementGovernedActionRevokeRequest,
            SettlementGovernedActionView, SettlementInventoryLotView,
            SettlementOperatorApprovalPreflightRequest, SettlementReadinessView,
            SettlementRedeemDetail, SettlementRedeemDetailView, SettlementRedeemListQuery,
            SettlementRedeemLotView, SettlementRedeemSummary, SettlementRedeemView,
        },
        pagination::Paginated,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, SettlementGovernedActionId, SettlementRedeemId, UserId},
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/settlement-readiness",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            readiness,
        ),
        spec(
            Method::GET,
            "/quant/settlement-redeems",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/quant/settlement-redeems/{id}",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            get,
        ),
        spec(
            Method::POST,
            "/quant/settlement-redeems/{id}/approve",
            Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Approve),
            approve,
        ),
        spec(
            Method::POST,
            "/quant/settlement-redeems/{id}/revoke-approval",
            Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Revoke),
            revoke_approval,
        ),
        spec(
            Method::POST,
            "/quant/settlement-operator-approvals/preflight",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            operator_approval_preflight,
        ),
        spec(
            Method::POST,
            "/quant/settlement-operator-approvals/apply",
            Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Create),
            operator_approval_apply,
        ),
        spec(
            Method::POST,
            "/quant/settlement-canaries/preflight",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            canary_preflight,
        ),
        spec(
            Method::POST,
            "/quant/settlement-canaries/apply",
            Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Create),
            canary_apply,
        ),
        spec(
            Method::GET,
            "/quant/settlement-governed-actions",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            list_governed_actions,
        ),
        spec(
            Method::GET,
            "/quant/settlement-governed-actions/{id}",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            get_governed_action,
        ),
        spec(
            Method::POST,
            "/quant/settlement-governed-actions/{id}/revoke",
            Rule::ActingRoleGoverned(ResourceType::SettlementRedeem, Operation::Revoke),
            revoke_governed_action,
        ),
    ]
}

async fn readiness(
    state: Data<AppState>,
) -> Result<WebResponse<SettlementReadinessView>, WebError> {
    Ok(WebResponse::ok(
        state.settlement_control.readiness(Utc::now()).await?,
    ))
}

async fn list(
    state: Data<AppState>,
    query: Query<SettlementRedeemListQuery>,
) -> Result<WebResponse<Paginated<SettlementRedeemView>>, WebError> {
    let page = state
        .execution_read
        .list_settlement_redeems(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(SettlementRedeemView::from)))
}

async fn get(
    state: Data<AppState>,
    id: Path<SettlementRedeemId>,
) -> Result<WebResponse<SettlementRedeemDetailView>, WebError> {
    let detail = state
        .execution_read
        .get_settlement_redeem(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("settlement redeem not found: {id}")))?;
    Ok(WebResponse::ok(detail_view(detail)))
}

async fn approve(
    state: Data<AppState>,
    id: Path<SettlementRedeemId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SettlementAuthorizationRequest>,
) -> Result<WebResponse<SettlementRedeemView>, WebError> {
    let settlement_redeem_id = id.into_inner();
    let actor_id = actor_user_id(&actor)?;
    let request = body.into_inner();
    let reason = authorization_reason(&request.reason)?;
    let before = require_detail(&state, &settlement_redeem_id).await?;
    state
        .settlement_control
        .approve_authorization(settlement_redeem_id, request.digest, actor_id, Utc::now())
        .await?;
    let after = require_detail(&state, &settlement_redeem_id).await?;
    record_authorization_audit(AuthorizationAudit {
        op_ctx: &op_ctx,
        action: "quant.settlement.authorization.approve",
        before: &before,
        after: &after,
        acting_role: &acting_role,
        request_id: &request_id,
        reason,
        digest: request.digest,
    })?;
    Ok(WebResponse::ok(detail_view(after).redeem))
}

async fn revoke_approval(
    state: Data<AppState>,
    id: Path<SettlementRedeemId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SettlementAuthorizationRequest>,
) -> Result<WebResponse<SettlementRedeemView>, WebError> {
    let settlement_redeem_id = id.into_inner();
    let actor_id = actor_user_id(&actor)?;
    let request = body.into_inner();
    let reason = authorization_reason(&request.reason)?;
    let before = require_detail(&state, &settlement_redeem_id).await?;
    state
        .settlement_control
        .revoke_authorization(settlement_redeem_id, request.digest, actor_id, Utc::now())
        .await?;
    let after = require_detail(&state, &settlement_redeem_id).await?;
    record_authorization_audit(AuthorizationAudit {
        op_ctx: &op_ctx,
        action: "quant.settlement.authorization.revoke",
        before: &before,
        after: &after,
        acting_role: &acting_role,
        request_id: &request_id,
        reason,
        digest: request.digest,
    })?;
    Ok(WebResponse::ok(detail_view(after).redeem))
}

async fn operator_approval_preflight(
    state: Data<AppState>,
    body: ValidatedJson<SettlementOperatorApprovalPreflightRequest>,
) -> Result<WebResponse<SettlementGovernedActionPreflightView>, WebError> {
    Ok(WebResponse::ok(
        state
            .settlement_control
            .operator_approval_preflight(body.into_inner(), Utc::now())
            .await?,
    ))
}

async fn canary_preflight(
    state: Data<AppState>,
    body: ValidatedJson<SettlementCanaryPreflightRequest>,
) -> Result<WebResponse<SettlementGovernedActionPreflightView>, WebError> {
    Ok(WebResponse::ok(
        state
            .settlement_control
            .canary_preflight(body.into_inner(), Utc::now())
            .await?,
    ))
}

async fn operator_approval_apply(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SettlementGovernedActionApplyRequest>,
) -> Result<WebResponse<SettlementGovernedActionView>, WebError> {
    apply_governed_action(
        &state,
        actor,
        acting_role,
        request_id,
        op_ctx,
        body.into_inner(),
        "quant.settlement.operator_approval.apply",
    )
    .await
}

async fn canary_apply(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SettlementGovernedActionApplyRequest>,
) -> Result<WebResponse<SettlementGovernedActionView>, WebError> {
    apply_governed_action(
        &state,
        actor,
        acting_role,
        request_id,
        op_ctx,
        body.into_inner(),
        "quant.settlement.canary.apply",
    )
    .await
}

async fn list_governed_actions(
    state: Data<AppState>,
    query: Query<SettlementGovernedActionListQuery>,
) -> Result<WebResponse<Paginated<SettlementGovernedActionView>>, WebError> {
    Ok(WebResponse::ok(
        state
            .settlement_control
            .list_governed_actions(query.into_inner())
            .await?,
    ))
}

async fn get_governed_action(
    state: Data<AppState>,
    id: Path<SettlementGovernedActionId>,
) -> Result<WebResponse<SettlementGovernedActionDetailView>, WebError> {
    let action_id = id.into_inner();
    let detail = state
        .settlement_control
        .get_governed_action(&action_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("settlement governed action not found: {action_id}"))
        })?;
    Ok(WebResponse::ok(detail))
}

async fn revoke_governed_action(
    state: Data<AppState>,
    id: Path<SettlementGovernedActionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SettlementGovernedActionRevokeRequest>,
) -> Result<WebResponse<SettlementGovernedActionView>, WebError> {
    let action_id = id.into_inner();
    let actor_id = actor_user_id(&actor)?;
    let before = state
        .settlement_control
        .get_governed_action(&action_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("settlement governed action not found: {action_id}"))
        })?;
    let after = state
        .settlement_control
        .revoke_governed_action(action_id, body.into_inner(), actor_id, Utc::now())
        .await?;
    record_governed_action_audit(
        &op_ctx,
        "quant.settlement.governed_action.revoke",
        Some(&before.action),
        &after,
        &acting_role,
        &request_id,
    )?;
    Ok(WebResponse::ok(after))
}

async fn apply_governed_action(
    state: &AppState,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    request: SettlementGovernedActionApplyRequest,
    action: &'static str,
) -> Result<WebResponse<SettlementGovernedActionView>, WebError> {
    let actor_id = actor_user_id(&actor)?;
    let applied = state
        .settlement_control
        .apply_governed_action(request, actor_id, Utc::now())
        .await?;
    record_governed_action_audit(&op_ctx, action, None, &applied, &acting_role, &request_id)?;
    Ok(WebResponse::ok(applied))
}

async fn require_detail(
    state: &AppState,
    id: &SettlementRedeemId,
) -> Result<SettlementRedeemDetail, WebError> {
    state
        .execution_read
        .get_settlement_redeem(id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("settlement redeem not found: {id}")))
}

fn detail_view(detail: SettlementRedeemDetail) -> SettlementRedeemDetailView {
    let inventory_lot_count = i64::try_from(detail.inventory_lots.len()).unwrap_or(i64::MAX);
    let inventory_lots = detail
        .inventory_lots
        .into_iter()
        .map(SettlementInventoryLotView::from)
        .collect();
    let redeemed_lots = detail
        .redeemed_lots
        .into_iter()
        .map(SettlementRedeemLotView::from)
        .collect();
    let submissions = detail
        .submissions
        .into_iter()
        .map(SettlementChainSubmissionView::from)
        .collect();
    SettlementRedeemDetailView {
        redeem: SettlementRedeemView::from(SettlementRedeemSummary {
            redeem: detail.redeem,
            inventory_lot_count,
        }),
        inventory_lots,
        redeemed_lots,
        submissions,
    }
}

#[derive(Clone, Copy)]
struct AuthorizationAudit<'a> {
    op_ctx: &'a OperationCtx,
    action: &'static str,
    before: &'a SettlementRedeemDetail,
    after: &'a SettlementRedeemDetail,
    acting_role: &'a ActingRole,
    request_id: &'a RequestId,
    reason: &'a str,
    digest: ContentHash,
}

fn record_authorization_audit(audit: AuthorizationAudit<'_>) -> Result<(), WebError> {
    audit
        .op_ctx
        .set_action(OperationCategory::Governance, audit.action);
    audit.op_ctx.set_resource(
        ResourceType::SettlementRedeem,
        audit.after.redeem.settlement_redeem_id.to_string(),
    );
    audit.op_ctx.set_state_hashes(
        Some(canonical_state_hash(&audit.before.redeem)?),
        Some(canonical_state_hash(&audit.after.redeem)?),
    );
    audit.op_ctx.set_detail(serde_json::json!({
        "settlement_redeem_id": audit.after.redeem.settlement_redeem_id.to_string(),
        "authorization_digest": audit.digest.to_string(),
        "acting_role": audit.acting_role.0,
        "request_id": audit.request_id.0,
        "reason": audit.reason,
    }))?;
    Ok(())
}

fn record_governed_action_audit(
    op_ctx: &OperationCtx,
    action: &'static str,
    before: Option<&SettlementGovernedActionView>,
    after: &SettlementGovernedActionView,
    acting_role: &ActingRole,
    request_id: &RequestId,
) -> Result<(), WebError> {
    op_ctx.set_action(OperationCategory::Governance, action);
    op_ctx.set_resource(
        ResourceType::SettlementRedeem,
        after.settlement_governed_action_id.to_string(),
    );
    op_ctx.set_state_hashes(
        before.map(canonical_state_hash).transpose()?,
        Some(canonical_state_hash(after)?),
    );
    op_ctx.set_detail(serde_json::json!({
        "settlement_governed_action_id": after.settlement_governed_action_id.to_string(),
        "kind": after.kind,
        "scope_digest": after.scope_digest.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(())
}

fn actor_user_id(actor: &AuthedActor) -> Result<UserId, WebError> {
    actor
        .claims
        .sub
        .parse()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))
}

fn authorization_reason(reason: &str) -> Result<&str, WebError> {
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(WebError::BadRequest(
            "settlement authorization reason must not be blank".to_owned(),
        ));
    }
    Ok(reason)
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
