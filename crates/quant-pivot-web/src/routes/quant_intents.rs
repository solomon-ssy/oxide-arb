//! Order-intent HTTP endpoints (Phase 05.2).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/intents` | `order_intent:read` | Paginated intent list |
//! | POST | `/quant/intents` | `order_intent:create` (governed) | Create from a recommendation |
//! | GET  | `/quant/intents/{id}` | `order_intent:read` | One intent |
//! | POST | `/quant/intents/{id}/approve` | `order_intent:approve` (governed) | Approve a pending intent |
//! | POST | `/quant/intents/{id}/reject` | `order_intent:reject` (governed) | Reject a pending intent |
//! | POST | `/quant/intents/{id}/cancel` | `order_intent:cancel` (governed) | Cancel a not-yet-submitted intent |
//! | POST | `/quant/intents/{id}/submit` | `order_intent:submit` (governed) | Submit an approved intent to the venue |
//!
//! Submit is the real-money path (Phase 05.4): it claims the intent, runs the
//! 20-check admission engine, and on `allow` signs + posts the order to the CLOB,
//! settling capital + position. Admission deny / non-submittable → 409; transient
//! defer → 503 (intent stays submittable). An unconfirmed venue response settles
//! as `ambiguous` (capital held, reconciled) and still returns `200`.
//!
//! Create is mode-gated: `report_only` is rejected (409), `semi_auto` yields a
//! `PendingApproval` intent, `auto_execution` yields `ApprovedByPolicy`. Approve
//! re-checks every invalidation condition (fail-closed) and may only narrow the
//! order. All mutations reserve / release capital atomically with the intent
//! transition and are recorded in the operation log.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        ApproveIntentCommand, CancelIntentCommand, CreateIntentCommand, ExecutionOrderView,
        OrderIntentListQuery, OrderIntentView, Paginated, RejectIntentCommand,
    },
    domain::{
        ApproveIntentRequest, CancelIntentRequest, CreateIntentRequest, RejectIntentRequest,
        SubmitIntentRequest,
    },
    enums::{
        execution::VenueOrderStatus,
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::OrderIntentId,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Order-intent routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/intents",
            Rule::ResourceOp(ResourceType::OrderIntent, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/quant/intents",
            Rule::ActingRoleGoverned(ResourceType::OrderIntent, Operation::Create),
            create,
        ),
        spec(
            Method::GET,
            "/quant/intents/{id}",
            Rule::ResourceOp(ResourceType::OrderIntent, Operation::Read),
            get,
        ),
        spec(
            Method::POST,
            "/quant/intents/{id}/approve",
            Rule::ActingRoleGoverned(ResourceType::OrderIntent, Operation::Approve),
            approve,
        ),
        spec(
            Method::POST,
            "/quant/intents/{id}/reject",
            Rule::ActingRoleGoverned(ResourceType::OrderIntent, Operation::Reject),
            reject,
        ),
        spec(
            Method::POST,
            "/quant/intents/{id}/cancel",
            Rule::ActingRoleGoverned(ResourceType::OrderIntent, Operation::Cancel),
            cancel,
        ),
        spec(
            Method::POST,
            "/quant/intents/{id}/submit",
            Rule::ActingRoleGoverned(ResourceType::OrderIntent, Operation::Submit),
            submit,
        ),
    ]
}

/// `GET /api/quant/intents` — paginated, filtered intent list.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<OrderIntentListQuery>,
) -> Result<WebResponse<Paginated<OrderIntentView>>, WebError> {
    let page = state
        .order_intents
        .list(query.into_inner().normalized())
        .await?;
    Ok(WebResponse::ok(page.map(OrderIntentView::from)))
}

/// `GET /api/quant/intents/{id}` — one intent.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<OrderIntentId>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let info = state
        .order_intents
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {id}")))?;
    Ok(WebResponse::ok(OrderIntentView::from(info)))
}

/// `POST /api/quant/intents` — create an intent from a recommendation.
pub async fn create(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor_uuid(&actor)?;
    let request = body.into_inner();
    let intent = state
        .order_intents
        .create(CreateIntentCommand {
            recommendation_id: request.recommendation_id.clone(),
            operator_id,
            acting_role: acting_role.0.clone(),
            reason: request.reason.clone(),
        })
        .await?;
    let after_hash = canonical_state_hash(&intent)?;
    op_ctx.set_action(OperationCategory::Governance, "quant.intent.create");
    op_ctx.set_resource(
        ResourceType::OrderIntent,
        intent.order_intent_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "order_intent_id": intent.order_intent_id.to_string(),
        "recommendation_id": request.recommendation_id.to_string(),
        "status": intent.status.as_str(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/approve` — approve a pending intent.
pub async fn approve(
    state: web::Data<AppState>,
    id: web::Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ApproveIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor_uuid(&actor)?;
    let request = body.into_inner();
    let intent_id = id.into_inner();
    let before = state
        .order_intents
        .find(&intent_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {intent_id}")))?;
    let intent = state
        .order_intents
        .approve(ApproveIntentCommand {
            order_intent_id: intent_id,
            operator_id,
            acting_role: acting_role.0.clone(),
            reason: request.reason.clone(),
            override_shares: request.override_shares,
            override_limit_price: request.override_limit_price,
            max_allowed_usd: request.max_allowed_usd,
            override_note: request.override_note.clone(),
        })
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&intent)?;
    op_ctx.set_action(OperationCategory::Governance, "quant.intent.approve");
    op_ctx.set_resource(
        ResourceType::OrderIntent,
        intent.order_intent_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "order_intent_id": intent.order_intent_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
        "override_note": request.override_note,
    }));
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/reject` — reject a pending intent.
pub async fn reject(
    state: web::Data<AppState>,
    id: web::Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RejectIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor_uuid(&actor)?;
    let request = body.into_inner();
    let intent_id = id.into_inner();
    let before = state
        .order_intents
        .find(&intent_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {intent_id}")))?;
    let intent = state
        .order_intents
        .reject(RejectIntentCommand {
            order_intent_id: intent_id,
            operator_id,
            acting_role: acting_role.0.clone(),
            reason: request.reason.clone(),
        })
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&intent)?;
    op_ctx.set_action(OperationCategory::Governance, "quant.intent.reject");
    op_ctx.set_resource(
        ResourceType::OrderIntent,
        intent.order_intent_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "order_intent_id": intent.order_intent_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/cancel` — cancel a not-yet-submitted intent.
pub async fn cancel(
    state: web::Data<AppState>,
    id: web::Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CancelIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor_uuid(&actor)?;
    let request = body.into_inner();
    let intent_id = id.into_inner();
    let before = state
        .order_intents
        .find(&intent_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {intent_id}")))?;
    let intent = state
        .order_intents
        .cancel(CancelIntentCommand {
            order_intent_id: intent_id,
            operator_id,
            acting_role: acting_role.0.clone(),
            reason: request.reason.clone(),
        })
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&intent)?;
    op_ctx.set_action(OperationCategory::Governance, "quant.intent.cancel");
    op_ctx.set_resource(
        ResourceType::OrderIntent,
        intent.order_intent_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "order_intent_id": intent.order_intent_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/submit` — submit an approved intent to the venue.
pub async fn submit(
    state: web::Data<AppState>,
    id: web::Path<OrderIntentId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SubmitIntentRequest>,
) -> Result<WebResponse<ExecutionOrderView>, WebError> {
    let intent_id = id.into_inner();
    let request = body.into_inner();
    let before = state
        .order_intents
        .find(&intent_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {intent_id}")))?;
    let order = state
        .execution_submit
        .submit_if_admitted(&intent_id)
        .await?;
    let after = state.order_intents.find(&intent_id).await?.ok_or_else(|| {
        WebError::NotFound(format!("order intent not found after submit: {intent_id}"))
    })?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&after)?;
    op_ctx.set_action(OperationCategory::Governance, "quant.intent.submit");
    op_ctx.set_resource(ResourceType::OrderIntent, intent_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "order_intent_id": intent_id.to_string(),
        "execution_order_id": order.execution_order_id.to_string(),
        "state": order.state.as_str(),
        "venue_status": order.venue_status.map(VenueOrderStatus::as_str),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(ExecutionOrderView::from(order)))
}

/// Parse the authenticated actor's stable user id into a UUID for `approved_by`.
fn actor_uuid(actor: &AuthedActor) -> Result<Uuid, WebError> {
    Uuid::parse_str(&actor.claims.sub)
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<String, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map(|hash| hash.as_str().to_owned())
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
