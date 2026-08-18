//! Order-intent HTTP endpoints.
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/quant/intents` | `order_intent:read` | Paginated intent list |
//! | POST | `/quant/intents` | `order_intent:create` (governed) | Create from a recommendation |
//! | GET | `/quant/intents/{id}` | `order_intent:read` | One intent |
//! | POST | `/quant/intents/{id}/approve` | `order_intent:approve` (governed) | Approve a pending intent |
//! | POST | `/quant/intents/{id}/reject` | `order_intent:reject` (governed) | Reject a pending intent |
//! | POST | `/quant/intents/{id}/cancel` | `order_intent:cancel` (governed) | Cancel a not-yet-submitted intent |
//!
//! Create applies the live entry-authorization policy: operator approval yields
//! `PendingAuthorization`; an active policy yields `Authorized`. Approve
//! re-checks every invalidation condition (fail-closed) and may only narrow the
//! order. All mutations reserve / release capital atomically with the intent
//! transition and are recorded in the operation log.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{
        api::{
            ApproveIntentRequest, CancelIntentRequest, CreateIntentRequest,
            EntryConditionInstanceSummaryView, ExitMonitorObservationView, OrderIntentListQuery,
            OrderIntentView, RejectIntentRequest,
        },
        market::BookSnapshot,
        pagination::Paginated,
        ports::{
            ApproveIntentCommand, CancelIntentCommand, CreateIntentCommand, RejectIntentCommand,
        },
        quant::StrategyPositionLot,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, OrderIntentId, RoleCode},
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
    ]
}

/// `GET /api/quant/intents` — paginated, filtered intent list.
pub async fn list(
    state: Data<AppState>,
    query: Query<OrderIntentListQuery>,
) -> Result<WebResponse<Paginated<OrderIntentView>>, WebError> {
    let page = state.order_intents.list(query.into_inner()).await?;
    Ok(WebResponse::ok(page.map(OrderIntentView::from)))
}

/// `GET /api/quant/intents/{id}` — one intent.
pub async fn get(
    state: Data<AppState>,
    id: Path<OrderIntentId>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let info = state
        .order_intents
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("order intent not found: {id}")))?;
    let mut view = OrderIntentView::from(info);
    view.entry_condition = state
        .entry_conditions
        .find_instance(&view.condition_instance_id)
        .await?
        .map(EntryConditionInstanceSummaryView::from);
    if let Some(position) = state
        .execution_read
        .get_position_by_intent(&view.order_intent_id)
        .await?
    {
        view.exit_monitor_observation =
            Some(exit_monitor_observation(&state, &view, &position).await?);
    }
    Ok(WebResponse::ok(view))
}

pub(crate) async fn exit_monitor_observation(
    state: &AppState,
    intent: &OrderIntentView,
    position: &StrategyPositionLot,
) -> Result<ExitMonitorObservationView, WebError> {
    let now = Utc::now();
    let max_book_age_ms = state
        .quant_reports
        .find_recommendation(&intent.recommendation_id)
        .await?
        .map_or(0, |recommendation| {
            recommendation.trade_plan.entry.max_book_age_ms
        });
    let snapshot = state
        .market_data
        .book_for_token(&intent.entry_order.token_id);
    let current_executable_bid = snapshot.as_deref().and_then(BookSnapshot::best_bid);
    let book_observed_at = snapshot
        .as_deref()
        .and_then(|book| i64::try_from(book.timestamp_ms).ok())
        .and_then(DateTime::from_timestamp_millis);
    let book_age_ms = snapshot.as_deref().map(|book| {
        let now_ms = u64::try_from(now.timestamp_millis()).unwrap_or(u64::MAX);
        now_ms.saturating_sub(book.timestamp_ms)
    });
    let book_fresh = book_age_ms.is_some_and(|age| age <= max_book_age_ms);
    let effective_stop = intent
        .exit_policy
        .effective_stop(position.avg_price, intent.peak_mark_price);
    let next_scale_out = intent.exit_policy.next_scale_out(
        &intent.scale_out_state,
        position.shares,
        current_executable_bid,
        now,
    );

    Ok(ExitMonitorObservationView {
        state: intent.exit_state,
        reason: intent.exit_reason,
        current_executable_bid,
        book_observed_at,
        book_age_ms,
        book_fresh,
        peak_mark: intent.peak_mark_price,
        effective_stop,
        next_scale_out,
        cumulative_exited_shares: intent.scale_out_state.cumulative_exited_shares,
        cumulative_exit_pct: intent.scale_out_state.cumulative_exit_pct(),
        latest_reinference: intent.latest_reinference.clone(),
        last_check_at: intent.last_signal_recheck_at,
        next_check_at: intent.next_check_at,
    })
}

/// `POST /api/quant/intents` — create an intent from a recommendation.
pub async fn create(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor
        .user_id()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
    let request = body.into_inner();
    let intent = state
        .order_intents
        .create(CreateIntentCommand {
            recommendation_id: request.recommendation_id,
            operator_id,
            acting_role: RoleCode::new(acting_role.0.clone()),
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
    }))?;
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/approve` — approve a pending intent.
pub async fn approve(
    state: Data<AppState>,
    id: Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ApproveIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor
        .user_id()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
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
            acting_role: RoleCode::new(acting_role.0.clone()),
            reason: request.reason.clone(),
            override_amount: request.override_amount,
            override_price: request.override_price,
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
        "override_amount": request.override_amount,
        "override_price": request.override_price,
    }))?;
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/reject` — reject a pending intent.
pub async fn reject(
    state: Data<AppState>,
    id: Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RejectIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor
        .user_id()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
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
            acting_role: RoleCode::new(acting_role.0.clone()),
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
    }))?;
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

/// `POST /api/quant/intents/{id}/cancel` — cancel a not-yet-submitted intent.
pub async fn cancel(
    state: Data<AppState>,
    id: Path<OrderIntentId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CancelIntentRequest>,
) -> Result<WebResponse<OrderIntentView>, WebError> {
    let operator_id = actor
        .user_id()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
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
            acting_role: RoleCode::new(acting_role.0.clone()),
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
    }))?;
    Ok(WebResponse::ok(OrderIntentView::from(intent)))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
