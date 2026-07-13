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
//!
//! Create is mode-gated: `report_only` is rejected (409), `semi_auto` yields a
//! `PendingApproval` intent, `auto_execution` yields `ApprovedByPolicy`. Approve
//! re-checks every invalidation condition (fail-closed) and may only narrow the
//! order. All mutations reserve / release capital atomically with the intent
//! transition and are recorded in the operation log.

use actix_web::{http::Method, web};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{
        ApproveIntentCommand, CancelIntentCommand, CreateIntentCommand,
        EntryTriggerObservationView, ExitMonitorObservationView, OrderIntentListQuery,
        OrderIntentView, Paginated, PositionInfo, RejectIntentCommand,
    },
    domain::{ApproveIntentRequest, CancelIntentRequest, CreateIntentRequest, RejectIntentRequest},
    enums::{
        common::Side,
        operation_log::OperationCategory,
        quant::{OrderIntentStatus, PriceComparison},
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{EntryTrigger, OrderIntentId, Price},
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
    ]
}

/// `GET /api/quant/intents` — paginated, filtered intent list.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<OrderIntentListQuery>,
) -> Result<WebResponse<Paginated<OrderIntentView>>, WebError> {
    let page = state.order_intents.list(query.into_inner()).await?;
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
    let mut view = OrderIntentView::from(info);
    view.entry_trigger_observation = Some(entry_trigger_observation(&state, &view).await?);
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
    position: &PositionInfo,
) -> Result<ExitMonitorObservationView, WebError> {
    let now = Utc::now();
    let max_book_age_ms = state
        .quant_reports
        .find_recommendation(&intent.recommendation_id)
        .await?
        .and_then(|recommendation| {
            recommendation
                .trade_plan
                .frozen()
                .map(|(_, entry, _, _, _)| entry.max_book_age_ms)
        })
        .unwrap_or(0);
    let snapshot = state
        .market_data
        .book_for_token(&intent.entry_order.token_id);
    let current_executable_bid = snapshot
        .as_deref()
        .and_then(quant_pivot_models::domain::BookSnapshot::best_bid);
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

async fn entry_trigger_observation(
    state: &AppState,
    intent: &OrderIntentView,
) -> Result<EntryTriggerObservationView, WebError> {
    let now = Utc::now();
    let recommendation = state
        .quant_reports
        .find_recommendation(&intent.recommendation_id)
        .await?;
    let max_book_age_ms = recommendation
        .as_ref()
        .and_then(|row| {
            row.trade_plan
                .frozen()
                .map(|(_, entry, _, _, _)| entry.max_book_age_ms)
        })
        .unwrap_or(0);
    let snapshot = state
        .market_data
        .book_for_token(&intent.entry_order.token_id);
    let current_price = snapshot
        .as_deref()
        .and_then(|book| match intent.entry_order.side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        });
    let book_observed_at = snapshot
        .as_deref()
        .and_then(|book| i64::try_from(book.timestamp_ms).ok())
        .and_then(DateTime::from_timestamp_millis);
    let book_age_ms = snapshot.as_deref().map(|book| {
        let now_ms = u64::try_from(now.timestamp_millis()).unwrap_or(u64::MAX);
        now_ms.saturating_sub(book.timestamp_ms)
    });
    let book_fresh = book_age_ms.is_some_and(|age| age <= max_book_age_ms);
    let condition_satisfied =
        trigger_condition_satisfied(&intent.entry_trigger, current_price, book_fresh);
    let confirmation_remaining_secs =
        confirmation_remaining_secs(&intent.entry_trigger, intent.trigger_confirming_since, now);
    let admission_blocker = intent
        .status_reason
        .clone()
        .or_else(|| {
            (intent.status == OrderIntentStatus::PendingApproval)
                .then(|| "approval_required".to_owned())
        })
        .or_else(|| {
            recommendation
                .is_none()
                .then(|| "recommendation_unavailable".to_owned())
        })
        .or_else(|| snapshot.is_none().then(|| "book_unavailable".to_owned()))
        .or_else(|| (!book_fresh).then(|| "book_stale".to_owned()))
        .or_else(|| (!condition_satisfied).then(|| "entry_condition_not_satisfied".to_owned()));

    Ok(EntryTriggerObservationView {
        current_price,
        book_observed_at,
        book_age_ms,
        book_fresh,
        condition_satisfied,
        confirmation_remaining_secs,
        admission_blocker,
    })
}

fn trigger_condition_satisfied(
    trigger: &EntryTrigger,
    current_price: Option<Price>,
    book_fresh: bool,
) -> bool {
    if !book_fresh {
        return false;
    }
    match trigger {
        EntryTrigger::Immediate => true,
        EntryTrigger::PriceCondition {
            comparison,
            threshold,
            ..
        } => current_price.is_some_and(|price| match comparison {
            PriceComparison::AtOrAbove => price >= *threshold,
            PriceComparison::AtOrBelow => price <= *threshold,
        }),
    }
}

fn confirmation_remaining_secs(
    trigger: &EntryTrigger,
    confirming_since: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<u64> {
    let EntryTrigger::PriceCondition {
        confirmation_secs, ..
    } = trigger
    else {
        return None;
    };
    let confirming_since = confirming_since?;
    let elapsed = now
        .signed_duration_since(confirming_since)
        .num_seconds()
        .max(0);
    let elapsed = u64::try_from(elapsed).unwrap_or(u64::MAX);
    Some(confirmation_secs.saturating_sub(elapsed))
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::PriceComparison,
        types::{EntryTrigger, Price},
    };
    use rust_decimal_macros::dec;

    use super::{confirmation_remaining_secs, trigger_condition_satisfied};

    #[test]
    fn price_condition_requires_a_fresh_executable_price() {
        let trigger = EntryTrigger::PriceCondition {
            comparison: PriceComparison::AtOrAbove,
            threshold: Price::new(dec!(0.60)),
            confirmation_secs: 10,
            max_observation_gap_ms: 2_000,
        };
        assert!(trigger_condition_satisfied(
            &trigger,
            Some(Price::new(dec!(0.61))),
            true,
        ));
        assert!(!trigger_condition_satisfied(
            &trigger,
            Some(Price::new(dec!(0.61))),
            false,
        ));
    }

    #[test]
    fn confirmation_countdown_saturates_at_zero() {
        let trigger = EntryTrigger::PriceCondition {
            comparison: PriceComparison::AtOrBelow,
            threshold: Price::new(dec!(0.40)),
            confirmation_secs: 10,
            max_observation_gap_ms: 2_000,
        };
        let start = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid test instant");
        assert_eq!(
            confirmation_remaining_secs(&trigger, Some(start), start + Duration::seconds(4)),
            Some(6),
        );
        assert_eq!(
            confirmation_remaining_secs(&trigger, Some(start), start + Duration::seconds(12)),
            Some(0),
        );
    }
}
