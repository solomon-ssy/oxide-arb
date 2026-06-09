//! Governance control-plane endpoints (control-factor lifecycle + publications).
//!
//! Reads (`ControlFactor:Read` / `Audit:Read`) project persistence `*Info` types
//! directly. Mutations are **governed**: each is `ActingRoleGoverned`, so authz
//! has already resolved an [`ActingRole`] into request extensions; the handler
//! builds an [`AuditActor`] envelope (`actor` = user id, `actor_role` = acting
//! role, `request_id`, `reason`) and delegates to the [`ControlFactorRegistry`],
//! which validates governance invariants and writes the audit hash chain
//! transactionally. The returned [`AuditedOutcome`] carries the appended event
//! id, which is stamped onto the operation log via [`OperationContext`] for a
//! hard cross-walk between the two audit tracks.
//!
//! Publications are atomic *sets* (a publication supersedes the active one for
//! its mode), so shadow/publish/emergency are collection endpoints under
//! `/control-factors/publications`, not per-factor actions; reject is per factor.

use actix_web::{http::Method, web};
use chrono::{Duration, Utc};
use oxide_arb_control::governance::PublicationRequest;
use oxide_arb_error::control::AuditChainError;
use oxide_arb_models::{
    domain::{
        AuditChainQuery, AuditChainResponse, ControlFactorListQuery, EmergencyPublishRequest,
        PublicationListQuery, PublishPublicationRequest, RejectFactorRequest,
        RollbackPublicationRequest, ShadowDecisionsQuery, ShadowDecisionsResponse,
        control_factor::{
            AuditActor, AuditChain, ControlFactorPublicationInfo, ControlFactorValueInfo,
            PublishPublicationOutcome,
        },
    },
    enums::{
        control_factor::FactorStatus,
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::{ControlFactorId, FactorPublicationId},
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

/// Bounded TTL for an emergency Published activation; it auto-expires so an
/// override can never become long-lived.
const EMERGENCY_PUBLICATION_TTL_HOURS: i64 = 1;
/// Default / maximum publications returned by the catalog list.
const DEFAULT_PUBLICATION_LIMIT: u64 = 50;
const MAX_PUBLICATION_LIMIT: u64 = 200;
/// Default / maximum audit-chain slice size.
const DEFAULT_AUDIT_LIMIT: u64 = 100;
const MAX_AUDIT_LIMIT: u64 = 1000;
/// Default shadow-decision lookback window and page size.
const DEFAULT_SHADOW_WINDOW_HOURS: i64 = 24;
const DEFAULT_SHADOW_LIMIT: u64 = 100;
const MAX_SHADOW_LIMIT: u64 = 1000;

/// Control-factor governance routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/control-factors",
            Rule::ResourceOp(ResourceType::ControlFactor, Operation::Read),
            list_factors,
        ),
        spec(
            Method::GET,
            "/control-factors/audit",
            Rule::ResourceOp(ResourceType::Audit, Operation::Read),
            audit_chain,
        ),
        spec(
            Method::GET,
            "/control-factors/publications",
            Rule::ResourceOp(ResourceType::ControlFactor, Operation::Read),
            list_publications,
        ),
        spec(
            Method::POST,
            "/control-factors/publications/shadow",
            Rule::ActingRoleGoverned(ResourceType::ControlFactor, Operation::Shadow),
            shadow_publication,
        ),
        spec(
            Method::POST,
            "/control-factors/publications/publish",
            Rule::ActingRoleGoverned(ResourceType::ControlFactor, Operation::Publish),
            publish_publication,
        ),
        spec(
            Method::POST,
            "/control-factors/publications/emergency",
            Rule::ActingRoleGoverned(ResourceType::ControlFactor, Operation::Emergency),
            emergency_publish,
        ),
        spec(
            Method::GET,
            "/control-factors/publications/{id}",
            Rule::ResourceOp(ResourceType::ControlFactor, Operation::Read),
            get_publication,
        ),
        spec(
            Method::POST,
            "/control-factors/publications/{id}/rollback",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Rollback),
            rollback_publication,
        ),
        spec(
            Method::GET,
            "/control-factors/publications/{id}/shadow-decisions",
            Rule::ResourceOp(ResourceType::ControlFactor, Operation::Read),
            shadow_decisions,
        ),
        spec(
            Method::GET,
            "/control-factors/{id}",
            Rule::ResourceOp(ResourceType::ControlFactor, Operation::Read),
            get_factor,
        ),
        spec(
            Method::POST,
            "/control-factors/{id}/reject",
            Rule::ActingRoleGoverned(ResourceType::ControlFactor, Operation::Reject),
            reject_factor,
        ),
    ]
}

/// `GET /api/control-factors` — list factors by status (defaults to the
/// `Candidate` review queue), optionally filtered by type.
pub async fn list_factors(
    state: web::Data<AppState>,
    query: web::Query<ControlFactorListQuery>,
) -> Result<WebResponse<Vec<ControlFactorValueInfo>>, WebError> {
    let query = query.into_inner();
    let status = query.status.unwrap_or(FactorStatus::Candidate);
    Ok(WebResponse::ok(
        state
            .control_factors
            .list_factors_by_status(status, query.factor_type)
            .await?,
    ))
}

/// `GET /api/control-factors/{id}` — fetch one factor.
pub async fn get_factor(
    state: web::Data<AppState>,
    id: web::Path<ControlFactorId>,
) -> Result<WebResponse<ControlFactorValueInfo>, WebError> {
    let factor = state
        .control_factors
        .load_factor(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("control factor not found: {}", *id)))?;
    Ok(WebResponse::ok(factor))
}

/// `POST /api/control-factors/{id}/reject` — reject a candidate factor.
pub async fn reject_factor(
    state: web::Data<AppState>,
    id: web::Path<ControlFactorId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RejectFactorRequest>,
) -> Result<WebResponse<ControlFactorValueInfo>, WebError> {
    let body = body.into_inner();
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason);
    let outcome = state
        .registry
        .reject_factor(envelope, &id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!(
                "control factor not found or not rejectable: {}",
                *id
            ))
        })?;

    op_ctx.set_action(OperationCategory::Governance, "control_factor.reject");
    op_ctx.set_resource(ResourceType::ControlFactor, id.to_string());
    op_ctx.link_governance(outcome.audit_event_id);
    Ok(WebResponse::ok(outcome.value))
}

/// `GET /api/control-factors/publications` — list publications for a mode.
pub async fn list_publications(
    state: web::Data<AppState>,
    query: web::Query<PublicationListQuery>,
) -> Result<WebResponse<Vec<ControlFactorPublicationInfo>>, WebError> {
    let query = query.into_inner();
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PUBLICATION_LIMIT)
        .min(MAX_PUBLICATION_LIMIT);
    Ok(WebResponse::ok(
        state
            .control_factors
            .list_publications(query.mode, query.status, limit)
            .await?,
    ))
}

/// `GET /api/control-factors/publications/{id}` — fetch one publication.
pub async fn get_publication(
    state: web::Data<AppState>,
    id: web::Path<FactorPublicationId>,
) -> Result<WebResponse<ControlFactorPublicationInfo>, WebError> {
    let publication = state
        .control_factors
        .load_publication(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("publication not found: {}", *id)))?;
    Ok(WebResponse::ok(publication))
}

/// `POST /api/control-factors/publications/shadow` — stage a Shadow publication.
pub async fn shadow_publication(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<PublishPublicationRequest>,
) -> Result<WebResponse<ControlFactorPublicationInfo>, WebError> {
    let body = body.into_inner();
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason.clone());
    let request = publication_request(&body);
    let outcome = state.registry.promote_to_shadow(envelope, request).await?;
    Ok(WebResponse::ok(record_publication_outcome(
        &op_ctx,
        "control_factor.shadow",
        outcome,
    )))
}

/// `POST /api/control-factors/publications/publish` — stage a Published publication.
pub async fn publish_publication(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<PublishPublicationRequest>,
) -> Result<WebResponse<ControlFactorPublicationInfo>, WebError> {
    let body = body.into_inner();
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason.clone());
    let request = publication_request(&body);
    let outcome = state.registry.publish(envelope, request).await?;
    Ok(WebResponse::ok(record_publication_outcome(
        &op_ctx,
        "control_factor.publish",
        outcome,
    )))
}

/// `POST /api/control-factors/publications/emergency` — short-TTL Published
/// activation with forced risk-expansion approval (the explicit override path).
pub async fn emergency_publish(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<EmergencyPublishRequest>,
) -> Result<WebResponse<ControlFactorPublicationInfo>, WebError> {
    let body = body.into_inner();
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason.clone());
    let request = PublicationRequest {
        factor_ids: body.factor_ids,
        idempotency_key: body.idempotency_key,
        effective_from: None,
        expires_at: Utc::now() + Duration::hours(EMERGENCY_PUBLICATION_TTL_HOURS),
        // Emergency is itself the operator-acknowledged risk-expansion override.
        manual_risk_expansion_approval: true,
    };
    let outcome = state.registry.publish(envelope, request).await?;
    Ok(WebResponse::ok(record_publication_outcome(
        &op_ctx,
        "control_factor.emergency",
        outcome,
    )))
}

/// `POST /api/control-factors/publications/{id}/rollback` — roll the active
/// publication `{id}` back to a known-good target.
pub async fn rollback_publication(
    state: web::Data<AppState>,
    id: web::Path<FactorPublicationId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RollbackPublicationRequest>,
) -> Result<WebResponse<ControlFactorPublicationInfo>, WebError> {
    let body = body.into_inner();
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason);
    let outcome = state
        .registry
        .rollback_publication(envelope, &id, &body.target_publication_id)
        .await?;

    op_ctx.set_action(OperationCategory::Governance, "publication.rollback");
    op_ctx.set_resource(ResourceType::Publication, id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "target_publication_id": body.target_publication_id,
    }));
    op_ctx.link_governance(outcome.audit_event_id);
    Ok(WebResponse::ok(outcome.value))
}

/// `GET /api/control-factors/audit` — load an audit-chain slice and verify it.
///
/// Verification failure is a data-integrity finding, not a request error: it is
/// returned as `verified: false` + `broken_at` with HTTP 200 for forensics.
pub async fn audit_chain(
    state: web::Data<AppState>,
    query: web::Query<AuditChainQuery>,
) -> Result<WebResponse<AuditChainResponse>, WebError> {
    let query = query.into_inner();
    let from_sequence = query.from_sequence.unwrap_or(1).max(1);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_AUDIT_LIMIT)
        .min(MAX_AUDIT_LIMIT);
    let events = state
        .control_factors
        .load_audit_chain(from_sequence, limit)
        .await?;
    let (verified, broken_at) = match AuditChain::verify(&events) {
        Ok(()) => (true, None),
        Err(error) => (false, audit_break_sequence(&error)),
    };
    Ok(WebResponse::ok(AuditChainResponse {
        events,
        verified,
        broken_at,
    }))
}

/// `GET /api/control-factors/publications/{id}/shadow-decisions` — windowed
/// shadow-decision rollup plus the raw decisions for drill-down.
pub async fn shadow_decisions(
    state: web::Data<AppState>,
    id: web::Path<FactorPublicationId>,
    query: web::Query<ShadowDecisionsQuery>,
) -> Result<WebResponse<ShadowDecisionsResponse>, WebError> {
    let query = query.into_inner();
    let to = query.to.unwrap_or_else(Utc::now);
    let from = query
        .from
        .unwrap_or_else(|| to - Duration::hours(DEFAULT_SHADOW_WINDOW_HOURS));
    let limit = query
        .limit
        .unwrap_or(DEFAULT_SHADOW_LIMIT)
        .min(MAX_SHADOW_LIMIT);

    let aggregate = state
        .shadow_decisions
        .aggregate_shadow_decisions(&id, from, to)
        .await?;
    let decisions = state
        .shadow_decisions
        .list_shadow_decisions(&id, from, to, limit)
        .await?;
    Ok(WebResponse::ok(ShadowDecisionsResponse {
        aggregate,
        decisions,
    }))
}

/// Assemble the governance audit envelope from the request-scoped attributes.
fn governance_envelope(
    actor: &AuthedActor,
    acting_role: ActingRole,
    request_id: &RequestId,
    reason: String,
) -> AuditActor {
    AuditActor {
        actor: actor.claims.sub.clone(),
        actor_role: acting_role.0,
        request_id: request_id.0.clone(),
        reason,
    }
}

/// Translate a publish/shadow request body into the service-layer request.
fn publication_request(body: &PublishPublicationRequest) -> PublicationRequest {
    PublicationRequest {
        factor_ids: body.factor_ids.clone(),
        idempotency_key: body.idempotency_key.clone(),
        effective_from: body.effective_from,
        expires_at: body.expires_at,
        manual_risk_expansion_approval: body.manual_risk_expansion_approval,
    }
}

/// Enrich the operation log for a publication outcome and return the publication.
///
/// `Published` links the appended hash-chain event; an idempotent `AlreadyApplied`
/// replay appended no new event, so it is left unlinked.
fn record_publication_outcome(
    op_ctx: &OperationCtx,
    action: &str,
    outcome: PublishPublicationOutcome,
) -> ControlFactorPublicationInfo {
    op_ctx.set_action(OperationCategory::Governance, action);
    op_ctx.set_resource(
        ResourceType::Publication,
        outcome.publication().publication_id.to_string(),
    );
    if let Some(event_id) = outcome.audit_event_id() {
        op_ctx.link_governance(event_id.clone());
    }
    match outcome {
        PublishPublicationOutcome::Published(audited) => audited.value,
        PublishPublicationOutcome::AlreadyApplied(info) => info,
    }
}

/// The first inconsistent sequence reported by a chain-verification failure, if
/// the failure is positional (a digest failure has no sequence).
const fn audit_break_sequence(error: &AuditChainError) -> Option<i64> {
    match error {
        AuditChainError::SequenceGap { actual, .. } => Some(*actual),
        AuditChainError::BrokenLink { sequence }
        | AuditChainError::GenesisPrevNotNull { sequence }
        | AuditChainError::HashMismatch { sequence, .. } => Some(*sequence),
        AuditChainError::Digest(_) => None,
    }
}
