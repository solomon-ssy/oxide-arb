//! Operation-audit middleware — track two of the dual-track audit model.
//!
//! Sits **outermost** in the pipeline (before `request_id`, authn, authz) so
//! that after the inner pipeline returns it observes the *final* HTTP status
//! (including 401/403 produced by the auth middleware) and every attribute the
//! inner layers injected: the correlation id, the authenticated [`Claims`], the
//! resolved [`ActingRole`], and the handler's [`OperationContext`] enrichment.
//!
//! It records only mutating methods (`POST`/`PUT`/`DELETE`/`PATCH`) — reads and
//! probes are skipped — and enqueues the row through the non-blocking
//! [`OperationLogBuffer`](crate::audit::OperationLogBuffer). It **never** reads
//! the request body: the only free-form detail comes from the handler via
//! [`OperationContext::set_detail`], which the handler must redact.
//!
//! Best-effort by construction: a missing app state, a full buffer, or any other
//! audit-side problem is logged and swallowed — the business response is always
//! returned unchanged.

use std::{rc::Rc, str::FromStr, time::Instant};

use actix_web::{
    Error, HttpMessage,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::{Method, StatusCode},
    middleware::Next,
    web::Data,
};
use quant_pivot_models::{
    domain::governance::NewOperationLog,
    enums::operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
    types::{OperationAction, OperationDetailDocument, OperationLogId, UserId},
};
use sea_orm::entity::prelude::IpNetwork;

use crate::{
    audit::OperationContext,
    extractors::{ActingRole, RequestId},
    jwt::Claims,
    state::AppState,
};

/// Capture every mutating request / auth event into the operation log.
pub async fn operation_audit<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    // Share an enrichment context with the handler (single-threaded per worker).
    req.extensions_mut()
        .insert(Rc::new(OperationContext::default()));

    // Snapshot envelope inputs available before dispatch. `peer_addr` is the
    // direct socket peer; `X-Forwarded-For` is intentionally NOT trusted here.
    let started = Instant::now();
    let method = req.method().clone();
    let http_path = req.path().to_owned();
    let client_ip = req.peer_addr().map(|addr| IpNetwork::from(addr.ip()));
    let user_agent = header_value(&req, "user-agent");
    // The buffer is cheap to clone; capturing it avoids borrowing the response.
    let buffer = req
        .app_data::<Data<AppState>>()
        .map(|state| state.operation_log.clone());

    let res = next.call(req).await?;

    if let Some(http_method) = operation_http_method(&method)
        && let Some(buffer) = buffer
    {
        let request = res.request();
        let extensions = request.extensions();
        let claims = extensions.get::<Claims>();
        let matched = request.match_pattern();
        let enrichment = extensions
            .get::<Rc<OperationContext>>()
            .map(|ctx| ctx.snapshot())
            .unwrap_or_default();

        let status = res.status();
        let outcome = enrichment.outcome.unwrap_or_else(|| derive_outcome(status));
        let category = enrichment
            .category
            .unwrap_or_else(|| fallback_category(matched.as_deref().unwrap_or(&http_path)));
        let action = enrichment.action.unwrap_or_else(|| {
            OperationAction::new(fallback_action(
                &method,
                matched.as_deref().unwrap_or(&http_path),
            ))
        });

        let log = NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: extensions
                .get::<RequestId>()
                .map(|id| id.0.clone())
                .unwrap_or_default()
                .into(),
            // Handler enrichment wins (login attributes the actor before any
            // `Claims` exist); otherwise fall back to the authenticated identity.
            actor_user_id: enrichment
                .actor_user_id
                .or_else(|| claims.and_then(|claims| UserId::from_str(&claims.sub).ok())),
            actor_username: enrichment
                .actor_username
                .or_else(|| claims.map(|claims| claims.username.clone())),
            acting_role: extensions
                .get::<ActingRole>()
                .map(|role| role.0.clone().into()),
            category,
            action,
            resource_type: enrichment.resource_type,
            resource_id: enrichment.resource_id,
            http_method,
            http_path,
            http_status: i16::try_from(status.as_u16()).unwrap_or(i16::MAX),
            outcome,
            client_ip,
            user_agent,
            latency_ms: i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX),
            detail: enrichment
                .detail
                .unwrap_or_else(OperationDetailDocument::empty),
            before_hash: enrichment.before_hash,
            after_hash: enrichment.after_hash,
            governance_audit_event_id: enrichment.governance_audit_event_id,
            governance_audit_sequence: enrichment.governance_audit_sequence,
        };
        buffer.try_enqueue(log);
    }

    Ok(res)
}

/// Read a request header as an owned string, if present and valid UTF-8.
fn header_value(req: &ServiceRequest, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

/// Convert only audited transport methods. Returning `None` for reads and
/// unsupported extension methods keeps the persisted enum semantically exact.
const fn operation_http_method(method: &Method) -> Option<OperationHttpMethod> {
    match *method {
        Method::POST => Some(OperationHttpMethod::Post),
        Method::PUT => Some(OperationHttpMethod::Put),
        Method::PATCH => Some(OperationHttpMethod::Patch),
        Method::DELETE => Some(OperationHttpMethod::Delete),
        _ => None,
    }
}

/// Derive the outcome from the final HTTP status when the handler did not set
/// one explicitly.
fn derive_outcome(status: StatusCode) -> OperationOutcome {
    if status.is_success() {
        OperationOutcome::Success
    } else if matches!(status.as_u16(), 401 | 403) {
        OperationOutcome::Denied
    } else {
        OperationOutcome::Failure
    }
}

/// Best-effort category inference from the matched route (or raw path) when the
/// handler did not enrich the context.
fn fallback_category(path: &str) -> OperationCategory {
    let path = path.strip_prefix("/api").unwrap_or(path);
    if path.starts_with("/auth") {
        OperationCategory::Auth
    } else if path.starts_with("/users")
        || path.starts_with("/roles")
        || path.starts_with("/menus")
        || path.starts_with("/permissions")
    {
        OperationCategory::Rbac
    } else if path.starts_with("/control-factor") {
        OperationCategory::Governance
    } else if path.starts_with("/runtime-config") {
        OperationCategory::DecisionPolicySnapshot
    } else if path.starts_with("/system") {
        OperationCategory::System
    } else if path.starts_with("/quant/reports") || path.starts_with("/quant/recommendations") {
        OperationCategory::QuantReport
    } else if path.starts_with("/markets") {
        OperationCategory::Market
    } else if path.starts_with("/replay") {
        OperationCategory::Replay
    } else {
        OperationCategory::Other
    }
}

/// Fallback action label (`METHOD pattern`) for un-enriched mutating requests.
fn fallback_action(method: &Method, path: &str) -> String {
    format!("{} {path}", method.as_str())
}
