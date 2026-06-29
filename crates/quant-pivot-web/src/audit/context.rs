//! Request-scoped enrichment the handler hands back to the audit middleware.
//!
//! The [`OperationAudit`](crate::middleware) middleware inserts a shared
//! [`OperationContext`] into request extensions before dispatch; the handler
//! injects it via the [`OperationCtx`] extractor and stamps the semantic
//! attributes the middleware cannot infer from the HTTP envelope alone
//! (category, action, affected resource, a redacted detail summary, the linked
//! governance hash-chain event, and an outcome override). After the response,
//! the middleware reads the [`snapshot`](OperationContext::snapshot).
//!
//! Sharing uses `Rc<RefCell<…>>`: an actix worker drives the whole request
//! pipeline (outer middleware → handler → outer middleware) on a single thread,
//! so the handler's mutations are visible to the middleware without `Send`.
//!
//! Redaction is the handler's responsibility: `detail` must already be a safe
//! summary or diff — credentials, tokens, and PII must never be passed in.

use std::{
    cell::RefCell,
    future::{Ready, ready},
    ops::Deref,
    rc::Rc,
};

use actix_web::{Error as ActixError, FromRequest, HttpMessage, HttpRequest, dev::Payload};
use quant_pivot_models::{
    enums::{
        operation_log::{OperationCategory, OperationOutcome},
        rbac::ResourceType,
    },
    types::{AuditEventId, UserId},
};
use serde_json::Value;

/// Handler-supplied audit attributes captured for one request.
#[derive(Debug, Default, Clone)]
pub struct OperationEnrichment {
    /// Coarse grouping of the operation (auth / rbac / governance / …).
    pub category: Option<OperationCategory>,
    /// Specific action verb, e.g. `runtime_config.activate`.
    pub action: Option<String>,
    /// The kind of resource the operation affected.
    pub resource_type: Option<ResourceType>,
    /// The affected resource's identifier (stringified).
    pub resource_id: Option<String>,
    /// Redacted detail summary / diff (never raw request bodies or secrets).
    pub detail: Option<Value>,
    /// Canonical hash of the governed resource before a successful mutation.
    pub before_hash: Option<String>,
    /// Canonical hash of the governed resource after a successful mutation.
    pub after_hash: Option<String>,
    /// Linked governance hash-chain event (dual-track hard link).
    pub governance_audit_event_id: Option<AuditEventId>,
    /// Monotonic sequence of the linked governance audit event.
    pub governance_audit_sequence: Option<i64>,
    /// Outcome override (otherwise derived from the HTTP status).
    pub outcome: Option<OperationOutcome>,
    /// Actor id override for endpoints where no `Claims` exist yet (login).
    pub actor_user_id: Option<UserId>,
    /// Actor username override (e.g. the attempted username on a failed login).
    pub actor_username: Option<String>,
}

/// Request-scoped, handler-populated audit enrichment.
///
/// Obtain it in a handler via the [`OperationCtx`] extractor and call the
/// `set_*` / `link_*` / `mark_*` methods to enrich the operation-log row.
#[derive(Debug, Default)]
pub struct OperationContext {
    inner: RefCell<OperationEnrichment>,
}

impl OperationContext {
    /// Record the operation's category and action verb.
    pub fn set_action(&self, category: OperationCategory, action: &str) {
        let mut inner = self.inner.borrow_mut();
        inner.category = Some(category);
        inner.action = Some(action.to_owned());
    }

    /// Record the affected resource type and identifier.
    pub fn set_resource(&self, resource_type: ResourceType, id: impl Into<String>) {
        let mut inner = self.inner.borrow_mut();
        inner.resource_type = Some(resource_type);
        inner.resource_id = Some(id.into());
    }

    /// Attach a redacted detail summary / diff. The caller is responsible for
    /// ensuring no credentials, tokens, or PII are present.
    pub fn set_detail(&self, detail: Value) {
        self.inner.borrow_mut().detail = Some(detail);
    }

    /// Record canonical before/after state hashes for a governed mutation.
    pub fn set_state_hashes(&self, before_hash: Option<String>, after_hash: Option<String>) {
        let mut inner = self.inner.borrow_mut();
        inner.before_hash = before_hash;
        inner.after_hash = after_hash;
    }

    /// Hard-link this operation to its governance hash-chain event, enabling a
    /// foreign-key cross-walk between the two audit tracks.
    pub fn link_governance(&self, audit_event_id: AuditEventId, audit_sequence: i64) {
        let mut inner = self.inner.borrow_mut();
        inner.governance_audit_event_id = Some(audit_event_id);
        inner.governance_audit_sequence = Some(audit_sequence);
    }

    /// Override the outcome that would otherwise be derived from the HTTP status
    /// (e.g. a handler that returns 200 but semantically failed).
    pub fn mark_outcome(&self, outcome: OperationOutcome) {
        self.inner.borrow_mut().outcome = Some(outcome);
    }

    /// Record the attributed actor's username when no authenticated `Claims`
    /// exist yet — e.g. the attempted username on the login path (success *or*
    /// failure), so failed logins are attributable without leaking the password.
    pub fn set_actor_username(&self, username: &str) {
        self.inner.borrow_mut().actor_username = Some(username.to_owned());
    }

    /// Record the fully-resolved actor (id + username), e.g. after a successful
    /// login authenticates the user.
    pub fn set_actor(&self, user_id: UserId, username: &str) {
        let mut inner = self.inner.borrow_mut();
        inner.actor_user_id = Some(user_id);
        inner.actor_username = Some(username.to_owned());
    }

    /// Snapshot the accumulated enrichment for the middleware to persist.
    pub(crate) fn snapshot(&self) -> OperationEnrichment {
        self.inner.borrow().clone()
    }
}

/// Extractor handing a handler the shared request-scoped [`OperationContext`].
///
/// Cloning is cheap (an `Rc` bump) and yields the *same* context the audit
/// middleware reads after the response. When the audit middleware is not in the
/// pipeline (e.g. a focused unit test), a detached context is returned so
/// handlers never fail to extract it.
pub struct OperationCtx(Rc<OperationContext>);

impl Deref for OperationCtx {
    type Target = OperationContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromRequest for OperationCtx {
    type Error = ActixError;
    type Future = Ready<Result<Self, ActixError>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let ctx = req
            .extensions()
            .get::<Rc<OperationContext>>()
            .map_or_else(|| Rc::new(OperationContext::default()), Rc::clone);
        ready(Ok(Self(ctx)))
    }
}
