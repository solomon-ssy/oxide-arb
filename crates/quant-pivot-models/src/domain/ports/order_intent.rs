//! Web-facing port for the governed order-intent surface.
//!
//! Dependency-inversion boundary between the HTTP handlers and the core
//! `CoreOrderIntentService`. Handlers depend only on this trait — never on a
//! repository, authorization policy implementation, or venue client directly. Implemented in
//! `quant-pivot-core` and injected into `quant_pivot_web::state::AppState`.
//!
//! Every mutation is governed and audited: `create` turns a published
//! recommendation into an `OrderIntent` via the authorization policy; `approve` /
//! `reject` / `cancel` drive the intent + capital FSM in one transaction. The
//! commands carry the acting operator identity so the service can stamp
//! `approved_by` and the operation log.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::OrderIntentListQuery,
        pagination::Paginated,
        quant::{ExecutionOrderInfo, OrderIntentInfo},
    },
    types::{OrderAmount, OrderIntentId, Price, RecommendationId, RoleCode, UserId},
};

/// Create an order intent from a published recommendation.
///
/// The live authorization policy decides whether the intent starts as
/// `PendingAuthorization` or `Authorized`. `operator_id` / `acting_role` / `reason` are
/// recorded in the operation log.
#[derive(Debug, Clone)]
pub struct CreateIntentCommand {
    pub recommendation_id: RecommendationId,
    pub operator_id: UserId,
    pub acting_role: RoleCode,
    pub reason: String,
}

/// Approve a `PendingAuthorization` intent.
///
/// Approval may only **narrow** the tagged amount and side-aware price. The
/// reserved capital is atomically shrunk to the final frozen order notional.
#[derive(Debug, Clone)]
pub struct ApproveIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: UserId,
    pub acting_role: RoleCode,
    pub reason: String,
    pub override_amount: Option<OrderAmount>,
    pub override_price: Option<Price>,
}

/// Reject a `PendingAuthorization` intent and release its reserved capital.
///
/// Both `operator` and `risk_owner` may reject (risk may veto but not approve).
#[derive(Debug, Clone)]
pub struct RejectIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: UserId,
    pub acting_role: RoleCode,
    pub reason: String,
}

/// Cancel a not-yet-submitted intent and release its reserved capital.
#[derive(Debug, Clone)]
pub struct CancelIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: UserId,
    pub acting_role: RoleCode,
    pub reason: String,
}

/// Read + governed-mutation port for order intents.
#[async_trait]
pub trait OrderIntentPort: Send + Sync {
    /// Create an intent from a recommendation (authorization-gated, reserves capital).
    async fn create(&self, command: CreateIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Approve a pending intent (re-checks invalidation, optional downscale).
    async fn approve(&self, command: ApproveIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Reject a pending intent and release its capital.
    async fn reject(&self, command: RejectIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Cancel a not-yet-submitted intent and release its capital.
    async fn cancel(&self, command: CancelIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Page intents filtered by status / authorization / `created_at` window.
    async fn list(&self, query: OrderIntentListQuery) -> QuantResult<Paginated<OrderIntentInfo>>;

    /// Load one intent by id.
    async fn find(&self, id: &OrderIntentId) -> QuantResult<Option<OrderIntentInfo>>;
}

/// Web-facing port for the real-money entry-submission bridge.
///
/// Dependency-inversion boundary between the HTTP handler / auto worker and the
/// core `CoreExecutionDispatcher`. Submission claims the intent (row-locked),
/// runs the 26-check admission engine, and — on `allow` — signs and posts the
/// order to the venue, settling capital + position in one transaction.
#[async_trait]
pub trait ExecutionSubmitPort: Send + Sync {
    /// Claim, admit, submit, and settle one intent; returns the resulting
    /// execution order (possibly `ambiguous` when the venue response is
    /// unconfirmed). Terminal admission denial / non-submittable intents return a
    /// typed `ExecutionError`; transient defers return `AdmissionDeferred`.
    async fn submit_if_admitted(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<ExecutionOrderInfo>;
}
