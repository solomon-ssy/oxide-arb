//! Web-facing port for the governed order-intent surface.
//!
//! Dependency-inversion boundary between the HTTP handlers and the core
//! `CoreOrderIntentService`. Handlers depend only on this trait — never on a
//! repository, the mode gate, or a venue client directly. Implemented in
//! `quant-pivot-core` and injected into `quant_pivot_web::AppState`.
//!
//! Every mutation is governed and audited: `create` turns a published
//! recommendation into an `OrderIntent` via the runtime mode gate; `approve` /
//! `reject` / `cancel` drive the intent + capital FSM in one transaction. The
//! commands carry the acting operator identity so the service can stamp
//! `approved_by` and the operation log.

use async_trait::async_trait;

use crate::{
    domain::{ExecutionOrderInfo, OrderIntentInfo, OrderIntentListQuery, Paginated},
    types::{OrderIntentId, Price, RecommendationId, Shares, Usd},
};
use quant_pivot_error::QuantResult;
use uuid::Uuid;

/// Create an order intent from a published recommendation.
///
/// The runtime mode gate decides the outcome: `report_only` is rejected,
/// `semi_auto` yields a `PendingApproval` intent, `auto_execution` yields an
/// `ApprovedByPolicy` intent. `operator_id` / `acting_role` / `reason` are
/// recorded in the operation log.
#[derive(Debug, Clone)]
pub struct CreateIntentCommand {
    pub recommendation_id: RecommendationId,
    pub operator_id: Uuid,
    pub acting_role: String,
    pub reason: String,
}

/// Approve a `PendingApproval` intent.
///
/// Approval may only **narrow** the order: `override_shares` (≤ frozen shares)
/// and `override_limit_price` (≤ recommendation limit) reduce the entry, and the
/// reserved capital is shrunk to match in the same transaction. Widening or
/// loosening any bound is rejected. `max_allowed_usd`, when present, caps the
/// resulting notional. Approval is refused outright if any invalidation
/// condition is met at approval time (fail-closed).
#[derive(Debug, Clone)]
pub struct ApproveIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: Uuid,
    pub acting_role: String,
    pub reason: String,
    /// Optional downscaled share quantity (must be ≤ the frozen entry shares).
    pub override_shares: Option<Shares>,
    /// Optional downscaled limit price (must be ≤ the frozen entry limit).
    pub override_limit_price: Option<Price>,
    /// Optional hard cap on the approved notional.
    pub max_allowed_usd: Option<Usd>,
    /// Optional free-form operator note recorded in the operation log.
    pub override_note: Option<String>,
}

/// Reject a `PendingApproval` intent and release its reserved capital.
///
/// Both `operator` and `risk_owner` may reject (risk may veto but not approve).
#[derive(Debug, Clone)]
pub struct RejectIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: Uuid,
    pub acting_role: String,
    pub reason: String,
}

/// Cancel a not-yet-submitted intent (`PendingApproval` / `Approved` /
/// `ApprovedByPolicy`) and release its reserved capital.
#[derive(Debug, Clone)]
pub struct CancelIntentCommand {
    pub order_intent_id: OrderIntentId,
    pub operator_id: Uuid,
    pub acting_role: String,
    pub reason: String,
}

/// Read + governed-mutation port for order intents.
#[async_trait]
pub trait OrderIntentPort: Send + Sync {
    /// Create an intent from a recommendation (mode-gated, reserves capital).
    async fn create(&self, command: CreateIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Approve a pending intent (re-checks invalidation, optional downscale).
    async fn approve(&self, command: ApproveIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Reject a pending intent and release its capital.
    async fn reject(&self, command: RejectIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Cancel a not-yet-submitted intent and release its capital.
    async fn cancel(&self, command: CancelIntentCommand) -> QuantResult<OrderIntentInfo>;

    /// Page intents filtered by status / mode / `created_at` window.
    async fn list(&self, query: OrderIntentListQuery) -> QuantResult<Paginated<OrderIntentInfo>>;

    /// Load one intent by id.
    async fn find(&self, id: &OrderIntentId) -> QuantResult<Option<OrderIntentInfo>>;
}

/// Web-facing port for the real-money entry-submission bridge (Phase 05.4).
///
/// Dependency-inversion boundary between the HTTP handler / auto worker and the
/// core `CoreExecutionDispatcher`. Submission claims the intent (row-locked),
/// runs the 20-check admission engine, and — on `allow` — signs and posts the
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
