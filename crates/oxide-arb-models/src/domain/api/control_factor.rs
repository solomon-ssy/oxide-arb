//! Governance control-plane API contract (control factors + publications).
//!
//! Mutating requests are governed: each carries a `reason` that enters the
//! immutable audit hash chain (and `AuditActor::validate` rejects an empty one),
//! while the acting role is supplied out-of-band via the `X-Acting-Role` header
//! and authorized by the authz middleware. Read responses project the
//! persistence `*Info` types directly — control-plane rows carry integrity
//! hashes and config payloads that authorized operators are meant to see, and no
//! sensitive credentials.

use crate::{
    domain::{
        control_factor::ControlFactorAuditEventInfo,
        evidence::{ControlFactorShadowDecisionInfo, ShadowDecisionAggregate},
    },
    enums::control_factor::{ControlFactorType, FactorStatus, PublicationMode, PublicationStatus},
    types::{ControlFactorId, FactorPublicationId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Reject a candidate factor. `reason` is recorded on the chained audit event.
#[derive(Debug, Deserialize, Validate)]
pub struct RejectFactorRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Stage a set of factors into an active publication (Shadow or Published).
///
/// Shared by the shadow-promote and publish endpoints. A publication is an
/// atomic *set*: it supersedes the current active publication for its mode, so
/// `factor_ids` is the complete membership. `idempotency_key` makes the activation
/// safely retryable; `reason` enters both the audit chain and the publication's
/// approval record.
#[derive(Debug, Deserialize, Validate)]
pub struct PublishPublicationRequest {
    #[validate(length(min = 1))]
    pub factor_ids: Vec<ControlFactorId>,
    #[validate(length(min = 1, max = 256))]
    pub idempotency_key: String,
    /// When the publication takes effect; defaults to now in the handler.
    pub effective_from: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    /// Explicit acknowledgement that the change expands risk vs. the active set.
    #[serde(default)]
    pub manual_risk_expansion_approval: bool,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Emergency publish: a short-TTL Published activation with operator-acknowledged
/// risk expansion.
///
/// The server computes `expires_at` (a short, bounded TTL) so an emergency
/// override can never become long-lived, and forces risk-expansion approval —
/// emergency *is* the explicit override path.
#[derive(Debug, Deserialize, Validate)]
pub struct EmergencyPublishRequest {
    #[validate(length(min = 1))]
    pub factor_ids: Vec<ControlFactorId>,
    #[validate(length(min = 1, max = 256))]
    pub idempotency_key: String,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Roll the active publication (path `{id}`) back to a known-good target.
#[derive(Debug, Deserialize, Validate)]
pub struct RollbackPublicationRequest {
    pub target_publication_id: FactorPublicationId,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Factor-catalog filter. The handler defaults `status` to `Candidate` (the
/// review queue) when omitted.
#[derive(Debug, Deserialize)]
pub struct ControlFactorListQuery {
    pub status: Option<FactorStatus>,
    pub factor_type: Option<ControlFactorType>,
}

/// Publication-catalog filter (`mode` is required; `status` optional).
#[derive(Debug, Deserialize)]
pub struct PublicationListQuery {
    pub mode: PublicationMode,
    pub status: Option<PublicationStatus>,
    pub limit: Option<u64>,
}

/// Audit-chain slice query (`from_sequence` defaults to 1, `limit` is capped in
/// the handler).
#[derive(Debug, Deserialize)]
pub struct AuditChainQuery {
    pub from_sequence: Option<i64>,
    pub limit: Option<u64>,
}

/// Shadow-decision window query for a publication.
#[derive(Debug, Deserialize)]
pub struct ShadowDecisionsQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<u64>,
}

/// Audit-chain query response.
///
/// `verified` is the result of [`AuditChain::verify`]; a failed verification is a
/// data-integrity *finding*, not a request error, so it is returned as a field
/// (HTTP 200) alongside `broken_at` (the first inconsistent sequence) for
/// forensic follow-up rather than surfaced as a 5xx.
///
/// [`AuditChain::verify`]: crate::domain::control_factor::AuditChain::verify
#[derive(Debug, Serialize)]
pub struct AuditChainResponse {
    pub events: Vec<ControlFactorAuditEventInfo>,
    pub verified: bool,
    pub broken_at: Option<i64>,
}

/// Shadow-publication decision evidence over a time window: the rollup that
/// supports a publish/abort decision, plus the raw decisions for drill-down.
#[derive(Debug, Serialize)]
pub struct ShadowDecisionsResponse {
    pub aggregate: ShadowDecisionAggregate,
    pub decisions: Vec<ControlFactorShadowDecisionInfo>,
}
