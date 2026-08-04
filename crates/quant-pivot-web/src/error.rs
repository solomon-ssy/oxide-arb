//! Web-layer error type and HTTP mapping.
//!
//! [`WebError`] is the single error surfaced by handlers, extractors, and
//! middleware. Its `ResponseError` implementation renders the *same* envelope
//! as [`crate::response::WebResponse`] (with `data: null`), so success and
//! error payloads are structurally identical.
//!
//! Domain errors are funneled in via `From` conversions:
//!
//! - [`AuthError`] — every variant maps to `401` except a token-store outage,
//!   which is `503`. The client-facing message is deliberately generic so it
//!   cannot be used as an authentication oracle.
//! - [`StorageError`] — `NotFound` → 404, duplicate/state/transition → 409,
//!   `InvariantViolation` → 400, transport/timeout failures → 503, everything
//!   else → 500 (details are logged, not leaked).
//! - [`RbacError`] — governed denial → 403; structural assignment / permission
//!   parsing → 400.

use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use quant_pivot_error::{
    QuantError, auth::AuthError, control::ControlError, execution::ExecutionError,
    feedback::FeedbackError, governance::GovernanceError, infra::InfraError, query::QueryError,
    rbac::RbacError, report::ReportError, research::ResearchError, storage::StorageError,
};
use thiserror::Error;

use crate::response::WebResponse;

/// The unified error returned across the web layer.
#[derive(Debug, Error)]
pub enum WebError {
    /// Authentication failed or was absent (HTTP 401).
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Authenticated but not permitted (HTTP 403).
    #[error("forbidden")]
    Forbidden,

    /// The requested resource does not exist (HTTP 404).
    #[error("not found: {0}")]
    NotFound(String),

    /// Malformed request / failed validation (HTTP 400).
    #[error("bad request: {0}")]
    BadRequest(String),

    /// A uniqueness or state conflict (HTTP 409).
    #[error("conflict: {0}")]
    Conflict(String),

    /// A bounded queue is full (HTTP 429).
    #[error("too many requests: {0}")]
    TooManyRequests(String),

    /// Well-formed request whose domain preconditions are not met (HTTP 422).
    #[error("unprocessable entity: {0}")]
    UnprocessableEntity(String),

    /// An unexpected server-side failure (HTTP 500). The detail is logged but
    /// never returned to the client.
    #[error("internal error: {0}")]
    Internal(String),

    /// A dependency (DB / Redis) is unavailable (HTTP 503).
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl WebError {
    /// HTTP status code for this error.
    const fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::UnprocessableEntity(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// Client-safe message. Internal failures are masked so implementation
    /// details never reach the wire.
    fn client_message(&self) -> String {
        match self {
            Self::Unauthorized(msg)
            | Self::NotFound(msg)
            | Self::BadRequest(msg)
            | Self::Conflict(msg)
            | Self::TooManyRequests(msg)
            | Self::UnprocessableEntity(msg)
            | Self::ServiceUnavailable(msg) => msg.clone(),
            Self::Forbidden => "forbidden".to_owned(),
            Self::Internal(_) => "internal error".to_owned(),
        }
    }
}

impl ResponseError for WebError {
    fn status_code(&self) -> StatusCode {
        self.status()
    }

    fn error_response(&self) -> HttpResponse {
        let status = self.status();
        if let Self::Internal(detail) = self {
            tracing::error!(%detail, "web handler internal error");
        }
        let envelope = WebResponse::<()> {
            code: status.as_u16(),
            message: self.client_message(),
            data: None,
        };
        HttpResponse::build(status).json(envelope)
    }
}

impl From<AuthError> for WebError {
    fn from(error: AuthError) -> Self {
        match error {
            // Fail-closed: the token revocation store is unreachable, so we
            // cannot prove a token is still valid — treat as transient outage.
            AuthError::BlacklistUnavailable => {
                Self::ServiceUnavailable("authentication temporarily unavailable".to_owned())
            }
            AuthError::InvalidCredentials => Self::Unauthorized("invalid credentials".to_owned()),
            // All token-shaped failures collapse to one generic message so the
            // client cannot distinguish missing / malformed / expired / revoked.
            AuthError::MissingToken
            | AuthError::InvalidToken
            | AuthError::ExpiredToken
            | AuthError::Blacklisted
            | AuthError::WrongTokenType { .. } => Self::Unauthorized("unauthorized".to_owned()),
        }
    }
}

impl From<StorageError> for WebError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound { entity, id } => {
                Self::NotFound(format!("{entity} not found: {id}"))
            }
            StorageError::Duplicate { entity, key } => {
                Self::Conflict(format!("{entity} already exists: {key}"))
            }
            StorageError::IllegalTransition {
                entity,
                id,
                from,
                to,
            } => Self::Conflict(format!(
                "illegal transition for {entity}{}: {from} -> {to}",
                id.as_ref()
                    .map_or(String::new(), |value| format!(" `{value}`"))
            )),
            StorageError::StateConflict { entity, id, detail } => Self::Conflict(format!(
                "state conflict for {entity}{}: {detail}",
                id.as_ref()
                    .map_or(String::new(), |value| format!(" `{value}`"))
            )),
            StorageError::InvariantViolation { detail, .. } => Self::BadRequest(detail),
            StorageError::CapacityExceeded { entity, limit } => {
                Self::TooManyRequests(format!("{entity} queue capacity {limit} reached"))
            }
            StorageError::Connection(_)
            | StorageError::Redis(_)
            | StorageError::RedisPool(_)
            | StorageError::Timeout { .. } => {
                Self::ServiceUnavailable("storage temporarily unavailable".to_owned())
            }
            other => Self::Internal(other.to_string()),
        }
    }
}

impl From<RbacError> for WebError {
    fn from(error: RbacError) -> Self {
        match error {
            RbacError::PermissionDenied { .. } => Self::Forbidden,
            RbacError::UnknownPermission { .. } | RbacError::InvalidAssignment { .. } => {
                Self::BadRequest(error.to_string())
            }
        }
    }
}

impl From<QueryError> for WebError {
    fn from(error: QueryError) -> Self {
        // Both variants are caller-supplied window misuse → 400.
        Self::BadRequest(error.to_string())
    }
}

impl From<ControlError> for WebError {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::Precondition(_) => Self::Conflict(error.to_string()),
            ControlError::Engine(_) => Self::Internal(error.to_string()),
            ControlError::CommittedGenerationApply { .. } => {
                Self::ServiceUnavailable("committed runtime generation is not ready".to_owned())
            }
        }
    }
}

impl From<ExecutionError> for WebError {
    fn from(error: ExecutionError) -> Self {
        match error {
            ExecutionError::CapitalRecoveryFailed { .. }
            | ExecutionError::AdmissionDeferred { .. }
            | ExecutionError::OutcomeReconciliationSource { .. } => {
                Self::ServiceUnavailable(error.to_string())
            }
            ExecutionError::ReportOnlyMode
            | ExecutionError::ModePreflightDenied { .. }
            | ExecutionError::KillSwitchBlocks { .. }
            | ExecutionError::RecommendationExpired { .. }
            | ExecutionError::NotSubmittable { .. }
            | ExecutionError::IntentDenied { .. }
            | ExecutionError::AdmissionDenied { .. }
            | ExecutionError::ApprovalInvalidated { .. }
            | ExecutionError::ReconciliationUnresolvable { .. }
            | ExecutionError::ReconciliationNotResolvable { .. }
            | ExecutionError::SettlementRecoveryUnavailable { .. }
            | ExecutionError::ModeTransitionForbidden { .. } => Self::Conflict(error.to_string()),
            ExecutionError::ReconciliationResolveInvalid { .. } => {
                Self::BadRequest(error.to_string())
            }
            ExecutionError::OutcomeReconciliationInvariant { .. }
            | ExecutionError::SettlementRedeemInvariant { .. }
            | ExecutionError::TimeConversion { .. } => Self::Internal(error.to_string()),
        }
    }
}

impl From<QuantError> for WebError {
    fn from(error: QuantError) -> Self {
        match error {
            QuantError::Storage(storage) => storage.into(),
            QuantError::Rbac(rbac) => rbac.into(),
            QuantError::Research(ResearchError::NotEligible { code, detail }) => {
                Self::UnprocessableEntity(format!("{code}: {detail}"))
            }
            QuantError::Research(research @ ResearchError::SellOofEstimatorRequired) => {
                Self::UnprocessableEntity(research.to_string())
            }
            QuantError::Research(
                ResearchError::DatasetPlan { detail }
                | ResearchError::LeakageDetected { detail }
                | ResearchError::LabelResolution { detail }
                | ResearchError::DatasetBuild { detail },
            ) => Self::BadRequest(detail),
            QuantError::Feedback(feedback) => feedback.into(),
            QuantError::Config(_) => Self::BadRequest(error.to_string()),
            QuantError::Governance(governance) => governance.into(),
            QuantError::Report(ReportError::IncomparableReports { detail }) => {
                Self::BadRequest(detail)
            }
            QuantError::Report(_) => Self::Internal(error.to_string()),
            QuantError::Execution(execution) => execution.into(),
            QuantError::Infra(ref infra) => match infra {
                InfraError::MetricsRegistration { .. }
                | InfraError::ChannelClosed { .. }
                | InfraError::ChannelTimeout { .. }
                | InfraError::ComputeDeadline { .. } => {
                    Self::ServiceUnavailable("service temporarily unavailable".to_owned())
                }
                InfraError::ComputeCapacity { .. } => {
                    Self::TooManyRequests("service capacity reached".to_owned())
                }
                InfraError::ServerBind { .. }
                | InfraError::ServerRuntime { .. }
                | InfraError::Misconfigured { .. }
                | InfraError::AuditDetailInvalid { .. }
                | InfraError::BlockingTaskJoin { .. }
                | InfraError::ComputeExecution { .. } => Self::Internal(error.to_string()),
            },
            QuantError::Control(control) => control.into(),
            other => Self::Internal(other.to_string()),
        }
    }
}

impl From<FeedbackError> for WebError {
    fn from(error: FeedbackError) -> Self {
        let message = error.to_string();
        match error {
            FeedbackError::InvalidPromotionPermit { .. } => Self::BadRequest(message),
            FeedbackError::InvalidCycleIdentity { .. }
            | FeedbackError::InvalidCycleState { .. }
            | FeedbackError::IllegalCycleTransition { .. }
            | FeedbackError::StaleCycleGeneration { .. }
            | FeedbackError::EvaluationReuse { .. }
            | FeedbackError::SameWindowMismatch { .. }
            | FeedbackError::PromotionPermitConflict { .. }
            | FeedbackError::InvalidModelRouteEvidence { .. }
            | FeedbackError::InvalidPromotionPreflight { .. }
            | FeedbackError::PromotionTransactionConflict { .. }
            | FeedbackError::InvalidBootstrapPreflight { .. }
            | FeedbackError::BootstrapTransactionConflict { .. }
            | FeedbackError::ModelRouteConvergenceConflict { .. } => Self::Conflict(message),
            _ => Self::Internal(message),
        }
    }
}

impl From<GovernanceError> for WebError {
    fn from(error: GovernanceError) -> Self {
        match error {
            GovernanceError::NotFound { entity, id } => {
                Self::NotFound(format!("{entity} not found: {id}"))
            }
            GovernanceError::QualityGateFailed { .. }
            | GovernanceError::ShadowNotStable { .. }
            | GovernanceError::IllegalTransition { .. } => Self::Conflict(error.to_string()),
            GovernanceError::NumericOverflow { .. }
            | GovernanceError::LinkagePayloadSerialization { .. } => {
                Self::Internal(error.to_string())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use quant_pivot_error::{
        QuantError,
        control::{ControlError, RuntimeApplyStage},
        execution::ExecutionError,
        feedback::FeedbackError,
        infra::InfraError,
        rbac::RbacError,
        research::ResearchError,
        storage::{
            StorageError,
            entity::{QUANT_ORDER_INTENT, USER},
        },
    };

    use super::WebError;

    #[test]
    fn storage_duplicate_maps_409() {
        let web = WebError::from(StorageError::duplicate(USER, "alice"));
        assert_eq!(web.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn storage_invariant_maps_400() {
        let web = WebError::from(StorageError::invariant_violation(
            Some(QUANT_ORDER_INTENT),
            "invalid create payload",
        ));
        assert_eq!(web.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rbac_denial_maps_403() {
        let error = RbacError::PermissionDenied {
            actor_user_id: "0198d9f6-f3f7-7000-8000-000000000001".to_owned(),
            acting_role: "risk_owner".to_owned(),
            resource: "publication",
            operation: "publish",
        };
        let web = WebError::from(QuantError::from(error));
        assert_eq!(web.status(), StatusCode::FORBIDDEN);
        assert_eq!(web.client_message(), "forbidden");
    }

    #[test]
    fn execution_conflict_maps_409() {
        let web = WebError::from(QuantError::from(ExecutionError::AdmissionDenied {
            reason: "spread too wide".to_owned(),
        }));
        assert_eq!(web.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn sell_oof_maps_422() {
        let web = WebError::from(QuantError::from(ResearchError::SellOofEstimatorRequired));
        assert_eq!(web.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn permit_contract_maps_400() {
        let web = WebError::from(QuantError::from(FeedbackError::InvalidPromotionPermit {
            detail: "allowed modes are not canonical".to_owned(),
        }));

        assert_eq!(web.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn feedback_conflict_maps_409() {
        let web = WebError::from(QuantError::from(FeedbackError::PromotionPermitConflict {
            detail: "revision differs".to_owned(),
        }));

        assert_eq!(web.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn capital_recovery_maps_503() {
        let web = WebError::from(QuantError::from(ExecutionError::CapitalRecoveryFailed {
            reason: "allocation invariant broken".to_owned(),
        }));
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn committed_apply_maps_503() {
        let web = WebError::from(QuantError::from(ControlError::CommittedGenerationApply {
            desired_generation: 12,
            applied_generation: 11,
            stage: RuntimeApplyStage::Prepare,
            detail: "private artifact location".to_owned(),
        }));
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            web.client_message(),
            "committed runtime generation is not ready"
        );
    }

    #[test]
    fn admission_deferred_maps_503() {
        let web = WebError::from(QuantError::from(ExecutionError::AdmissionDeferred {
            reason: "book snapshot stale".to_owned(),
        }));
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn compute_capacity_maps_429() {
        let web = WebError::from(QuantError::from(InfraError::ComputeCapacity {
            subsystem: "password crypto",
            limit: 8,
        }));
        assert_eq!(web.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(web.client_message(), "service capacity reached");
    }

    #[test]
    fn compute_deadline_maps_503() {
        let web = WebError::from(QuantError::from(InfraError::ComputeDeadline {
            subsystem: "password crypto",
            deadline_ms: 15_000,
        }));
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(web.client_message(), "service temporarily unavailable");
    }

    #[test]
    fn outcome_reconciliation_maps_503() {
        let web = WebError::from(QuantError::from(
            ExecutionError::OutcomeReconciliationSource {
                reason: "finalized RPC unavailable".to_owned(),
            },
        ));
        assert_eq!(web.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn outcome_reconciliation_maps_500() {
        let web = WebError::from(QuantError::from(
            ExecutionError::OutcomeReconciliationInvariant {
                reason: "durable fact readback mismatch".to_owned(),
            },
        ));
        assert_eq!(web.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
