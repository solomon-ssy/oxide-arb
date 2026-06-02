//! Control-plane validation and governance errors.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PayloadSafetyError {
    #[error("{field} must be in the inclusive range 0..=1")]
    MultiplierOutOfRange { field: &'static str },
    #[error("{field} must be non-negative")]
    NegativeAddon { field: &'static str },
    #[error("{field} expands risk and requires explicit manual approval")]
    RiskExpandingWithoutApproval { field: &'static str },
    #[error("{field} cannot relax from true to false without manual approval")]
    BlockFlagRelaxed { field: &'static str },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FactorValueError {
    #[error("factor payload type {payload_type} does not match row type {factor_type}")]
    PayloadTypeMismatch {
        factor_type: String,
        payload_type: String,
    },
    #[error("factor expires_at must be after generated_at")]
    InvalidExpiry,
    #[error("failed to decode control-factor {field}: {message}")]
    TypedRowDecode {
        field: &'static str,
        message: String,
    },
    #[error("factor cannot enter governed status without sufficient evidence")]
    InsufficientEvidence,
    #[error("illegal factor status transition {from} -> {to}")]
    IllegalTransition { from: String, to: String },
    #[error("report-only factors cannot enter {target}")]
    ReportOnlyPromotionForbidden { target: String },
    #[error(transparent)]
    PayloadSafety(#[from] PayloadSafetyError),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GovernanceError {
    #[error(transparent)]
    FactorValue(#[from] FactorValueError),
    #[error("publication must include at least one factor")]
    EmptyPublication,
    #[error("publication expires_at must be after effective_from")]
    InvalidPublicationWindow,
    #[error("publication factor IDs do not match provided factor values")]
    FactorSetMismatch,
    #[error("failed to serialize publication hash input: {0}")]
    HashInput(String),
    #[error("publication hash mismatch: expected {expected}, got {actual}")]
    PublicationHashMismatch { expected: String, actual: String },
    #[error(
        "factor {factor_id} has status {actual} but publication mode {mode} requires {expected}"
    )]
    FactorNotReadyForPublication {
        factor_id: String,
        mode: String,
        expected: String,
        actual: String,
    },
}
