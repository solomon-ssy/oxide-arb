//! Control-plane validation and governance errors.

use thiserror::Error;

use crate::storage::StorageError;

/// Failure while computing a canonical BLAKE3 digest over a serializable value.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CanonicalDigestError {
    #[error("failed to serialize value for canonical digest: {0}")]
    Serialize(String),
}

/// Failure detected while verifying the append-only audit hash chain.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuditChainError {
    #[error("audit event at sequence {sequence} has hash {actual}, expected {expected}")]
    HashMismatch {
        sequence: i64,
        expected: String,
        actual: String,
    },
    #[error("audit chain sequence gap: expected {expected}, found {actual}")]
    SequenceGap { expected: i64, actual: i64 },
    #[error("audit event at sequence {sequence} does not link to its predecessor")]
    BrokenLink { sequence: i64 },
    #[error("genesis audit event at sequence {sequence} must have no predecessor hash")]
    GenesisPrevNotNull { sequence: i64 },
    #[error(transparent)]
    Digest(#[from] CanonicalDigestError),
}

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

/// Failure while compiling an active publication into a live `ControlFactorSnapshot`.
///
/// Surfaced by the live refresher. Whether a given failure aborts Live startup
/// or merely keeps the prior snapshot is a policy decision made by the caller
/// (fail closed vs fail neutral); this enum only classifies the cause.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SnapshotBuildError {
    #[error(
        "factor {factor_id} schema version {actual} is incompatible with supported {supported}"
    )]
    SchemaMismatch {
        factor_id: String,
        actual: u32,
        supported: u32,
    },
    #[error("factor {factor_id} payload type does not match its declared dimensions")]
    DimensionPayloadMismatch { factor_id: String },
    #[error("factor {factor_id} payload violates conservative safety constraints: {source}")]
    PayloadConstraint {
        factor_id: String,
        #[source]
        source: PayloadSafetyError,
    },
    #[error("publication member {factor_id} is not present in the resolved factor set")]
    MissingMember { factor_id: String },
    #[error("critical safety factor {factor_id} expired at {expires_at} (now {now})")]
    ExpiredSafetyFactor {
        factor_id: String,
        expires_at: String,
        now: String,
    },
    #[error("publication hash mismatch: expected {expected}, got {actual}")]
    PublicationHashMismatch { expected: String, actual: String },
    #[error("factor {factor_id} payload hash mismatch: expected {expected}, got {actual}")]
    PayloadHashMismatch {
        factor_id: String,
        expected: String,
        actual: String,
    },
    #[error("factor {factor_id} dimensions hash mismatch: expected {expected}, got {actual}")]
    DimensionsHashMismatch {
        factor_id: String,
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StatsError {
    #[error("statistical sample is empty")]
    EmptySample,
    #[error("statistical denominator is zero")]
    ZeroDenominator,
}

pub type StatsResult<T> = Result<T, StatsError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControlPersistenceError {
    #[error("failed to encode control persistence field {field}: {message}")]
    Encode {
        field: &'static str,
        message: String,
    },
    #[error("control persistence integer field {field} overflowed with value {value}")]
    IntegerOverflow { field: &'static str, value: u64 },
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
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
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
    #[error("mutating governance operation requires a non-empty reason")]
    MissingReason,
    #[error("mutating governance operation requires a non-empty {field}")]
    MissingField { field: &'static str },
    #[error("idempotency conflict for key {key}: a different request already exists")]
    IdempotencyConflict { key: String },
    #[error("risk-expanding change requires explicit risk-owner approval and justification")]
    RiskExpansionNotApproved,
    #[error("publication to Published requires a known-good rollback target")]
    RollbackTargetMissing,
    #[error("publication activation lost a concurrency race; retry")]
    PublicationLockConflict,
    #[error(transparent)]
    AuditChain(#[from] AuditChainError),
}

/// Error surface of the control-factor registry service (orchestration layer).
#[derive(Debug, Error)]
pub enum RegistryError {
    #[error(transparent)]
    Governance(#[from] GovernanceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

pub type MaterializationResult<T> = Result<T, MaterializationError>;

#[derive(Debug, Error)]
pub enum MaterializationError {
    #[error("{code}: {message}")]
    Stable { code: String, message: String },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("codec error: {0}")]
    Codec(String),

    #[error(transparent)]
    Persistence(#[from] ControlPersistenceError),

    #[error(transparent)]
    CanonicalDigest(#[from] CanonicalDigestError),
}

impl MaterializationError {
    #[must_use]
    pub fn stable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Stable {
            code: code.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Stable { code, .. } => Some(code.as_str()),
            Self::Storage(_) | Self::Codec(_) | Self::Persistence(_) | Self::CanonicalDigest(_) => {
                None
            }
        }
    }

    #[must_use]
    pub fn failure_code(&self) -> String {
        self.code()
            .map_or_else(|| "run.storage_or_codec_error".to_owned(), str::to_owned)
    }
}
