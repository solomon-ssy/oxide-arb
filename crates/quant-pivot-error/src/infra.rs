//! Process bootstrap, observability, and runtime infrastructure errors.

use thiserror::Error;

/// Fail-stop reasons for the canonical L2 persistence boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CanonicalLedgerFailure {
    #[error("writer queue closed")]
    QueueClosed,
    #[error("commit cursor closed")]
    CursorClosed,
    #[error("absolute reconciliation ceiling elapsed")]
    ReconciliationTimeout,
    #[error("partition client was already fenced")]
    ClientFenced,
}

/// Failures during startup wiring, metrics, channels, and server runtime.
#[derive(Debug, Error)]
pub enum InfraError {
    #[error("metrics registration failed for {subsystem}: {detail}")]
    MetricsRegistration {
        subsystem: &'static str,
        detail: String,
    },

    #[error("misconfigured startup: {detail}")]
    Misconfigured { detail: String },

    #[error("channel closed: {name}")]
    ChannelClosed { name: &'static str },

    #[error("bounded channel timed out: {name}")]
    ChannelTimeout { name: &'static str },

    #[error("canonical ledger fail-stop for partition {partition}, batch {batch_id}: {failure}")]
    CanonicalLedger {
        partition: u8,
        batch_id: u64,
        failure: CanonicalLedgerFailure,
    },

    #[error("derived fact persistence failed for partition {partition}")]
    DerivedFactPersistence { partition: u8 },

    #[error("blocking task join failed: {detail}")]
    BlockingTaskJoin { detail: String },

    #[error("governed compute execution failed: {detail}")]
    ComputeExecution { detail: String },

    #[error("{subsystem} capacity {limit} reached")]
    ComputeCapacity {
        subsystem: &'static str,
        limit: usize,
    },

    #[error("{subsystem} exceeded its {deadline_ms} ms deadline")]
    ComputeDeadline {
        subsystem: &'static str,
        deadline_ms: u64,
    },

    #[error("operation audit detail is invalid: {detail}")]
    AuditDetailInvalid { detail: String },

    #[error("web server bind failed: {detail}")]
    ServerBind { detail: String },

    #[error("web server runtime error: {detail}")]
    ServerRuntime { detail: String },

    #[error("venue incentive reconciliation failed for {failed_days} day(s): {detail}")]
    VenueIncentiveReconciliation { failed_days: usize, detail: String },
}
