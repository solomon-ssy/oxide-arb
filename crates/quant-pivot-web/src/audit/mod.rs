//! Track two of the dual-track audit model: the general operation log.
//!
//! Every mutating HTTP request and authentication event is captured into the
//! append-only `operation_log` for forensics. This module owns the request-
//! scoped enrichment context, the non-blocking bounded buffer that decouples
//! request handling from persistence, and the background writer that batches
//! rows into Postgres.
//!
//! It is deliberately **best-effort**: a full channel or a failed write only
//! emits a warning and is dropped — the audit log must never delay or fail a
//! business response. Track one (the governance hash chain) is a future,
//! synchronously persisted facility; the two tracks are linked by
//! [`OperationContext::link_governance`] once it lands.

mod buffer;
mod context;

pub use buffer::OperationLogBuffer;
pub use context::{OperationContext, OperationCtx};
