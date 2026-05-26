//! Non-blocking audit enqueue for pre-trade decision events.

use crate::audit::RiskAuditEvent;

/// Result of a best-effort audit enqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEnqueueResult {
    Queued,
    Dropped,
}

/// Best-effort sink for pre-trade audit events.
///
/// Implementations must never block the hot path or halt the engine on failure.
pub trait AuditSink: Send + Sync {
    fn try_enqueue(&self, event: RiskAuditEvent) -> AuditEnqueueResult;
}
