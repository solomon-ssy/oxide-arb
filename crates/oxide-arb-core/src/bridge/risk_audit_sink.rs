//! Bridge [`RiskDecisionAuditBuffer`] to the risk crate [`AuditSink`] trait.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::infra::risk_decision_audit_buffer::{EnqueueResult, RiskDecisionAuditBuffer};
use oxide_arb_risk::audit::RiskAuditEvent;
use oxide_arb_risk::audit_sink::{AuditEnqueueResult, AuditSink};

impl AuditSink for RiskDecisionAuditBuffer {
    fn try_enqueue(&self, event: RiskAuditEvent) -> AuditEnqueueResult {
        match Self::try_enqueue(self, event) {
            EnqueueResult::Queued => AuditEnqueueResult::Queued,
            EnqueueResult::DroppedChannelFull => AuditEnqueueResult::Dropped,
        }
    }
}

/// Create a buffer + receiver pair wrapped as an [`AuditSink`].
pub fn new_audit_sink(
    capacity: usize,
) -> (
    Arc<RiskDecisionAuditBuffer>,
    Mutex<Option<flume::Receiver<RiskAuditEvent>>>,
) {
    let (buffer, rx) = RiskDecisionAuditBuffer::new(capacity);
    (Arc::new(buffer), Mutex::new(Some(rx)))
}
