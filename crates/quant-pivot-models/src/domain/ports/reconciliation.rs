//! Governed operator reconciliation resolve port.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::domain::{ResolveReconciliationCommand, ResolveReconciliationOutcome};

#[async_trait]
pub trait ReconciliationPort: Send + Sync {
    /// Operator override of a blocking `Unresolvable` reconciliation row.
    async fn resolve_operator(
        &self,
        command: ResolveReconciliationCommand,
    ) -> QuantResult<ResolveReconciliationOutcome>;
}
