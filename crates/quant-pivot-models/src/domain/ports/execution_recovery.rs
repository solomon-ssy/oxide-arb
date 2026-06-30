//! Execution recovery playbook read port.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::domain::ExecutionRecoveryView;

#[async_trait]
pub trait ExecutionRecoveryPort: Send + Sync {
    /// Detailed recovery view for operator runbooks.
    async fn view(&self) -> QuantResult<ExecutionRecoveryView>;
}
