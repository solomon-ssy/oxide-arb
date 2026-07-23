//! Core [`ExecutionRecoveryPort`] — detailed recovery view for operators.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::{
    api::ExecutionRecoveryView,
    ports::{ExecutionRecoveryPort, KillSwitchPort},
};
use quant_pivot_repository::traits::ReconciliationRepository;

use crate::governance::{RuntimeControlsHandle, execution_recovery::build_execution_recovery_view};

/// Production execution recovery read port.
pub struct CoreExecutionRecoveryPort {
    reconciliation: Arc<dyn ReconciliationRepository>,
    kill_switch: Arc<dyn KillSwitchPort>,
    runtime_controls: RuntimeControlsHandle,
}

impl CoreExecutionRecoveryPort {
    #[must_use]
    pub fn new(
        reconciliation: Arc<dyn ReconciliationRepository>,
        kill_switch: Arc<dyn KillSwitchPort>,
        runtime_controls: RuntimeControlsHandle,
    ) -> Self {
        Self {
            reconciliation,
            kill_switch,
            runtime_controls,
        }
    }
}

#[async_trait]
impl ExecutionRecoveryPort for CoreExecutionRecoveryPort {
    async fn view(&self) -> QuantResult<ExecutionRecoveryView> {
        build_execution_recovery_view(
            &self.reconciliation,
            &self.kill_switch,
            &self.runtime_controls,
        )
        .await
    }
}
