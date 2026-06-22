//! Quant runtime control port implementation for the web layer.

use crate::{governance::RuntimeModeHandle, infra::health_checker::HealthChecker};
use async_trait::async_trait;
use quant_pivot_models::{
    domain::{
        HealthReport, QuantModeTransitionReport, RuntimeControlError, RuntimeControlPort,
        SystemStatus,
    },
    enums::quant::QuantRuntimeMode,
};
use quant_pivot_repository::{
    postgres::PgSystemRuntimeStateRepository, traits::SystemRuntimeStateRepository,
};
use std::sync::Arc;

/// Phase 0 runtime control: quant mode reads and governed transitions.
pub struct QuantRuntimeControl {
    runtime_mode: RuntimeModeHandle,
    health_checker: Arc<HealthChecker>,
    runtime_state_repo: PgSystemRuntimeStateRepository,
}

impl QuantRuntimeControl {
    pub const fn new(
        runtime_mode: RuntimeModeHandle,
        health_checker: Arc<HealthChecker>,
        runtime_state_repo: PgSystemRuntimeStateRepository,
    ) -> Self {
        Self {
            runtime_mode,
            health_checker,
            runtime_state_repo,
        }
    }
}

#[async_trait]
impl RuntimeControlPort for QuantRuntimeControl {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode {
        self.runtime_mode.current()
    }

    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        reason: &str,
    ) -> Result<QuantModeTransitionReport, RuntimeControlError> {
        let from = self.runtime_mode.current();
        if from == target {
            return Ok(QuantModeTransitionReport { from, to: target });
        }
        self.runtime_state_repo
            .upsert_quant_runtime_mode(target, "operator", reason)
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))?;
        self.runtime_mode.store(target);
        Ok(QuantModeTransitionReport { from, to: target })
    }

    fn system_status(&self) -> SystemStatus {
        SystemStatus::report_only_bootstrap(self.runtime_mode.current())
    }

    async fn health(&self) -> HealthReport {
        self.health_checker.check_all().await
    }
}
