//! Operational runtime-state singleton repository (active quant runtime mode).

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ActivateBootstrapState, BootstrapActivationInfo, SystemRuntimeStateInfo},
    enums::quant::QuantRuntimeMode,
};

/// Persistence for the `system_runtime_state` singleton.
#[async_trait]
pub trait SystemRuntimeStateRepository: Send + Sync {
    /// Load the singleton, or `None` on a fresh database (first boot).
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError>;

    /// Idempotently record `initializing -> collecting_baseline` at process startup.
    async fn begin_baseline_collection(&self) -> Result<SystemRuntimeStateInfo, StorageError>;

    /// Idempotently record the first complete catalog baseline.
    async fn mark_catalog_baseline_ready(&self) -> Result<SystemRuntimeStateInfo, StorageError>;

    /// Atomically activate config, bootstrap state, and the WORM transition audit.
    async fn activate_bootstrap(
        &self,
        command: ActivateBootstrapState,
    ) -> Result<BootstrapActivationInfo, StorageError>;

    /// Update the active quant runtime mode. Missing/non-active state fails closed.
    async fn set_quant_runtime_mode(
        &self,
        mode: QuantRuntimeMode,
        changed_by: &str,
        reason: &str,
    ) -> Result<(), StorageError>;
}
