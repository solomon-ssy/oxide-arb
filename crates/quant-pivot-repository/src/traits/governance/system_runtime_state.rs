//! Operational runtime-state singleton repository (active quant runtime mode).

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{domain::SystemRuntimeStateInfo, enums::quant::QuantRuntimeMode};

/// Persistence for the `system_runtime_state` singleton.
#[async_trait]
pub trait SystemRuntimeStateRepository: Send + Sync {
    /// Load the singleton, or `None` on a fresh database (first boot).
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError>;

    /// Upsert the active quant runtime mode with its change metadata.
    async fn upsert_quant_runtime_mode(
        &self,
        mode: QuantRuntimeMode,
        changed_by: &str,
        reason: &str,
    ) -> Result<(), StorageError>;
}
