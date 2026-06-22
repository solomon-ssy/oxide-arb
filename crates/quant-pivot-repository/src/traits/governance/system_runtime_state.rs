//! Operational runtime-state singleton repository (active execution mode).

use async_trait::async_trait;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{domain::SystemRuntimeStateInfo, enums::common::ExecutionMode};

/// Persistence for the `system_runtime_state` singleton.
///
/// The active execution mode survives restarts: the bootstrap restores the
/// operator's most recent deliberate mode, and `/system/mode` writes the new
/// mode here on every transition.
#[async_trait]
pub trait SystemRuntimeStateRepository: Send + Sync {
    /// Load the singleton, or `None` on a fresh database (first boot).
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError>;

    /// Upsert the active execution mode with its change metadata.
    async fn upsert_execution_mode(
        &self,
        mode: ExecutionMode,
        changed_by: &str,
        reason: &str,
    ) -> Result<(), StorageError>;
}
