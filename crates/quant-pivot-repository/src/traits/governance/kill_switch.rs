use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::governance::{KillSwitchStateInfo, UpsertKillSwitchState};

/// Operational kill-switch singleton persistence port.
#[async_trait::async_trait]
pub trait KillSwitchStateRepository: Send + Sync {
    async fn load(&self) -> Result<Option<KillSwitchStateInfo>, StorageError>;

    async fn upsert(
        &self,
        state: UpsertKillSwitchState,
    ) -> Result<KillSwitchStateInfo, StorageError>;
}
