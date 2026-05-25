use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{RiskStateInfo, UpsertRiskEngineState};

pub trait RiskStateRepository: Send + Sync {
    async fn load(&self) -> Result<RiskStateInfo, StorageError>;
    async fn upsert(&self, state: UpsertRiskEngineState) -> Result<(), StorageError>;
    async fn reset_hourly_window(&self) -> Result<(), StorageError>;
    async fn reset_daily_window(&self) -> Result<(), StorageError>;
    async fn reset_weekly_window(&self) -> Result<(), StorageError>;
}
