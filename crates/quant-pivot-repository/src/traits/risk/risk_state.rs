use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{RiskStateInfo, UpsertRiskEngineState};

#[async_trait::async_trait]
pub trait RiskStateRepository: Send + Sync {
    async fn load(&self) -> Result<RiskStateInfo, StorageError>;
    async fn upsert(&self, state: UpsertRiskEngineState) -> Result<(), StorageError>;
    async fn reset_hourly_window(&self) -> Result<(), StorageError>;
    async fn reset_daily_window(&self) -> Result<(), StorageError>;
    async fn reset_weekly_window(&self) -> Result<(), StorageError>;
}
