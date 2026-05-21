use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::risk_state;

pub trait RiskStateRepository: Send + Sync {
    async fn load(&self) -> Result<risk_state::Model, StorageError>;
    async fn save(&self, state: risk_state::ActiveModel) -> Result<(), StorageError>;
    async fn reset_hourly_window(&self) -> Result<(), StorageError>;
    async fn reset_daily_window(&self) -> Result<(), StorageError>;
    async fn reset_weekly_window(&self) -> Result<(), StorageError>;
}
