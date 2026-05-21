use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::accounting_period;

pub trait AccountingRepository: Send + Sync {
    async fn get_current_daily(&self) -> Result<Option<accounting_period::Model>, StorageError>;
    async fn get_current_weekly(&self) -> Result<Option<accounting_period::Model>, StorageError>;
    async fn create_period(
        &self,
        period: accounting_period::ActiveModel,
    ) -> Result<accounting_period::Model, StorageError>;
    async fn update_period(
        &self,
        period: accounting_period::ActiveModel,
    ) -> Result<accounting_period::Model, StorageError>;
    async fn finalize_period(&self, period_id: &str) -> Result<(), StorageError>;
    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<accounting_period::Model>, StorageError>;
}
