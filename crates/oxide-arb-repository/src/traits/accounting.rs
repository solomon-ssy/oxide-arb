use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{AccountingPeriodInfo, NewAccountingPeriod, UpdateAccountingPeriod},
    types::PeriodId,
};

pub trait AccountingRepository: Send + Sync {
    async fn get_current_daily(&self) -> Result<Option<AccountingPeriodInfo>, StorageError>;
    async fn get_current_weekly(&self) -> Result<Option<AccountingPeriodInfo>, StorageError>;

    async fn create(
        &self,
        period: NewAccountingPeriod,
    ) -> Result<AccountingPeriodInfo, StorageError>;

    async fn update(
        &self,
        period_id: &PeriodId,
        update: UpdateAccountingPeriod,
    ) -> Result<AccountingPeriodInfo, StorageError>;

    async fn finalize_period(&self, period_id: &PeriodId) -> Result<(), StorageError>;

    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<AccountingPeriodInfo>, StorageError>;
}
