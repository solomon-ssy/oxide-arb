use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{ModelRunInfo, NewModelRun};
use quant_pivot_models::types::ModelRunId;

/// Model run persistence port (distinct from registry spec/version lifecycle).
#[async_trait::async_trait]
pub trait ModelRunRepository: Send + Sync {
    async fn create(&self, run: NewModelRun) -> Result<ModelRunInfo, StorageError>;

    async fn find_by_id(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Option<ModelRunInfo>, StorageError>;
}
