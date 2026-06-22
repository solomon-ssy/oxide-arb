use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{
    FactorDefinitionInfo, FactorValueInfo, NewFactorDefinition, NewFactorValue,
};
use quant_pivot_models::types::{FactorDefinitionId, ModelRunId};

/// Factor definition and value persistence port.
#[async_trait::async_trait]
pub trait FactorRepository: Send + Sync {
    async fn create_definition(
        &self,
        definition: NewFactorDefinition,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    async fn create_values(
        &self,
        values: Vec<NewFactorValue>,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;

    async fn find_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<Option<FactorDefinitionInfo>, StorageError>;

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;
}
