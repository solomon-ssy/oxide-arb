use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        FactorDefinitionInfo, FactorDefinitionListQuery, FactorValueInfo, NewFactorDefinition,
        NewFactorValue, Paginated,
    },
    types::{FactorDefinitionId, ModelRunId},
};

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

    async fn find_definitions_by_ids(
        &self,
        factor_definition_ids: &[FactorDefinitionId],
    ) -> Result<Vec<FactorDefinitionInfo>, StorageError>;

    /// Page the factor-definition governance catalog, newest (`created_at`) first.
    async fn page_definitions(
        &self,
        query: FactorDefinitionListQuery,
    ) -> Result<Paginated<FactorDefinitionInfo>, StorageError>;

    async fn publish_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    async fn retire_definition(
        &self,
        factor_definition_id: &FactorDefinitionId,
    ) -> Result<FactorDefinitionInfo, StorageError>;

    async fn list_values_for_run(
        &self,
        model_run_id: &ModelRunId,
    ) -> Result<Vec<FactorValueInfo>, StorageError>;
}
