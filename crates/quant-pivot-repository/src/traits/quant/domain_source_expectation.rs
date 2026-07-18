//! Expected domain-source binding repository.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        DomainSourceExpectationInfo, DomainSourceExpectationTransition,
        UpsertDomainSourceExpectation,
    },
    types::DomainSourceExpectationId,
};

#[async_trait::async_trait]
pub trait DomainSourceExpectationRepository: Send + Sync {
    async fn find(
        &self,
        expectation_id: &DomainSourceExpectationId,
    ) -> Result<Option<DomainSourceExpectationInfo>, StorageError>;

    async fn upsert(
        &self,
        expectation: UpsertDomainSourceExpectation,
    ) -> Result<DomainSourceExpectationInfo, StorageError>;

    async fn transition(
        &self,
        transition: DomainSourceExpectationTransition,
    ) -> Result<DomainSourceExpectationInfo, StorageError>;

    async fn list_all(&self) -> Result<Vec<DomainSourceExpectationInfo>, StorageError>;
}
