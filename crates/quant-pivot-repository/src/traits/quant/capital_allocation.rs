use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{CapitalAllocationInfo, CapitalAllocationPatch, NewCapitalAllocation},
    types::{CapitalAllocationId, OrderIntentId, Usd},
};

/// Capital-allocation FSM persistence port.
#[async_trait::async_trait]
pub trait CapitalAllocationRepository: Send + Sync {
    async fn create(
        &self,
        allocation: NewCapitalAllocation,
    ) -> Result<CapitalAllocationInfo, StorageError>;

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError>;

    async fn transition(
        &self,
        capital_allocation_id: &CapitalAllocationId,
        patch: CapitalAllocationPatch,
    ) -> Result<CapitalAllocationInfo, StorageError>;

    /// Net reserved capital across in-flight allocations (see [`ReservedCapitalRepository`]).
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError>;

    /// Whether any allocation is in the `Impaired` recovery state.
    ///
    /// A blocking recovery condition: mode upgrades fail closed while impaired
    /// capital exists (corrupted invariants must be resolved before trading).
    async fn has_impaired(&self) -> Result<bool, StorageError>;
}
