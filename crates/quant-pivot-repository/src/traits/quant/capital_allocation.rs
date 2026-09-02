use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::CapitalAllocationInfo,
    types::{OrderIntentId, Usd},
};

/// Capital-allocation read port.
///
/// Capital is **written only as part of an order-intent transition**
/// ([`OrderIntentRepository`](crate::traits::OrderIntentRepository)): the
/// `planned → allocated → locked → spent → released | impaired` FSM is the
/// money truth source and must be atomic with the intent state machine, so the
/// allocation ledger has no standalone write surface. This trait exposes only
/// the reads used outside that transaction (account sizing, recovery gates).
#[async_trait::async_trait]
pub trait CapitalAllocationRepository: Send + Sync {
    /// Load the (1:1) allocation for an intent, if any.
    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<CapitalAllocationInfo>, StorageError>;

    /// Net reserved capital across in-flight allocations (see `ReservedCapitalRepository`).
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError>;

    /// Whether any allocation is in the `Impaired` recovery state.
    ///
    /// A blocking recovery condition: authorization-policy upgrades fail closed while impaired
    /// capital exists (corrupted invariants must be resolved before trading).
    async fn has_impaired(&self) -> Result<bool, StorageError>;
}
