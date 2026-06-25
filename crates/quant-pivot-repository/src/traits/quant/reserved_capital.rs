use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::Usd;

/// Read-only aggregation of capital locked by in-flight order intents.
///
/// Phase 4 reads only: sums the `sizing_plan.suggested_usd` of recommendations
/// bridged by order intents in a locked (pending / approved / submitted) state.
/// The full capital-allocation FSM (planned → spent writes) lands in Phase 5.
#[async_trait::async_trait]
pub trait ReservedCapitalRepository: Send + Sync {
    /// Total reserved USD across locked order intents (zero when none).
    async fn sum_locked_usd(&self) -> Result<Usd, StorageError>;
}
