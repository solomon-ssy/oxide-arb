//! Admin port for the live neg-risk structural monitor (Phase 11.2.1).

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::domain::NegRiskEventDriftView;

/// Live read of the `struct.negrisk_leg_sum_drift` signal at the event level.
///
/// Backed by the in-memory `MarketRegistry` + `BookStore`; computes the
/// best-ask sum across each active neg-risk event's YES legs (which should be
/// ≈ 1) so operators can spot structural mispricing without a persisted fact.
#[async_trait]
pub trait StructuralMonitorPort: Send + Sync {
    /// Snapshot the neg-risk leg-sum drift across all active neg-risk events,
    /// ordered by descending absolute drift (most mispriced first).
    async fn negrisk_events(&self) -> QuantResult<Vec<NegRiskEventDriftView>>;
}
