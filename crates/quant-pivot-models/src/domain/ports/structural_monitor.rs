//! Admin port for the Structural Alpha monitor.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::api::{
        ExecutionHistoryCoverageView, NegRiskEventDriftView, ParticipantConcentrationDetailView,
        ParticipantConcentrationSummaryView,
    },
    types::MarketId,
};

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

    /// Snapshot accepted-frontier and quarantine health for exchange history.
    async fn execution_history_coverage(&self) -> QuantResult<ExecutionHistoryCoverageView>;

    /// Cross-market participant concentration summary, most concentrated first.
    async fn participant_concentration(&self) -> QuantResult<ParticipantConcentrationSummaryView>;

    /// Per-market participant concentration detail and top participant breakdown.
    async fn participant_concentration_market(
        &self,
        market_id: &MarketId,
    ) -> QuantResult<Option<ParticipantConcentrationDetailView>>;
}
