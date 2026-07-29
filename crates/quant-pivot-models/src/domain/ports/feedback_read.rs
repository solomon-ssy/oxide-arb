//! Read-only feedback-cycle boundary for the operator workbench.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            DriftReportListQuery, DriftReportView, FeedbackCycleDetailView, FeedbackCycleListQuery,
            FeedbackCycleView, FeedbackOverviewView,
        },
        pagination::Paginated,
    },
    types::FeedbackCycleId,
};

/// Dependency-inversion boundary between HTTP and feedback persistence.
#[async_trait]
pub trait FeedbackReadPort: Send + Sync {
    /// Build one authoritative dashboard snapshot.
    async fn overview(&self) -> QuantResult<FeedbackOverviewView>;

    /// Page feedback cycles newest first.
    async fn list_cycles(
        &self,
        query: FeedbackCycleListQuery,
    ) -> QuantResult<Paginated<FeedbackCycleView>>;

    /// Page immutable drift headers across cycles.
    async fn list_drift_reports(
        &self,
        query: DriftReportListQuery,
    ) -> QuantResult<Paginated<DriftReportView>>;

    /// Load one cycle with its complete immutable evidence timeline.
    async fn get_cycle(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> QuantResult<Option<FeedbackCycleDetailView>>;
}
