//! Web-facing feature-integrity read and governed-action port.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::domain::{
    api::{
        AcknowledgeFeatureParityLatchRequest, FeatureIntegrityLatchView,
        FeatureIntegritySummaryView, FeatureParityEventListQuery, FeatureParityEventView,
        FeatureParityRunListQuery, FeatureParityRunView, ResearchJobView,
        RunFullFeatureParityRequest,
    },
    pagination::Paginated,
};

/// Actor provenance captured for governed parity mutations.
#[derive(Debug, Clone)]
pub struct FeatureIntegrityActionContext {
    pub actor: Option<String>,
    pub acting_role: String,
}

/// Dependency-inversion boundary for the Research > Feature Integrity page.
#[async_trait]
pub trait FeatureIntegrityPort: Send + Sync {
    async fn summary(&self) -> QuantResult<FeatureIntegritySummaryView>;

    async fn list_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> QuantResult<Paginated<FeatureParityRunView>>;

    async fn list_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> QuantResult<Paginated<FeatureParityEventView>>;

    async fn request_full_run(
        &self,
        request: RunFullFeatureParityRequest,
        ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<ResearchJobView>;

    async fn acknowledge_latch(
        &self,
        request: AcknowledgeFeatureParityLatchRequest,
        ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<FeatureIntegrityLatchView>;
}
