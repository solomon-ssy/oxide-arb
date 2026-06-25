use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{domain::PortfolioPlanInfo, types::PortfolioPlanId};

/// Read-only portfolio-plan access.
///
/// Portfolio plans are written only as part of the report-creation transaction
/// ([`super::RecommendationReportRepository::create_report`]); there is no
/// standalone create.
#[async_trait::async_trait]
pub trait PortfolioPlanRepository: Send + Sync {
    async fn find_by_id(
        &self,
        portfolio_plan_id: &PortfolioPlanId,
    ) -> Result<Option<PortfolioPlanInfo>, StorageError>;
}
