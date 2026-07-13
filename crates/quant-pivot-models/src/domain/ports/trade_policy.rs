use async_trait::async_trait;
use uuid::Uuid;

use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        FitTradePolicyRequest, Paginated, TradePolicyArtifactInfo, TradePolicyFitPreflightRequest,
        TradePolicyFitPreflightView, TradePolicyListQuery,
    },
    enums::quant::TradePolicyStatus,
    types::TradePolicyArtifactId,
};

#[async_trait]
pub trait TradePolicyPort: Send + Sync {
    async fn preflight(
        &self,
        request: &TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView>;

    async fn fit(&self, request: FitTradePolicyRequest) -> QuantResult<TradePolicyArtifactInfo>;

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>>;

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>>;

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        target: TradePolicyStatus,
        actor_id: Uuid,
        reason: String,
    ) -> QuantResult<TradePolicyArtifactInfo>;
}
