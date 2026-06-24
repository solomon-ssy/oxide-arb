//! Application composition and bootstrap wiring (Phase 0).

use super::{
    AppContext,
    bundles::{
        DataBundle, DataBundleDeps, GovernanceBundle, GovernanceBundleDeps, InfraBundle,
        ResearchBundle, ResearchBundleDeps, RuntimeSnapshot,
    },
};
use crate::observability::metrics_hub::MetricsHub;
use parking_lot::Mutex;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{config::DeployConfig, domain::CoreEventPublisher};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl AppContext {
    /// Build all subsystems from deploy config.
    pub async fn build(
        deploy: Arc<DeployConfig>,
        shutdown: CancellationToken,
    ) -> QuantResult<Self> {
        let metrics = Arc::new(MetricsHub::new());
        let infra = InfraBundle::assemble(&deploy, Arc::clone(&metrics)).await?;
        let runtime = RuntimeSnapshot::bootstrap(&infra.pg).await?;
        let data = DataBundle::assemble(&DataBundleDeps {
            deploy: &deploy,
            shutdown: &shutdown,
            metrics: &metrics,
            infra: &infra,
            runtime: &runtime.config,
        });
        let governance = GovernanceBundle::assemble(GovernanceBundleDeps {
            deploy: &deploy,
            metrics: &metrics,
            infra: &infra,
            data: &data,
            runtime,
        });
        let research = ResearchBundle::assemble(&ResearchBundleDeps {
            deploy: &deploy,
            infra: &infra,
            data: &data,
            governance: &governance,
        });
        let (events, event_rx) = CoreEventPublisher::bounded(4096);

        Ok(Self {
            config: deploy,
            shutdown,
            events,
            event_rx: Mutex::new(Some(event_rx)),
            infra,
            data,
            governance,
            research,
        })
    }
}
