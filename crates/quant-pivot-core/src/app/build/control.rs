//! Control-factor wiring during infrastructure bootstrap.

use super::types::{BuildRepos, ControlFactorWiring, ControlFactorWiringParts};
use crate::{
    control::{
        ControlFactorRegistry,
        factor_refresher::{FactorRefreshConfig, FactorRefresher},
        factor_shadow::ShadowDecisionWriter,
        factor_snapshot::FactorSnapshotStore,
    },
    observability::metrics_hub::MetricsHub,
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_repository::traits::ControlFactorShadowDecisionRepository;
use parking_lot::Mutex;
use std::sync::Arc;

impl ControlFactorWiring {
    pub(super) async fn wire(
        repos: &BuildRepos,
        metrics: &Arc<MetricsHub>,
        mode: ExecutionMode,
    ) -> OxideResult<Self> {
        let factor_store = Arc::new(FactorSnapshotStore::new(chrono::Utc::now()));
        let shadow_repo_concrete = Arc::clone(repos.fact_data());
        let shadow_repo: Arc<dyn ControlFactorShadowDecisionRepository> = shadow_repo_concrete;
        let (shadow_writer, shadow_writer_task) =
            ShadowDecisionWriter::new(shadow_repo, Arc::clone(metrics));
        let factor_refresher = Arc::new(FactorRefresher::new(
            Arc::clone(repos.control_factor()),
            Arc::clone(&factor_store),
            Arc::clone(metrics),
            FactorRefreshConfig::for_live(mode == ExecutionMode::Live),
        ));
        let factor_registry = Arc::new(
            ControlFactorRegistry::new(
                Arc::clone(repos.control_factor()),
                Arc::clone(repos.runtime_config()),
            )
            .with_snapshot_refresh_notify(factor_refresher.notify_handle()),
        );
        factor_refresher.startup().await?;
        Ok(Self::assembled(ControlFactorWiringParts {
            factor_store,
            factor_refresher,
            factor_registry,
            shadow_writer,
            shadow_writer_task: Mutex::new(Some(shadow_writer_task)),
        }))
    }
}
