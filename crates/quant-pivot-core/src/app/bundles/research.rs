//! Research plane bundle (Phase 3+): artifacts, selection, feature pipeline.

use super::{DataBundle, InfraBundle};
use crate::{
    pipeline::{
        feature_window_provider::FeatureWindowProvider,
        market_candidate_provider::MarketCandidateProvider,
    },
    service::feature_pipeline::FeaturePipelineService,
};
use quant_pivot_models::config::DeployConfig;
use quant_pivot_repository::{
    postgres::{PgFeatureRepository, PgMarketSelectionRepository},
    traits::{FeatureRepository, MarketSelectionRepository},
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    selection::{ConfiguredMarketSelector, MarketSelector},
};
use std::sync::Arc;

/// Dependencies required to assemble the research plane after infra + data.
pub struct ResearchBundleDeps<'a> {
    /// Deploy-time configuration (artifact root, etc.).
    pub deploy: &'a DeployConfig,
    /// Persistence and analytics handles.
    pub infra: &'a InfraBundle,
    /// Live data plane (books, registry, PIT source for online feature builds).
    pub data: &'a DataBundle,
}

/// Research plane: selection, feature pipeline, and artifact store (Phase 3+).
pub struct ResearchBundle {
    /// Local (or future object-store) backend for dataset / model artifact bytes.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Pure, config-driven market selector (3.1).
    pub market_selector: Arc<dyn MarketSelector>,
    /// Persistence port for selection snapshots and their members (3.1).
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    /// Core-side projector freezing market facts into selector inputs (3.1).
    pub candidate_provider: Arc<MarketCandidateProvider>,
    /// Postgres persistence for feature vectors (3.2).
    pub feature_repo: Arc<dyn FeatureRepository>,
    /// Online feature build loop: resolve → build → persist → emit (3.2).
    pub feature_pipeline: FeaturePipelineService,
}

impl ResearchBundle {
    /// Build the research bundle from deploy config plus wired infra/data handles.
    ///
    /// No report scheduler or trigger is wired here — periodic report generation
    /// is a Phase 4 concern. The feature pipeline is ready for on-demand invocation
    /// with a frozen runtime-config snapshot per round.
    #[must_use]
    pub fn assemble(deps: &ResearchBundleDeps<'_>) -> Self {
        let artifact_store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            deps.deploy.research.artifact_root.clone(),
        ));
        let market_selector: Arc<dyn MarketSelector> = Arc::new(ConfiguredMarketSelector::new());
        let market_selection_repo: Arc<dyn MarketSelectionRepository> = Arc::new(
            PgMarketSelectionRepository::new(deps.infra.pg.connection().clone()),
        );
        let candidate_provider = Arc::new(MarketCandidateProvider::new(
            Arc::clone(&deps.data.market_registry),
            Arc::clone(&deps.data.book_store),
            Arc::clone(&deps.infra.fact_lag_tracker),
        ));
        let feature_repo: Arc<dyn FeatureRepository> =
            Arc::new(PgFeatureRepository::new(deps.infra.pg.connection().clone()));
        let feature_pipeline = FeaturePipelineService::new(
            FeatureWindowProvider::new(Arc::clone(&deps.infra.quant_fact_read)),
            Arc::clone(&feature_repo),
            Arc::clone(&deps.infra.feature_event_writer),
        );

        Self {
            artifact_store,
            market_selector,
            market_selection_repo,
            candidate_provider,
            feature_repo,
            feature_pipeline,
        }
    }
}
