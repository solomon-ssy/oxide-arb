//! Research plane bundle (Phase 3+): artifacts, selection, candidate projection.

use crate::{
    observability::fact_lag::FactLagTracker,
    pipeline::{
        book_store::BookStore, market_candidate_provider::MarketCandidateProvider,
        market_registry::MarketRegistry,
    },
};
use quant_pivot_models::config::DeployConfig;
use quant_pivot_repository::{
    postgres::PgMarketSelectionRepository, traits::MarketSelectionRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactStore, LocalArtifactStore},
    selection::{ConfiguredMarketSelector, MarketSelector},
};
use quant_pivot_storage::postgres::PostgresPool;
use std::sync::Arc;

/// Research plane: artifact store and compute contracts (Phase 3+).
pub struct ResearchBundle {
    /// Local (or future object-store) backend for dataset / model artifact bytes.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Pure, config-driven market selector (3.1).
    pub market_selector: Arc<dyn MarketSelector>,
    /// Persistence port for selection snapshots and their members (3.1).
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    /// Core-side projector freezing market facts into selector inputs (3.1).
    pub candidate_provider: Arc<MarketCandidateProvider>,
}

impl ResearchBundle {
    /// Build the research bundle from deploy config plus the live data-plane
    /// handles the candidate projector needs.
    ///
    /// No report scheduler or trigger is wired here — periodic report generation
    /// is a Phase 4 concern. This only assembles the selection building blocks.
    pub fn assemble(
        deploy: &DeployConfig,
        registry: Arc<MarketRegistry>,
        book_store: Arc<BookStore>,
        fact_lag: Arc<FactLagTracker>,
        pg: &PostgresPool,
    ) -> Self {
        let artifact_store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(
            deploy.research.artifact_root.clone(),
        ));
        let market_selector: Arc<dyn MarketSelector> = Arc::new(ConfiguredMarketSelector::new());
        let market_selection_repo: Arc<dyn MarketSelectionRepository> =
            Arc::new(PgMarketSelectionRepository::new(pg.connection().clone()));
        let candidate_provider =
            Arc::new(MarketCandidateProvider::new(registry, book_store, fact_lag));
        Self {
            artifact_store,
            market_selector,
            market_selection_repo,
            candidate_provider,
        }
    }
}
