//! Production-composition fixture for the contract-keyed serving registry.

use std::sync::Arc;

use quant_pivot_compute::ComputeExecutor;
use quant_pivot_core::service::{
    model_serving_generation::ModelServingGenerationStore,
    model_serving_preimage::{ModelServingPreimageDeps, ModelServingPreimageService},
    model_serving_registry::ModelServingRuntimeRegistry,
    research_readiness::{
        EvidenceAttestor, EvidenceScopeIdentity, ResearchReadinessEvidenceService,
    },
    trade_policy_evidence::{TradePolicyEvidenceVerifier, TradePolicyEvidenceVerifierDeps},
    trade_policy_preimage::{TradePolicyPreimageVerifier, TradePolicyPreimageVerifierDeps},
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::config::ModelServingRegistryConfig;
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgModelRegistryRepository, PgPolicyRepository,
        PgResearchReadinessEvidenceRepository, PgSourceSliceRepository, PgTradePolicyRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        CalibrationArtifactRepository, ModelRegistryRepository, PolicyRepository,
        TradePolicyRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::artifact::ArtifactStore;
use sea_orm::DatabaseConnection;

/// Exact process dependencies for a system-test serving registry.
pub struct ModelServingRegistryFixture {
    pub db: DatabaseConnection,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub evidence_scope: EvidenceScopeIdentity,
    pub evidence_attestor: Option<EvidenceAttestor>,
}

impl ModelServingRegistryFixture {
    /// Assemble the same deep-preimage loader used by production.
    #[must_use]
    pub fn build_preimages(self) -> Arc<ModelServingPreimageService> {
        let model_registry_repo = Arc::new(PgModelRegistryRepository::new(self.db.clone()))
            as Arc<dyn ModelRegistryRepository>;
        let dataset_repo = Arc::new(PgTrainingDatasetRepository::new(self.db.clone()))
            as Arc<dyn TrainingDatasetRepository>;
        let calibration_repo = Arc::new(PgCalibrationArtifactRepository::new(self.db.clone()))
            as Arc<dyn CalibrationArtifactRepository>;
        let trade_policy_repo = Arc::new(PgTradePolicyRepository::new(self.db.clone()))
            as Arc<dyn TradePolicyRepository>;
        let readiness = Arc::new(
            ResearchReadinessEvidenceService::new(
                Arc::new(PgResearchReadinessEvidenceRepository::new(self.db.clone())),
                Arc::clone(&self.artifact_store),
                self.evidence_attestor,
                &self.evidence_scope,
            )
            .expect("model-serving readiness verifier"),
        );
        let evidence = Arc::new(TradePolicyEvidenceVerifier::new(
            TradePolicyEvidenceVerifierDeps {
                artifacts: Arc::clone(&self.artifact_store),
                policies: Arc::clone(&trade_policy_repo),
                readiness,
            },
        ));
        let trade_policy_preimages = Arc::new(TradePolicyPreimageVerifier::new(
            TradePolicyPreimageVerifierDeps {
                trade_policy_repo,
                dataset_repo: Arc::clone(&dataset_repo),
                model_registry_repo: Arc::clone(&model_registry_repo),
                evidence,
            },
        ));
        Arc::new(ModelServingPreimageService::new(ModelServingPreimageDeps {
            compute: Arc::new(ComputeExecutor::new().expect("model-serving compute executor")),
            model_registry_repo,
            dataset_repo,
            source_slice_repo: Arc::new(PgSourceSliceRepository::new(self.db.clone())),
            policy_repo: Arc::new(PgPolicyRepository::new(self.db)),
            calibration_repo,
            trade_policy_preimages,
            artifact_store: Arc::clone(&self.artifact_store),
        }))
    }

    /// Assemble the same deep-preimage loader and bounded registry as production.
    #[must_use]
    pub fn build(self) -> Arc<ModelServingRuntimeRegistry> {
        let serving_preimages = self.build_preimages();
        Arc::new(
            ModelServingRuntimeRegistry::new(
                ModelServingRegistryConfig::default(),
                serving_preimages,
            )
            .expect("model-serving registry"),
        )
    }

    /// Assemble the production registry and bootstrap the exact current policy
    /// generation through the same all-route resolver used by the application.
    pub async fn build_generation(self) -> QuantResult<Arc<ModelServingGenerationStore>> {
        let db = self.db.clone();
        let registry = self.build();
        let model_registry = Arc::new(PgModelRegistryRepository::new(db.clone()))
            as Arc<dyn ModelRegistryRepository>;
        let bundle = PgPolicyRepository::new(db)
            .load_current_bundle()
            .await?
            .ok_or_else(|| {
                QuantError::config("model-serving fixture has no active policy bundle")
            })?;
        ModelServingGenerationStore::bootstrap(model_registry, registry, bundle)
            .await
            .map(Arc::new)
    }
}
