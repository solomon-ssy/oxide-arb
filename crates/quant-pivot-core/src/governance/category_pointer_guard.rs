//! [`CategoryPointerGuard`]: config-apply-time validation of
//! `model.category_model_pointers`.
//!
//! A configured category route is a governed decision and never falls back to
//! the generic model. This guard runs on every runtime-config activation and
//! rejects the activation outright when a category pointer does not parse,
//! does not resolve to a `Published` registered model version, or resolves to
//! a version whose frozen artifact does not declare the exact category scope.
//! The online runner repeats the same invariant before each round so artifact
//! replacement or registry corruption still fails closed.

use std::sync::Arc;

use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::PublicationStatus},
    runtime_config::ModelConfig,
    types::ModelVersionId,
};
use quant_pivot_repository::traits::ModelRegistryRepository;
use quant_pivot_research::{artifact::ArtifactStore, model::load_hash_verified_artifact};
/// Validates `model.category_model_pointers` against the model registry and
/// the content-addressed artifact store on every runtime-config activation.
pub struct CategoryPointerGuard {
    model_registry: Arc<dyn ModelRegistryRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
}

impl CategoryPointerGuard {
    /// Build the guard over the shared model registry and artifact store.
    #[must_use]
    pub const fn new(
        model_registry: Arc<dyn ModelRegistryRepository>,
        artifact_store: Arc<dyn ArtifactStore>,
    ) -> Self {
        Self {
            model_registry,
            artifact_store,
        }
    }

    /// Validate every configured category pointer.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::Precondition`] naming the first invalid
    /// pointer — malformed ref, unresolvable or non-`Published` version, or a
    /// `category_scope` mismatch with the pointer's own category key.
    pub async fn validate(&self, model: &ModelConfig) -> Result<(), ControlError> {
        for (category, reference) in &model.category_model_pointers {
            let version_id = reference.id.clone();
            let version = self
                .model_registry
                .find_model_version_by_id(&version_id)
                .await
                .map_err(|error| {
                    ControlError::Precondition(format!(
                        "model.category_model_pointers[{category}] load failed: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    ControlError::Precondition(format!(
                        "model.category_model_pointers[{category}] = `{version_id}` not found — \
                         cannot activate a config pinning a nonexistent model version"
                    ))
                })?;
            if version.publication_status != PublicationStatus::Published {
                return Err(ControlError::Precondition(format!(
                    "model.category_model_pointers[{category}] = `{version_id}` is not \
                     Published (status={:?}) — cannot activate a config pinning a \
                     non-production model version to a category route",
                    version.publication_status
                )));
            }
            let artifact = load_hash_verified_artifact(&self.artifact_store, &version)
                .await
                .map_err(|error| {
                    ControlError::Precondition(format!(
                        "model.category_model_pointers[{category}] = `{version_id}` artifact \
                         load failed: {error}"
                    ))
                })?;
            Self::check_scope(*category, &version_id, artifact.category_scope())?;
        }
        Ok(())
    }

    /// Only a `category_scope` exactly matching the pointer's category is
    /// accepted. An unscoped artifact is a generic route, not a category model.
    fn check_scope(
        category: MarketCategory,
        version_id: &ModelVersionId,
        category_scope: Option<MarketCategory>,
    ) -> Result<(), ControlError> {
        if category_scope == Some(category) {
            return Ok(());
        }
        Err(ControlError::Precondition(format!(
            "model.category_model_pointers[{category}] = `{version_id}` has \
             category_scope={category_scope:?}, which disagrees with its own category key \
             `{category}` — a category route must declare \
             category_scope = Some({category})"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        env, process,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        domain::{
            api::{ModelPickerSide, ModelSpecListQuery, ModelVersionListQuery},
            pagination::Paginated,
            quant::{
                ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion,
                PublishedModelCatalogInfo,
            },
        },
        enums::{common::MarketCategory, model::ModelFamily, quant::PublicationStatus},
        runtime_config::{FactorCrossSectionConfig, ModelConfig, ModelVersionRef},
        types::{
            BacktestPathSetId, ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract,
            ModelVersionId, model_metrics::ModelVersionMetrics, model_quality::QualityGateReport,
            model_training::ModelTrainingObjective,
        },
    };
    use quant_pivot_repository::traits::{
        ModelRegistryRepository, PublishModelVersionCommit, PublishModelVersionResult,
    };
    use quant_pivot_research::{
        artifact::{ArtifactStore, LocalArtifactStore},
        factors::{FrozenReferenceQuantiles, names},
        model::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
            SubstitutionConfidenceRules, WeightedFactorModelArtifact, model_input_contract_hash,
        },
    };
    use rust_decimal_macros::dec;

    use super::CategoryPointerGuard;
    use crate::test_fixtures::{
        execution_pg_seed::fixture_profile_ref,
        model_spec_fixtures::{model_spec_lineage_fixture, new_model_spec_fixture},
    };

    struct FakeRegistry {
        version: ModelVersionInfo,
        spec: ModelSpecInfo,
    }

    #[async_trait]
    impl ModelRegistryRepository for FakeRegistry {
        async fn create_model_spec(
            &self,
            _spec: NewModelSpec,
        ) -> Result<ModelSpecInfo, StorageError> {
            unimplemented!()
        }
        async fn find_model_spec_by_id(
            &self,
            model_spec_id: &ModelSpecId,
        ) -> Result<Option<ModelSpecInfo>, StorageError> {
            Ok((self.spec.model_spec_id == *model_spec_id).then(|| self.spec.clone()))
        }
        async fn create_model_version(
            &self,
            _version: NewModelVersion,
        ) -> Result<ModelVersionInfo, StorageError> {
            unimplemented!()
        }
        async fn next_version_for_spec(
            &self,
            _model_spec_id: &ModelSpecId,
        ) -> Result<i32, StorageError> {
            unimplemented!()
        }
        async fn find_model_version_by_id(
            &self,
            model_version_id: &ModelVersionId,
        ) -> Result<Option<ModelVersionInfo>, StorageError> {
            Ok((self.version.model_version_id == *model_version_id).then(|| self.version.clone()))
        }
        async fn page_specs(
            &self,
            _query: ModelSpecListQuery,
        ) -> Result<Paginated<ModelSpecInfo>, StorageError> {
            unimplemented!()
        }
        async fn page_versions(
            &self,
            _query: ModelVersionListQuery,
        ) -> Result<Paginated<ModelVersionInfo>, StorageError> {
            unimplemented!()
        }
        async fn list_published_catalog(
            &self,
            _side: ModelPickerSide,
            _category: Option<MarketCategory>,
        ) -> Result<Vec<PublishedModelCatalogInfo>, StorageError> {
            unimplemented!()
        }
        async fn list_published_for_spec(
            &self,
            _model_spec_id: &ModelSpecId,
        ) -> Result<Vec<ModelVersionInfo>, StorageError> {
            unimplemented!()
        }
        async fn retire_model_version(
            &self,
            _model_version_id: &ModelVersionId,
        ) -> Result<ModelVersionInfo, StorageError> {
            unimplemented!()
        }
        async fn publish_model_version(
            &self,
            _commit: PublishModelVersionCommit<'_>,
        ) -> Result<PublishModelVersionResult, StorageError> {
            unimplemented!()
        }
        async fn promote_model_to_shadow(
            &self,
            _model_version_id: &ModelVersionId,
        ) -> Result<ModelVersionInfo, StorageError> {
            unimplemented!()
        }
        async fn set_quality_gate_report(
            &self,
            _model_version_id: &ModelVersionId,
            _quality_gate_report: QualityGateReport,
        ) -> Result<ModelVersionInfo, StorageError> {
            unimplemented!()
        }
        async fn set_publish_path_set_id(
            &self,
            _model_version_id: &ModelVersionId,
            _publish_path_set_id: Option<BacktestPathSetId>,
        ) -> Result<ModelVersionInfo, StorageError> {
            unimplemented!()
        }
    }

    fn spec() -> ModelSpecInfo {
        let authored = new_model_spec_fixture(
            ModelSpecId::from_v7(),
            "crypto-spec",
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        );
        ModelSpecInfo {
            model_spec_id: authored.model_spec_id,
            name: authored.name,
            model_family: authored.model_family,
            prediction_horizon_secs: authored.prediction_horizon_secs,
            feature_schema_version: authored.feature_schema_version,
            label_schema_version: authored.label_schema_version,
            thesis: authored.thesis,
            input_contract: authored.input_contract,
            training_contract: authored.training_contract,
            definition_hash: authored.definition_hash,
            created_by_user_id: authored.created_by_user_id,
            created_by_label: authored.created_by_label,
            created_by_role: authored.created_by_role,
            reason: authored.reason,
            created_at: Utc::now(),
        }
    }

    fn version(
        model_spec_id: ModelSpecId,
        artifact_hash: ContentHash,
        status: PublicationStatus,
    ) -> ModelVersionInfo {
        let (model_spec_thesis, model_spec_definition_hash) =
            model_spec_lineage_fixture("category-pointer-test-spec");
        ModelVersionInfo {
            model_version_id: ModelVersionId::from_v7(),
            model_spec_id,
            model_spec_name: "category-pointer-test-spec".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            model_spec_thesis,
            model_spec_definition_hash,
            version: 1,
            artifact_hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: None,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation_kind: ModelVersionInfo::training_derivation_kind(),
            parent_model_version_id: None,
            source_backtest_report_id: None,
            calibration_artifact_id: None,
            score_multiplier_calibration_report: None,
            derivation_evidence_hash: None,
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: status,
            published_at: Some(Utc::now()),
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    fn artifact(
        model_version_id: ModelVersionId,
        category_scope: Option<MarketCategory>,
    ) -> ModelArtifact {
        let input_contract = ModelInputContract::single_required("book.mid");
        let input_contract_hash =
            model_input_contract_hash(&input_contract).expect("input contract hash");
        ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
            header: ModelArtifactHeader {
                model_version_id,
                model_spec_definition_hash: spec().definition_hash,
                profile_ref: fixture_profile_ref(),
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64)))
                    .expect("hash"),
                factor_schema_hash: ContentHash::parse(format!("blake3:{}", "2".repeat(64)))
                    .expect("hash"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            training_dataset_hash: ContentHash::parse(format!("blake3:{}", "3".repeat(64)))
                .expect("hash"),
            training_input_hash: ContentHash::parse(format!("blake3:{}", "4".repeat(64)))
                .expect("hash"),
            input_contract,
            input_contract_hash,
            weights: vec![FactorWeight {
                factor: names::LIQUIDITY_DEPTH,
                weight: dec!(1),
            }],
            prediction_horizon_secs: 86_400,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            factor_cross_section: FactorCrossSectionConfig::default(),
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
            objective_report: None,
            category_scope,
        }))
    }

    fn temp_store() -> Arc<dyn ArtifactStore> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let root = env::temp_dir().join(format!(
            "qp_category_pointer_guard_test_{}_{}_{}",
            process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        Arc::new(LocalArtifactStore::new(root))
    }

    async fn seeded_guard(
        category_scope: Option<MarketCategory>,
        status: PublicationStatus,
    ) -> (CategoryPointerGuard, ModelVersionId) {
        let spec = spec();
        let store = temp_store();
        let model_version_id = ModelVersionId::from_v7();
        let artifact = artifact(model_version_id.clone(), category_scope);
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");
        let version = version(spec.model_spec_id.clone(), digest, status);
        let version_id = version.model_version_id.clone();
        let registry = Arc::new(FakeRegistry { version, spec });
        (CategoryPointerGuard::new(registry, store), version_id)
    }

    fn config_with(category: MarketCategory, version_id: &ModelVersionId) -> ModelConfig {
        let mut category_model_pointers = BTreeMap::new();
        category_model_pointers.insert(category, ModelVersionRef::new(version_id.clone()));
        ModelConfig {
            category_model_pointers,
            ..ModelConfig::default()
        }
    }

    #[tokio::test]
    async fn correctly_scoped_pointer_is_accepted() {
        let (guard, version_id) =
            seeded_guard(Some(MarketCategory::Crypto), PublicationStatus::Published).await;
        let config = config_with(MarketCategory::Crypto, &version_id);
        assert!(guard.validate(&config).await.is_ok());
    }

    #[tokio::test]
    async fn generic_unscoped_artifact_is_rejected_as_a_category_pointer() {
        let (guard, version_id) = seeded_guard(None, PublicationStatus::Published).await;
        let config = config_with(MarketCategory::Crypto, &version_id);
        let error = guard.validate(&config).await.expect_err("must reject");
        assert!(error.to_string().contains("category_scope=None"));
    }

    #[tokio::test]
    async fn mismatched_scope_is_rejected() {
        let (guard, version_id) =
            seeded_guard(Some(MarketCategory::Sports), PublicationStatus::Published).await;
        let config = config_with(MarketCategory::Crypto, &version_id);
        let error = guard.validate(&config).await.expect_err("must reject");
        assert!(error.to_string().contains("category_scope"));
    }

    #[tokio::test]
    async fn non_published_version_is_rejected() {
        let (guard, version_id) =
            seeded_guard(Some(MarketCategory::Crypto), PublicationStatus::Candidate).await;
        let config = config_with(MarketCategory::Crypto, &version_id);
        let error = guard.validate(&config).await.expect_err("must reject");
        assert!(error.to_string().contains("not Published"));
    }

    #[tokio::test]
    async fn nonexistent_version_is_rejected() {
        let (guard, _version_id) =
            seeded_guard(Some(MarketCategory::Crypto), PublicationStatus::Published).await;
        let config = config_with(MarketCategory::Crypto, &ModelVersionId::from_v7());
        let error = guard.validate(&config).await.expect_err("must reject");
        assert!(error.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn empty_pointers_are_always_accepted() {
        let (guard, _version_id) =
            seeded_guard(Some(MarketCategory::Crypto), PublicationStatus::Published).await;
        assert!(guard.validate(&ModelConfig::default()).await.is_ok());
    }
}
