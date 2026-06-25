//! [`DefaultModelRuntimeFactory`]: the single place that reads artifact bytes and
//! knows a concrete model type.
//!
//! Loading is fail-closed and money-safe:
//!
//! 1. resolve the content-addressed artifact (`models/<artifact_hash>.json`),
//! 2. recompute its canonical hash and reject a mismatch ([`ResearchError::ArtifactHashMismatch`]),
//! 3. validate the artifact's structural invariants,
//! 4. verify the artifact's schema hashes bind to the **active** feature / factor
//!    schema ([`ResearchError::FeatureSchemaMismatch`] / [`ResearchError::SchemaHashMismatch`]),
//! 5. dispatch on family, building the concrete runtime behind `dyn QuantModelRuntime`.
//!
//! Business layers (the core `ModelRunner`, the Phase 04 report builder) only ever
//! see `dyn QuantModelRuntime` — never a weighted/classical concrete type.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{domain::ModelVersionInfo, types::ContentHash};

#[cfg(not(feature = "ml-classical"))]
use crate::model::artifact::ClassicalModelArtifact;
#[cfg(feature = "ml-classical")]
use crate::model::{artifact::ClassicalModelArtifact, classical_runtime::ClassicalRuntime};
use crate::{
    artifact::ArtifactStore,
    model::{
        artifact::{ModelArtifact, ModelArtifactHeader},
        runtime::{ModelRuntimeFactory, QuantModelRuntime},
        weighted::WeightedFactorRuntime,
    },
};

/// The active feature / factor schema hashes a loaded artifact must bind to.
///
/// Computed per round from the frozen runtime config (the `FeatureSchema` digest
/// and the enabled `FactorSet` digest), so a published model trained against a
/// stale schema is rejected rather than silently scored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSchemaBinding {
    /// Hash of the active feature schema.
    pub feature_schema_hash: ContentHash,
    /// Hash of the active enabled factor set.
    pub factor_schema_hash: ContentHash,
}

/// Loads governed model runtimes from the content-addressed artifact store.
pub struct DefaultModelRuntimeFactory {
    store: Arc<dyn ArtifactStore>,
    binding: ActiveSchemaBinding,
}

/// Builds a schema-bound [`ModelRuntimeFactory`] for one inference round.
///
/// `ActiveSchemaBinding` is computed per round from frozen config, so the core
/// `ModelRunner` holds a builder (not a single long-lived factory).
pub trait ModelRuntimeFactoryBuilder: Send + Sync {
    /// Produce a factory bound to the active feature / factor schema for this round.
    fn build(&self, binding: ActiveSchemaBinding) -> Arc<dyn ModelRuntimeFactory>;
}

/// Default builder backed by the content-addressed [`ArtifactStore`].
pub struct DefaultModelRuntimeFactoryBuilder {
    store: Arc<dyn ArtifactStore>,
}

impl DefaultModelRuntimeFactoryBuilder {
    /// Wrap the shared artifact store used for model bytes.
    #[must_use]
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self { store }
    }
}

impl ModelRuntimeFactoryBuilder for DefaultModelRuntimeFactoryBuilder {
    fn build(&self, binding: ActiveSchemaBinding) -> Arc<dyn ModelRuntimeFactory> {
        Arc::new(DefaultModelRuntimeFactory::new(
            Arc::clone(&self.store),
            binding,
        ))
    }
}

impl DefaultModelRuntimeFactory {
    /// Build a factory bound to the active schema for this round.
    #[must_use]
    pub fn new(store: Arc<dyn ArtifactStore>, binding: ActiveSchemaBinding) -> Self {
        Self { store, binding }
    }

    /// Reject an artifact whose schema hashes do not bind to the active schema,
    /// or whose declared version disagrees with the registry row.
    fn verify_header(
        &self,
        header: &ModelArtifactHeader,
        model_version: &ModelVersionInfo,
    ) -> QuantResult<()> {
        if header.model_version_id != model_version.model_version_id {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "artifact model_version_id {} disagrees with registry row {}",
                    header.model_version_id, model_version.model_version_id
                ),
            }
            .into());
        }
        if header.feature_schema_hash != self.binding.feature_schema_hash {
            return Err(ResearchError::FeatureSchemaMismatch {
                expected: header.feature_schema_hash.as_str().to_owned(),
                actual: self.binding.feature_schema_hash.as_str().to_owned(),
            }
            .into());
        }
        if header.factor_schema_hash != self.binding.factor_schema_hash {
            return Err(ResearchError::SchemaHashMismatch {
                detail: format!(
                    "factor schema hash: artifact `{}`, active `{}`",
                    header.factor_schema_hash, self.binding.factor_schema_hash
                ),
            }
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl ModelRuntimeFactory for DefaultModelRuntimeFactory {
    async fn load(
        &self,
        model_version: &ModelVersionInfo,
    ) -> QuantResult<Box<dyn QuantModelRuntime>> {
        let recorded = &model_version.artifact_hash;
        let key = ModelArtifact::artifact_key(recorded)?;
        let bytes = self.store.get_by_key(&key).await?;
        let artifact = ModelArtifact::from_bytes(&bytes)?;

        let recomputed = artifact.content_hash()?;
        if recomputed != *recorded {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: recorded.as_str().to_owned(),
                actual: recomputed.as_str().to_owned(),
            }
            .into());
        }

        artifact.validate()?;
        self.verify_header(artifact.header(), model_version)?;

        match artifact {
            ModelArtifact::WeightedFactor(weighted) => {
                Ok(Box::new(WeightedFactorRuntime::new(*weighted)?))
            }
            ModelArtifact::Classical(classical) => self.load_classical(*classical).await,
        }
    }
}

impl DefaultModelRuntimeFactory {
    /// Load a classical runtime: fetch the serialized estimator bytes and build
    /// the runtime (crate-version + format checked). Available only when the
    /// `ml-classical` feature is linked; otherwise the family is unavailable.
    #[cfg(feature = "ml-classical")]
    async fn load_classical(
        &self,
        classical: ClassicalModelArtifact,
    ) -> QuantResult<Box<dyn QuantModelRuntime>> {
        let bytes = self.store.get(&classical.serialized_model_uri).await?;
        let runtime = ClassicalRuntime::load(classical, &bytes)?;
        Ok(Box::new(runtime))
    }

    /// Classical family is not linked in this build.
    #[cfg(not(feature = "ml-classical"))]
    #[allow(clippy::unused_async)]
    async fn load_classical(
        &self,
        classical: ClassicalModelArtifact,
    ) -> QuantResult<Box<dyn QuantModelRuntime>> {
        Err(ResearchError::RuntimeUnavailable {
            family: classical.kind.to_string(),
            detail: "classical runtimes require the `ml-classical` build".to_owned(),
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveSchemaBinding, DefaultModelRuntimeFactory};
    use chrono::Utc;
    use quant_pivot_models::{
        domain::ModelVersionInfo,
        enums::quant::ModelPublicationStatus,
        types::{ContentHash, ModelSpecId, ModelVersionId},
    };
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use crate::{
        artifact::{ArtifactStore, LocalArtifactStore},
        factors::names,
        model::{
            artifact::{
                FactorWeight, ModelArtifact, ModelArtifactHeader, ReturnModelSpec,
                ScoreMultiplierSpec, SubstitutionConfidenceRules, WeightedFactorModelArtifact,
            },
            runtime::{ModelFamily, ModelRuntimeFactory},
        },
    };

    use std::fmt::Write;

    fn hash(seed: &str) -> ContentHash {
        let hex = seed
            .bytes()
            .fold(String::with_capacity(seed.len() * 2), |mut acc, byte| {
                let _ = write!(acc, "{byte:02x}");
                acc
            });
        let padded = format!("{hex:0<64}");
        ContentHash::parse(format!("blake3:{}", &padded[..64])).expect("hash")
    }

    fn artifact(version: &ModelVersionId, feature_hash: ContentHash) -> ModelArtifact {
        ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: version.clone(),
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: feature_hash,
                factor_schema_hash: hash("fac"),
            },
            weights: vec![FactorWeight {
                factor: names::LIQUIDITY_DEPTH,
                weight: dec!(1),
            }],
            prediction_horizon_secs: 86_400,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            required_features: Vec::new(),
            objective_report: None,
        }))
    }

    fn version_info(id: &ModelVersionId, artifact_hash: ContentHash) -> ModelVersionInfo {
        ModelVersionInfo {
            model_version_id: id.clone(),
            model_spec_id: ModelSpecId::from_v7(),
            version: 1,
            artifact_hash,
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: ModelPublicationStatus::Published,
            published_at: Some(Utc::now()),
            retired_at: None,
            created_at: Utc::now(),
        }
    }

    fn temp_store() -> Arc<dyn ArtifactStore> {
        let root = std::env::temp_dir().join(format!(
            "qp_factory_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        Arc::new(LocalArtifactStore::new(root))
    }

    fn binding(feature_hash: ContentHash) -> ActiveSchemaBinding {
        ActiveSchemaBinding {
            feature_schema_hash: feature_hash,
            factor_schema_hash: hash("fac"),
        }
    }

    #[tokio::test]
    async fn loads_weighted_runtime_when_consistent() {
        let store = temp_store();
        let version = ModelVersionId::from_v7();
        let feature_hash = hash("feat");
        let artifact = artifact(&version, feature_hash.clone());
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        let factory =
            DefaultModelRuntimeFactory::new(Arc::clone(&store), binding(feature_hash.clone()));
        let runtime = factory
            .load(&version_info(&version, digest))
            .await
            .expect("load");
        assert_eq!(runtime.feature_schema_hash(), feature_hash);
        assert_eq!(runtime.model_version_id(), version);
    }

    #[tokio::test]
    async fn runtime_factory_rejects_artifact_hash_mismatch() {
        let store = temp_store();
        let version = ModelVersionId::from_v7();
        let feature_hash = hash("feat");
        let artifact = artifact(&version, feature_hash.clone());
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        // The registry records a different (wrong) hash than the stored bytes —
        // but the store is keyed by the recorded hash, so a wrong record cannot
        // even resolve the bytes. Store the SAME bytes under the wrong key to
        // exercise the recompute check directly.
        let wrong = hash("dead");
        let wrong_key = ModelArtifact::artifact_key(&wrong).expect("key");
        store
            .put(wrong_key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        let factory = DefaultModelRuntimeFactory::new(Arc::clone(&store), binding(feature_hash));
        let Err(err) = factory.load(&version_info(&version, wrong)).await else {
            panic!("hash mismatch must be rejected");
        };
        assert!(err.to_string().contains("artifact hash mismatch"));
    }

    #[tokio::test]
    async fn runtime_rejects_feature_schema_hash_mismatch() {
        let store = temp_store();
        let version = ModelVersionId::from_v7();
        let artifact = artifact(&version, hash("trained_feat"));
        let digest = artifact.content_hash().expect("hash");
        let key = ModelArtifact::artifact_key(&digest).expect("key");
        store
            .put(key, &artifact.to_bytes().expect("bytes"))
            .await
            .expect("put");

        // Active schema differs from what the artifact was built against.
        let factory =
            DefaultModelRuntimeFactory::new(Arc::clone(&store), binding(hash("active_feat")));
        let Err(err) = factory.load(&version_info(&version, digest)).await else {
            panic!("schema mismatch must be rejected");
        };
        assert!(err.to_string().contains("feature schema hash mismatch"));
    }
}
