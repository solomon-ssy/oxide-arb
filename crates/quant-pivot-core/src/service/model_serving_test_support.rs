//! Complete sealed model-serving artifacts shared by crate-local tests.

use chrono::{TimeZone, Utc};
use quant_pivot_models::{
    domain::quant::ModelVersionInfo,
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::{FactorFamily, FactorNormalization},
        model::ModelFamily,
        quant::{CalibrationKind, DatasetPurpose, PublicationStatus},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        FactorCrossSectionConfig, FactorHeadConfig, ImmutableProfileArtifacts, SellScorerConfig,
    },
    types::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetManifest, DatasetSourceLineage,
        DecisionPolicySnapshotId, ModelInputContract, ModelSpecId, ModelVersionId,
        ReaderContractVersion, ResearchProfileArtifactId, ResearchProfileRef,
        SchemaContractVersion, SchemaVersion, SourceSliceId, SourceSliceManifestRef,
        TrainingDatasetId, builtin_research_profiles,
        factor::{
            FactorAlphaOrientation, FactorComputationContract, FactorDefinitionDocument,
            FactorDefinitionRef, FactorOutputSemantics, FactorServingPlane,
        },
        model_metrics::ModelVersionMetrics,
        model_quality::QualityGateReport,
        model_serving::{
            ModelServingBindings, ModelServingCalibrationArtifactRef, ModelServingContract,
            ModelServingDatasetBinding, ModelServingEstimatorBinding, ModelServingFactorBinding,
            ModelServingModelBinding, ModelServingPolicySnapshotBinding, ModelServingSchemaBinding,
            ModelServingTransformBinding,
        },
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_research::{
    factors::{
        FrozenReferenceQuantiles,
        names::{
            DOMAIN_CRYPTO_STRIKE_PRESSURE, DOMAIN_WEATHER_ENSEMBLE_BIN_PROBABILITY, MOMENTUM_ROC,
        },
    },
    model::{
        HorizonMultipliers, ModelArtifact, ReturnModelSpec, SubstitutionConfidenceRules,
        artifact::{
            ModelPayload, SellEstimatorSpec, SellScorerOutputSpec, SellScorerPayload,
            WeightedFactorModelPayload, model_input_contract_hash,
        },
        factor_heads::FactorHeadSpec,
    },
};

use crate::test_fixtures::model_spec_fixtures::model_spec_lineage_fixture;

fn content_hash(seed: &str) -> ContentHash {
    CanonicalDigest::content_hash_json(&seed).expect("canonical fixture content hash")
}

fn factor_plane(category_scope: Option<MarketCategory>) -> FactorServingPlane {
    let (name, family) = match category_scope {
        Some(MarketCategory::Crypto) => (DOMAIN_CRYPTO_STRIKE_PRESSURE, FactorFamily::DomainCrypto),
        Some(MarketCategory::Weather) => (
            DOMAIN_WEATHER_ENSEMBLE_BIN_PROBABILITY,
            FactorFamily::DomainWeather,
        ),
        _ => (MOMENTUM_ROC, FactorFamily::Momentum),
    };
    let revision = FactorDefinitionRef::try_seal(
        FactorDefinitionDocument {
            name,
            family,
            input_features: Vec::new(),
            output: FactorOutputSemantics::OutcomeAlpha {
                orientation: FactorAlphaOrientation::CanonicalYes,
            },
            normalization: FactorNormalization::Rank,
            owner: "quant-pivot-core-tests".to_owned(),
            required: false,
            computation: FactorComputationContract {
                semantic_version: 1,
                semantic_key: "quant-pivot/model-serving-test@1".to_owned(),
            },
        },
        content_hash("model-serving-test-feature-contract"),
        SchemaVersion::FIRST,
        SchemaVersion::FIRST,
    )
    .expect("valid factor revision");
    FactorServingPlane::try_seal(vec![revision]).expect("valid factor plane")
}

struct FactorPayloadContract {
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
    estimator: ModelServingEstimatorBinding,
    calibration: Option<ModelServingCalibrationArtifactRef>,
}

struct FactorDatasetContract {
    manifest: DatasetManifest,
    manifest_hash: ContentHash,
    required_domain_families: Vec<DomainFamily>,
    capability_registry_hashes: CapabilityRegistryHashes,
    profile_ref: ResearchProfileRef,
    prediction_horizon_secs: u64,
    policy_hash: ContentHash,
}

struct FactorArtifactFixture {
    payload: ModelPayload,
    plane: FactorServingPlane,
    model_family: ModelFamily,
    category_scope: Option<MarketCategory>,
}

impl FactorArtifactFixture {
    fn new(
        payload: ModelPayload,
        plane: FactorServingPlane,
        model_family: ModelFamily,
        category_scope: Option<MarketCategory>,
    ) -> Self {
        Self {
            payload,
            plane,
            model_family,
            category_scope,
        }
    }

    fn payload_contract(&self) -> FactorPayloadContract {
        let (input_contract, input_transform_hash) = match &self.payload {
            ModelPayload::WeightedFactor(weighted) => (
                &weighted.input_contract,
                weighted
                    .input_transform_hash()
                    .expect("weighted transform hash"),
            ),
            ModelPayload::SellScorer(sell_payload) => (
                &sell_payload.input_contract,
                sell_payload
                    .input_transform_hash()
                    .expect("Sell transform hash"),
            ),
            ModelPayload::Classical(_) => {
                panic!("factor fixture cannot seal a classical payload");
            }
        };
        let estimator = self
            .payload
            .serving_estimator_binding(&self.plane)
            .expect("serving estimator");
        let calibration = match &self.payload {
            ModelPayload::WeightedFactor(weighted) => match &weighted.return_model {
                ReturnModelSpec::Heuristic(_) => None,
                ReturnModelSpec::Calibrated(calibrated) => {
                    Some(ModelServingCalibrationArtifactRef {
                        artifact_id: calibrated.calibrator_ref,
                        kind: CalibrationKind::ModelScore,
                        content_hash: content_hash("model-serving-test-calibration"),
                    })
                }
            },
            ModelPayload::SellScorer(_) => None,
            ModelPayload::Classical(_) => {
                panic!("factor fixture cannot seal a classical payload");
            }
        };
        FactorPayloadContract {
            input_contract_hash: model_input_contract_hash(input_contract)
                .expect("input contract hash"),
            input_transform_hash,
            estimator,
            calibration,
        }
    }

    fn dataset_contract(
        &self,
        model_spec_id: ModelSpecId,
        training_dataset_id: TrainingDatasetId,
        feature_schema_hash: ContentHash,
        model_spec_definition_hash: ContentHash,
    ) -> FactorDatasetContract {
        let profile = builtin_research_profiles()
            .expect("built-in profiles")
            .into_iter()
            .find(|profile| profile.spec.category == self.category_scope)
            .expect("category-matched built-in profile");
        let policy_hash = content_hash("model-serving-test-policy");
        let required_domain_families = [DomainFamily::Crypto, DomainFamily::Weather]
            .into_iter()
            .filter(|domain| {
                let family = match domain {
                    DomainFamily::Crypto => FactorFamily::DomainCrypto,
                    DomainFamily::Weather => FactorFamily::DomainWeather,
                };
                self.plane
                    .definitions()
                    .iter()
                    .any(|revision| revision.definition().family == family)
            })
            .collect::<Vec<_>>();
        let capability_registry_hashes = CapabilityRegistryHashes::try_new(
            required_domain_families
                .iter()
                .map(|domain| content_hash(&format!("model-serving-test-{domain:?}-capability")))
                .collect(),
        )
        .expect("canonical capabilities");
        let window_start = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("window start");
        let window_end = Utc
            .with_ymd_and_hms(2026, 1, 2, 0, 0, 0)
            .single()
            .expect("window end");
        let source_lineage = DatasetSourceLineage {
            format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
            source_slice_id: SourceSliceId::from_v7(),
            source_slice_identity_hash: content_hash("model-serving-test-source-slice"),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                &profile.profile_ref,
            ),
            research_program_hash: content_hash("model-serving-test-research-program"),
            source_slice: SourceSliceManifestRef {
                manifest_uri: ArtifactUri::parse(
                    "file://source-slices/model-serving-test-manifest.json",
                )
                .expect("source manifest URI"),
                manifest_hash: content_hash("model-serving-test-source-manifest"),
            },
            source_window_start: window_start,
            source_window_end: window_end,
            pit_cutoff: Utc
                .with_ymd_and_hms(2026, 1, 3, 0, 0, 0)
                .single()
                .expect("PIT cutoff"),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(&policy_hash),
            runtime_config_hash: policy_hash,
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::parse("model_serving_test_schema_v1")
                .expect("schema contract"),
            source_schema_hash: content_hash("model-serving-test-source-schema"),
            capability_registry_hashes: capability_registry_hashes.clone(),
        };
        let manifest = DatasetManifest {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id,
            source_lineage,
            cohort_manifest: None,
            model_spec_id,
            model_family: self.model_family,
            model_spec_definition_hash,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 60,
            sample_interval_secs: 300,
            horizons_secs: vec![900],
            feature_schema_version: SchemaVersion::FIRST,
            feature_schema_hash,
            factor_serving_plane: self.plane.clone(),
            label_schema_hash: content_hash("model-serving-test-label-schema"),
            semantic_dataset_hash: content_hash("model-serving-test-dataset"),
            source_fingerprint: content_hash("model-serving-test-source-fingerprint"),
            sample_count: 128,
        };
        let manifest_hash = manifest.content_hash().expect("dataset manifest hash");
        FactorDatasetContract {
            manifest,
            manifest_hash,
            required_domain_families,
            capability_registry_hashes,
            profile_ref: profile.profile_ref,
            prediction_horizon_secs: profile.spec.target_horizon_secs,
            policy_hash,
        }
    }

    fn seal(self) -> ModelArtifact {
        let feature_schema_hash = content_hash("model-serving-test-feature-contract");
        let model_version_id = ModelVersionId::from_v7();
        let model_spec_id = ModelSpecId::from_v7();
        let payload_contract = self.payload_contract();
        let dataset_contract = self.dataset_contract(
            model_spec_id,
            TrainingDatasetId::from_v7(),
            feature_schema_hash,
            content_hash("model-serving-test-model-spec"),
        );
        let contract = ModelServingContract::try_seal(ModelServingBindings {
            policy_snapshot: ModelServingPolicySnapshotBinding {
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                    &dataset_contract.policy_hash,
                ),
                snapshot_hash: dataset_contract.policy_hash,
                profile_artifacts: ImmutableProfileArtifacts::default()
                    .references()
                    .expect("profile references"),
            },
            required_domain_families: dataset_contract.required_domain_families,
            capability_registry_hashes: dataset_contract.capability_registry_hashes,
            factors: ModelServingFactorBinding {
                plane: self.plane,
                bias_table: None,
            },
            schemas: ModelServingSchemaBinding {
                feature_schema_hash,
                label_schema_hash: content_hash("model-serving-test-label-schema"),
            },
            transform: ModelServingTransformBinding {
                input_contract_hash: payload_contract.input_contract_hash,
                input_transform_hash: payload_contract.input_transform_hash,
                training_input_hash: content_hash("model-serving-test-training-input"),
                training_dataset_hash: content_hash("model-serving-test-dataset"),
            },
            model: ModelServingModelBinding {
                model_version_id,
                model_spec_id,
                model_spec_definition_hash: content_hash("model-serving-test-model-spec"),
                model_family: self.model_family,
                category_scope: self.category_scope,
                profile_ref: dataset_contract.profile_ref,
                prediction_horizon_secs: dataset_contract.prediction_horizon_secs,
                estimator: payload_contract.estimator,
                calibration: payload_contract.calibration,
            },
            trade_policy: None,
            dataset: ModelServingDatasetBinding {
                manifest: dataset_contract.manifest,
                manifest_hash: dataset_contract.manifest_hash,
                artifact_bytes_hash: content_hash("model-serving-test-dataset-bytes"),
            },
        })
        .expect("valid serving contract");
        ModelArtifact::try_seal(contract, self.payload).expect("valid model artifact")
    }
}

pub fn model_artifact(category_scope: Option<MarketCategory>) -> ModelArtifact {
    artifact_with_return(category_scope, ReturnModelSpec::heuristic_default())
}

pub fn artifact_with_return(
    category_scope: Option<MarketCategory>,
    return_model: ReturnModelSpec,
) -> ModelArtifact {
    let plane = factor_plane(category_scope);
    let weighted = WeightedFactorModelPayload {
        factor_head: FactorHeadSpec::from_config(&plane, &FactorHeadConfig::default())
            .expect("valid factor head"),
        input_contract: ModelInputContract::single_required("book.mid"),
        horizon_multipliers: HorizonMultipliers::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model,
        factor_cross_section: FactorCrossSectionConfig::default(),
        frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
    };
    let payload = ModelPayload::WeightedFactor(Box::new(weighted));
    FactorArtifactFixture::new(payload, plane, ModelFamily::WeightedFactor, category_scope).seal()
}

pub fn sell_artifact() -> ModelArtifact {
    let plane = factor_plane(None);
    let config = SellScorerConfig::default();
    let sell = SellScorerPayload {
        factor_head: FactorHeadSpec::from_config(&plane, &FactorHeadConfig::default())
            .expect("valid factor head"),
        estimator: SellEstimatorSpec::try_from(&config).expect("valid Sell estimator"),
        output_spec: SellScorerOutputSpec::try_from(&config).expect("valid Sell output"),
        input_contract: ModelInputContract::single_required("book.mid"),
        factor_cross_section: FactorCrossSectionConfig::default(),
        frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
    };
    FactorArtifactFixture::new(
        ModelPayload::SellScorer(Box::new(sell)),
        plane,
        ModelFamily::HoldVsExitWeighted,
        None,
    )
    .seal()
}

pub fn model_version(
    artifact: &ModelArtifact,
    publication_status: PublicationStatus,
    quality_gate_report: Option<QualityGateReport>,
) -> ModelVersionInfo {
    let serving_contract = artifact.header().serving_contract().clone();
    let bindings = serving_contract.bindings();
    let model = &bindings.model;
    let trade_policy = bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    let (model_spec_thesis, _) = model_spec_lineage_fixture("model-serving-test-spec");
    ModelVersionInfo {
        model_version_id: model.model_version_id,
        model_spec_id: model.model_spec_id,
        model_spec_name: "model-serving-test-spec".to_owned(),
        model_family: model.model_family,
        model_spec_thesis,
        model_spec_definition_hash: model.model_spec_definition_hash,
        model_spec_prediction_horizon_secs: i64::try_from(model.prediction_horizon_secs)
            .expect("horizon fits i64"),
        version: 1,
        artifact_hash: artifact.content_hash().expect("artifact hash"),
        serving_contract_hash: serving_contract.contract_hash(),
        category_scope: model.category_scope,
        profile_ref: model.profile_ref.clone(),
        training_dataset_id: Some(bindings.dataset.manifest.training_dataset_id),
        trade_policy_artifact_id: trade_policy.map(|(artifact_id, _)| artifact_id),
        trade_policy_hash: trade_policy.map(|(_, content_hash)| content_hash),
        publish_path_set_id: None,
        derivation_kind: ModelVersionInfo::training_derivation_kind(),
        parent_model_version_id: None,
        calibration_artifact_id: model
            .calibration
            .as_ref()
            .map(|calibration| calibration.artifact_id),
        derivation_evidence_hash: None,
        metrics: ModelVersionMetrics::not_measured("test fixture"),
        training_objective: ModelTrainingObjective::hand_authored("test fixture"),
        quality_gate_report,
        publication_status,
        published_at: (publication_status == PublicationStatus::Published).then(Utc::now),
        retired_at: None,
        created_at: Utc::now(),
        serving_contract,
    }
}
