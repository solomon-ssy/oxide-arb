//! Quant research plane: market selection, feature/factor computation, model
//! runtime, training, backtest, and quality governance.
//!
//! This crate owns every **computation trait** and **compute-domain value type**
//! of the research plane. Persistence DTOs (`*Info` / `New*`), typed IDs, the
//! `enums::quant` lifecycle enums, and the content-addressing newtypes
//! (`ContentHash` / `ArtifactUri` / `SchemaVersion`) live in `quant-pivot-models`;
//! this crate depends on them and maps compute types to persistence rows at
//! explicit boundaries (never by merging the two families).
//!
//! # Module map
//!
//! - [`selection`] / [`features`] / [`factors`] / [`model`] — the **online
//!   closure**: `MarketSelection → FeatureVector → FactorValue → SignalCandidate`.
//! - [`pit`] / [`training`] / [`backtest`] / [`gates`] / [`governance`] — the
//!   **offline closure**: historical point-in-time access, dataset/label
//!   construction, training, backtest, and quality gating.
//! - [`artifact`] — content-addressed artifact storage (`ArtifactStore` +
//!   `LocalArtifactStore`).
//! - [`hashing`] — `ResearchHasher`, the canonical `blake3:` content hasher for
//!   research artifacts (order-independent for sets).
//!
//! # Feature flags
//!
//! The base research build links the pure-Rust numeric stack (`ndarray` /
//! `statrs` / `rayon`) and the required `HiGHS` MILP portfolio solver. `research-jobs`
//! (`S3` / `polars` / `parquet`), `optimize` (`argmin`), and `ml-classical`
//! (`smartcore`) remain independently feature-gated; the production binary
//! chooses its deployment feature set explicitly.

#![deny(unsafe_code)]

use quant_pivot_allocator as _;

mod naming;
mod parallel;
pub mod precision;
pub mod stats;
pub mod structural_volatility;

pub mod artifact;
pub mod attribution;
pub mod backtest;
pub mod domain;
pub mod execution_semantics;
pub mod factors;
pub mod features;
pub mod feedback;
pub mod feedback_comparison;
pub mod feedback_decision;
pub mod feedback_governance;
pub mod feedback_learning;
pub mod feedback_recipe;
pub mod feedback_shadow;
pub mod feedback_shadow_binding;
pub mod gates;
pub mod governance;
pub mod hashing;
pub mod linkage;
pub mod model;
pub mod pit;
pub mod policy_evidence;
pub mod policy_replay;
pub mod policy_validation;
pub mod portfolio;
pub mod selection;
pub mod source_slice;
pub mod trade_tape;
pub mod training;
pub mod validation;
pub mod weather_proxy_validation;

#[cfg(test)]
pub(crate) mod test_support {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            domain::DomainFamily,
            factor::{FactorFamily, FactorNormalization},
            model::ModelFamily,
            quant::{CalibrationKind, DatasetPurpose},
        },
        hashing::CanonicalDigest,
        runtime_config::{FactorCrossSectionConfig, ImmutableProfileArtifacts, SellScorerConfig},
        types::{
            ArtifactUri, CapabilityRegistryHashes, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetManifest, DatasetSourceLineage,
            DecisionPolicySnapshotId, ModelInputContract, ModelSpecId, ModelVersionId,
            ReaderContractVersion, ResearchProfileArtifactId, ResearchProfileRef,
            SchemaContractVersion, SchemaVersion, SourceSliceId, SourceSliceManifestRef,
            TrainingDatasetId, builtin_research_profiles,
            factor::{
                FactorAlphaOrientation, FactorComputationContract, FactorContextEffect,
                FactorDefinitionDocument, FactorDefinitionRef, FactorOutputSemantics,
                FactorServingPlane,
            },
            model_serving::{
                ModelServingBindings, ModelServingCalibrationArtifactRef, ModelServingContract,
                ModelServingDatasetBinding, ModelServingFactorBinding, ModelServingModelBinding,
                ModelServingPolicySnapshotBinding, ModelServingSchemaBinding,
                ModelServingTransformBinding,
            },
            stable_name::FactorName,
        },
    };
    use rust_decimal::Decimal;
    use uuid::Uuid;

    use crate::{
        factors::{
            FrozenReferenceQuantiles,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        model::{
            artifact::{
                HorizonMultipliers, ModelArtifact, ModelPayload, ReturnModelSpec,
                SellEstimatorSpec, SellScorerOutputSpec, SellScorerPayload,
                SubstitutionConfidenceRules, WeightedFactorModelPayload, model_input_contract_hash,
            },
            factor_heads::{AlphaFactorWeight, ContextFactorWeight, FactorHeadSpec},
        },
    };

    /// Deterministic, syntactically valid content hash for semantic test seeds.
    pub fn content_hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("canonical fixture content hash")
    }

    /// Canonical feature-contract hash shared by model test artifacts.
    pub fn feature_contract_hash() -> ContentHash {
        content_hash("model-fixture-feature-contract")
    }

    /// Seal one deterministic factor revision for model tests.
    pub fn factor_revision(
        name: FactorName,
        family: FactorFamily,
        output: FactorOutputSemantics,
    ) -> FactorDefinitionRef {
        FactorDefinitionRef::try_seal(
            FactorDefinitionDocument {
                name,
                family,
                input_features: Vec::new(),
                output,
                normalization: FactorNormalization::Rank,
                owner: "quant-pivot-research-tests".to_owned(),
                required: false,
                computation: FactorComputationContract {
                    semantic_version: 1,
                    semantic_key:
                        "quant-pivot/research-test-factor@1+quant-pivot/factor-normalization-boundary@1"
                            .to_owned(),
                },
            },
            feature_contract_hash(),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
        )
        .expect("valid model fixture factor revision")
    }

    /// A canonical plane with one outcome-alpha factor and one side-neutral
    /// context factor. Definitions are sealed and sorted by the plane owner.
    pub fn weighted_factor_plane() -> FactorServingPlane {
        FactorServingPlane::try_seal(vec![
            factor_revision(
                LIQUIDITY_DEPTH,
                FactorFamily::Liquidity,
                FactorOutputSemantics::Context {
                    effect: FactorContextEffect::HigherIsSupportive,
                },
            ),
            factor_revision(
                MOMENTUM_ROC,
                FactorFamily::Momentum,
                FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::CanonicalYes,
                },
            ),
        ])
        .expect("valid model fixture factor plane")
    }

    /// Build a complete head spec that binds every non-diagnostic plane
    /// revision exactly once.
    pub fn factor_head(plane: &FactorServingPlane) -> FactorHeadSpec {
        let alpha = plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().is_outcome_alpha())
            .collect::<Vec<_>>();
        let contexts = plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().is_context())
            .collect::<Vec<_>>();
        let alpha_weight =
            Decimal::ONE / Decimal::from(u64::try_from(alpha.len()).expect("alpha count"));
        let context_weight = if contexts.is_empty() {
            Decimal::ZERO
        } else {
            Decimal::ONE / Decimal::from(u64::try_from(contexts.len()).expect("context count"))
        };
        FactorHeadSpec {
            alpha_weights: alpha
                .into_iter()
                .map(|revision| AlphaFactorWeight {
                    factor_definition_id: revision.factor_definition_id(),
                    factor: revision.factor_name().clone(),
                    weight: alpha_weight,
                })
                .collect(),
            context_weights: contexts
                .into_iter()
                .map(|revision| ContextFactorWeight {
                    factor_definition_id: revision.factor_definition_id(),
                    factor: revision.factor_name().clone(),
                    coverage_weight: context_weight,
                    penalty_strength: Decimal::new(5, 1),
                })
                .collect(),
            alpha_deadband: Decimal::ZERO,
        }
    }

    /// Header-free weighted payload fixture.
    pub fn weighted_payload(plane: &FactorServingPlane) -> WeightedFactorModelPayload {
        WeightedFactorModelPayload {
            factor_head: factor_head(plane),
            input_contract: ModelInputContract::single_required("book.mid"),
            horizon_multipliers: HorizonMultipliers::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            factor_cross_section: FactorCrossSectionConfig::default(),
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        }
    }

    /// Header-free Sell payload fixture with the exact canonical four
    /// model-intrinsic inputs.
    pub fn sell_payload(plane: &FactorServingPlane) -> SellScorerPayload {
        let config = SellScorerConfig::default();
        SellScorerPayload {
            factor_head: factor_head(plane),
            estimator: SellEstimatorSpec::try_from(&config)
                .expect("canonical sell estimator fixture"),
            output_spec: SellScorerOutputSpec::try_from(&config)
                .expect("canonical sell output fixture"),
            input_contract: ModelInputContract::single_required("book.mid"),
            factor_cross_section: FactorCrossSectionConfig::default(),
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        }
    }

    struct ModelArtifactFixtureContext {
        feature_hash: ContentHash,
        model_version_id: ModelVersionId,
        model_spec_id: ModelSpecId,
        training_dataset_id: TrainingDatasetId,
        source_slice_id: SourceSliceId,
        profile_ref: ResearchProfileRef,
        runtime_config_hash: ContentHash,
        capability_registry_hashes: CapabilityRegistryHashes,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        pit_cutoff: DateTime<Utc>,
        semantic_dataset_hash: ContentHash,
        model_spec_definition_hash: ContentHash,
        label_schema_hash: ContentHash,
    }

    impl ModelArtifactFixtureContext {
        fn new(plane: &FactorServingPlane) -> Self {
            let profile_ref = builtin_research_profiles()
                .expect("built-in profiles")
                .into_iter()
                .next()
                .expect("built-in profile")
                .profile_ref;
            Self {
                feature_hash: feature_contract_hash(),
                model_version_id: ModelVersionId::new(Uuid::from_u128(
                    0x019a_0000_0000_7000_8000_0000_0000_0001,
                )),
                model_spec_id: ModelSpecId::new(Uuid::from_u128(
                    0x019a_0000_0000_7000_8000_0000_0000_0002,
                )),
                training_dataset_id: TrainingDatasetId::new(Uuid::from_u128(
                    0x019a_0000_0000_7000_8000_0000_0000_0003,
                )),
                source_slice_id: SourceSliceId::new(Uuid::from_u128(
                    0x019a_0000_0000_7000_8000_0000_0000_0004,
                )),
                profile_ref,
                runtime_config_hash: content_hash("model-fixture-runtime-config"),
                capability_registry_hashes: CapabilityRegistryHashes::try_new(
                    required_capabilities(plane),
                )
                .expect("canonical model fixture capabilities"),
                window_start: Utc
                    .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                    .single()
                    .expect("fixture window start"),
                window_end: Utc
                    .with_ymd_and_hms(2026, 1, 2, 0, 0, 0)
                    .single()
                    .expect("fixture window end"),
                pit_cutoff: Utc
                    .with_ymd_and_hms(2026, 1, 3, 0, 0, 0)
                    .single()
                    .expect("fixture PIT cutoff"),
                semantic_dataset_hash: content_hash("model-fixture-semantic-dataset"),
                model_spec_definition_hash: content_hash("model-fixture-model-spec"),
                label_schema_hash: content_hash("model-fixture-label-schema"),
            }
        }

        fn manifest(&self, plane: &FactorServingPlane, family: ModelFamily) -> DatasetManifest {
            let source_lineage = DatasetSourceLineage {
                format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                source_slice_id: self.source_slice_id,
                source_slice_identity_hash: content_hash("model-fixture-source-slice"),
                research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                    &self.profile_ref,
                ),
                research_program_hash: content_hash("model-fixture-research-program"),
                source_slice: SourceSliceManifestRef {
                    manifest_uri: ArtifactUri::parse(
                        "file://source-slices/model-fixture-manifest.json",
                    )
                    .expect("fixture source manifest URI"),
                    manifest_hash: content_hash("model-fixture-source-manifest"),
                },
                source_window_start: self.window_start,
                source_window_end: self.window_end,
                pit_cutoff: self.pit_cutoff,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                    &self.runtime_config_hash,
                ),
                runtime_config_hash: self.runtime_config_hash,
                reader_contract_version: ReaderContractVersion::v1(),
                schema_contract_version: SchemaContractVersion::parse("source_slice_schema_v1")
                    .expect("fixture schema contract"),
                source_schema_hash: content_hash("model-fixture-source-schema"),
                capability_registry_hashes: self.capability_registry_hashes.clone(),
            };
            DatasetManifest {
                format_version: DATASET_ARTIFACT_FORMAT_VERSION,
                training_dataset_id: self.training_dataset_id,
                source_lineage,
                cohort_manifest: None,
                model_spec_id: self.model_spec_id,
                model_family: family,
                model_spec_definition_hash: self.model_spec_definition_hash,
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                window_start: self.window_start,
                window_end: self.window_end,
                purpose: DatasetPurpose::Training,
                knowledge_lag_secs: 60,
                sample_interval_secs: 300,
                horizons_secs: vec![900],
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: self.feature_hash,
                factor_serving_plane: plane.clone(),
                label_schema_hash: self.label_schema_hash,
                semantic_dataset_hash: self.semantic_dataset_hash,
                source_fingerprint: content_hash("model-fixture-source-fingerprint"),
                sample_count: 128,
            }
        }

        fn seal(
            self,
            payload: ModelPayload,
            plane: FactorServingPlane,
            family: ModelFamily,
            training_input_hash: ContentHash,
            prediction_horizon_secs: u64,
        ) -> ModelArtifact {
            let manifest = self.manifest(&plane, family);
            let manifest_hash = manifest.content_hash().expect("fixture manifest hash");
            let estimator = payload
                .serving_estimator_binding(&plane)
                .expect("fixture estimator binding");
            let (input_contract, input_transform_hash, calibration) = match &payload {
                ModelPayload::WeightedFactor(weighted) => (
                    &weighted.input_contract,
                    weighted
                        .input_transform_hash()
                        .expect("weighted fixture transform hash"),
                    match &weighted.return_model {
                        ReturnModelSpec::Heuristic(_) => None,
                        ReturnModelSpec::Calibrated(calibrated) => {
                            Some(ModelServingCalibrationArtifactRef {
                                artifact_id: calibrated.calibrator_ref,
                                kind: CalibrationKind::ModelScore,
                                content_hash: content_hash("model-fixture-calibrator"),
                            })
                        }
                    },
                ),
                ModelPayload::SellScorer(sell_payload) => (
                    &sell_payload.input_contract,
                    sell_payload
                        .input_transform_hash()
                        .expect("sell fixture transform hash"),
                    None,
                ),
                ModelPayload::Classical(classical) => (
                    &classical.input_contract,
                    classical
                        .input_transform
                        .transform_hash()
                        .expect("classical fixture transform hash"),
                    None,
                ),
            };
            let contract = ModelServingContract::try_seal(ModelServingBindings {
                policy_snapshot: ModelServingPolicySnapshotBinding {
                    decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                        &self.runtime_config_hash,
                    ),
                    snapshot_hash: self.runtime_config_hash,
                    profile_artifacts: ImmutableProfileArtifacts::default()
                        .references()
                        .expect("fixture profile references"),
                },
                required_domain_families: required_domain_families(&plane),
                capability_registry_hashes: self.capability_registry_hashes,
                factors: ModelServingFactorBinding {
                    plane,
                    bias_table: None,
                },
                schemas: ModelServingSchemaBinding {
                    feature_schema_hash: self.feature_hash,
                    label_schema_hash: self.label_schema_hash,
                },
                transform: ModelServingTransformBinding {
                    input_contract_hash: model_input_contract_hash(input_contract)
                        .expect("fixture input contract hash"),
                    input_transform_hash,
                    training_input_hash,
                    training_dataset_hash: self.semantic_dataset_hash,
                },
                model: ModelServingModelBinding {
                    model_version_id: self.model_version_id,
                    model_spec_id: self.model_spec_id,
                    model_spec_definition_hash: self.model_spec_definition_hash,
                    model_family: family,
                    category_scope: None,
                    profile_ref: self.profile_ref,
                    prediction_horizon_secs,
                    estimator,
                    calibration,
                },
                trade_policy: None,
                dataset: ModelServingDatasetBinding {
                    manifest,
                    manifest_hash,
                    artifact_bytes_hash: content_hash("model-fixture-dataset-bytes"),
                },
            })
            .expect("valid fixture serving contract");
            ModelArtifact::try_seal(contract, payload).expect("valid sealed model fixture")
        }
    }

    /// Seal a payload into the exact immutable serving contract required by
    /// production deserialization and runtime loading.
    pub fn seal_model_payload(
        payload: ModelPayload,
        plane: FactorServingPlane,
        family: ModelFamily,
    ) -> ModelArtifact {
        ModelArtifactFixtureContext::new(&plane).seal(
            payload,
            plane,
            family,
            content_hash("model-fixture-training-input"),
            900,
        )
    }

    /// Seal a model payload while binding the exact estimator-ready training
    /// input produced by the training invocation under test.
    #[cfg(feature = "ml-classical")]
    pub fn seal_model_training_payload(
        payload: ModelPayload,
        plane: FactorServingPlane,
        family: ModelFamily,
        training_input_hash: ContentHash,
        prediction_horizon_secs: u64,
    ) -> ModelArtifact {
        ModelArtifactFixtureContext::new(&plane).seal(
            payload,
            plane,
            family,
            training_input_hash,
            prediction_horizon_secs,
        )
    }

    impl ModelArtifact {
        /// Deterministic sealed weighted artifact.
        pub(crate) fn weighted_fixture() -> Self {
            let plane = weighted_factor_plane();
            seal_model_payload(
                ModelPayload::WeightedFactor(Box::new(weighted_payload(&plane))),
                plane,
                ModelFamily::WeightedFactor,
            )
        }

        /// Deterministic sealed Sell artifact.
        pub(crate) fn sell_fixture() -> Self {
            let plane = weighted_factor_plane();
            seal_model_payload(
                ModelPayload::SellScorer(Box::new(sell_payload(&plane))),
                plane,
                ModelFamily::HoldVsExitWeighted,
            )
        }
    }

    fn required_domain_families(plane: &FactorServingPlane) -> Vec<DomainFamily> {
        [DomainFamily::Crypto, DomainFamily::Weather]
            .into_iter()
            .filter(|domain| {
                let family = match domain {
                    DomainFamily::Crypto => FactorFamily::DomainCrypto,
                    DomainFamily::Weather => FactorFamily::DomainWeather,
                };
                plane
                    .definitions()
                    .iter()
                    .any(|revision| revision.definition().family == family)
            })
            .collect()
    }

    fn required_capabilities(plane: &FactorServingPlane) -> Vec<ContentHash> {
        required_domain_families(plane)
            .into_iter()
            .enumerate()
            .map(|(index, _)| {
                ContentHash::from_bytes(
                    [u8::try_from(index + 1).expect("small capability index"); 32],
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod acceptance_tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        process::Command,
    };

    /// The default build must not link `Polars`, `SmartCore`, or `Argmin`.
    #[test]
    fn research_default_excludes_deps() {
        let output = Command::new("cargo")
            .args(["tree", "-p", "quant-pivot-research", "--depth", "1"])
            .output()
            .expect("cargo tree must succeed");
        assert!(
            output.status.success(),
            "cargo tree failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
        for forbidden in ["polars", "smartcore", "argmin"] {
            assert!(
                !stdout.contains(forbidden),
                "default build must not list `{forbidden}`:\n{stdout}"
            );
        }
    }

    /// boundary: no `smartcore` concrete type may leak into the
    /// business layers (core / web / models). Inside this crate it may appear
    /// only behind the `ml-classical` adapter / runtime modules.
    #[test]
    fn business_layer_no_type() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let crates = manifest_dir.parent().expect("crates dir");
        for crate_name in ["quant-pivot-core", "quant-pivot-web", "quant-pivot-models"] {
            let src = crates.join(crate_name).join("src");
            assert_no_token(&src, "smartcore::");
        }
        // Within research, the `smartcore::` concrete path is confined to the
        // classical adapter / runtime modules.
        let research_src = manifest_dir.join("src");
        for entry in walk_rs(&research_src) {
            let name = entry.to_string_lossy();
            // Skip the classical modules (where it legitimately lives) and this
            // acceptance file (which names the token in its assertions).
            if name.contains("classical") || name.ends_with("lib.rs") {
                continue;
            }
            let body = fs::read_to_string(&entry).unwrap_or_default();
            assert!(
                !body.contains("smartcore::"),
                "smartcore concrete type leaked into non-classical research file {name}"
            );
        }
    }

    /// Assert no `.rs` file under `dir` mentions `token`.
    fn assert_no_token(dir: &Path, token: &str) {
        for entry in walk_rs(dir) {
            let body = fs::read_to_string(&entry).unwrap_or_default();
            assert!(
                !body.contains(token),
                "`{token}` must not appear in {}",
                entry.to_string_lossy()
            );
        }
    }

    /// Recursively collect `.rs` files under `dir`.
    fn walk_rs(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_rs(&path));
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
        out
    }
}

#[cfg(test)]
mod feature_guard_tests {
    /// The optimizer (`argmin`) and classical-ML (`smartcore`) stacks stay behind
    /// explicit features and must never be linked by default.
    ///
    /// `research-jobs` (S3/polars/parquet) is **intentionally excluded** from this
    /// guard because `quant-pivot-core` enables it to materialize offline
    /// training datasets. Under
    /// `cargo test --workspace` feature unification therefore turns `research-jobs`
    /// on here, which is expected.
    //
    // Gated to the default build: heavy-feature CI jobs legitimately enable
    // `optimize` / `ml-classical`, so the guard only asserts the default build.
    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    use std::hint;

    #[cfg(not(any(feature = "optimize", feature = "ml-classical")))]
    #[test]
    fn default_build_excludes_features() {
        // `black_box` hides the cfg constants from const-eval so this stays a
        // runtime assertion rather than a (clippy-flagged) constant one.
        let heavy = hint::black_box(cfg!(feature = "optimize"))
            || hint::black_box(cfg!(feature = "ml-classical"));
        assert!(!heavy, "default build must exclude argmin / smartcore");
    }
}
