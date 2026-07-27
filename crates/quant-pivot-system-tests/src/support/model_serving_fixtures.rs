//! Canonical sealed model-serving fixtures for cross-crate system tests.

use std::{
    collections::BTreeMap,
    env,
    future::Future,
    pin::Pin,
    process,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionClock,
        governance::DecisionPolicySnapshotInfo,
        quant::{
            CompleteFeatureParityRun, ModelSpecInfo, ModelVersionInfo, ModelVersionParityEvidence,
            NewFeatureParityRun, NewFrozenModelParitySubject, NewModelVersion, TrainingDatasetInfo,
            TrainingDatasetMaterialization,
        },
    },
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        factor::{FactorFamily, FactorNormalization},
        model::ModelFamily,
        quant::{
            CalibrationKind, DataQualityStatus, DatasetPurpose, FeatureParityRunKind,
            FeatureParityRunStatus, PublicationStatus, TradePolicyStatus, TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{FactorCrossSectionConfig, FactorHeadConfig, SellScorerConfig},
    types::{
        CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId, EventId, FeatureCell,
        FeatureParityRunId, FeatureStaleness, FeatureValue, MarketId, ModelInputContract,
        ModelSpecId, ModelVersionId, Probability, ResearchEvaluationTrack, ResearchProfileRef,
        RoleCode, SchemaVersion, TokenId, TradePolicyArtifactId, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, TrainingSampleSources, Usd,
        builtin_research_profiles,
        factor::{
            FactorAlphaOrientation, FactorComputationContract, FactorDefinitionDocument,
            FactorDefinitionRef, FactorExplanation, FactorOutputSemantics, FactorServingPlane,
        },
        model_metrics::ModelVersionMetrics,
        model_serving::{
            ModelServingBindings, ModelServingCalibrationArtifactRef, ModelServingContract,
            ModelServingDatasetBinding, ModelServingFactorBinding, ModelServingModelBinding,
            ModelServingPolicySnapshotBinding, ModelServingSchemaBinding,
            ModelServingTradePolicyBinding, ModelServingTransformBinding,
        },
        model_training::ModelTrainingObjective,
        stable_name::FactorName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCalibrationArtifactRepository, PgFeatureParityRepository, PgModelRegistryRepository,
        PgPolicyRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        CalibrationArtifactRepository, FeatureParityRepository, ModelRegistryRepository,
        PolicyRepository, PublishFeatureParityPermit, PublishModelVersionCommit,
        TradePolicyRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{FactorEngine, FactorValue, FrozenReferenceQuantiles, NormalizedFactor},
    features::{
        FeatureSchema, FeatureVector,
        names::{book::MID, market::CATEGORY},
    },
    hashing::ResearchHasher,
    model::{
        artifact::{
            HorizonMultipliers, ModelArtifact, ModelPayload, ReturnModelSpec, SellEstimatorSpec,
            SellScorerOutputSpec, SellScorerPayload, SubstitutionConfidenceRules,
            WeightedFactorModelPayload, model_input_contract_hash,
        },
        factor_heads::FactorHeadSpec,
    },
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, POLICY_ENTRY_FILL_RATIO, POLICY_EXIT_FILL_RATIO,
        POLICY_NET_POSITIVE, POLICY_NET_RETURN_BPS, TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_source_fingerprint, label_names_for_sources,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use super::{
    artifact_store::VersionedArtifactStoreFixture,
    policy_fixtures::bootstrap_default_policy_bundle,
    research_fixtures::{
        DatasetLedgerFixture, DatasetLedgerSeed, ReplayableSourceSliceFixture,
        bind_fixture_decision_capture, model_learning_cohort, persist_replayable_source_slice,
        seed_source_manifest,
    },
    seeded_uuid,
};

const MODEL_DATASET_GROUP_SIZE: usize = 20;

/// Inputs for one repository-backed Training Dataset v3 ledger fixture.
pub struct ModelDatasetLedgerSeed {
    pub scope: String,
    pub model_spec_id: ModelSpecId,
    pub model_family: ModelFamily,
    pub model_spec_definition_hash: ContentHash,
    pub factor_serving_plane: FactorServingPlane,
    pub feature_schema_version: SchemaVersion,
    pub feature_schema_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub profile_ref: ResearchProfileRef,
    pub prediction_horizon_secs: u64,
    pub purpose: DatasetPurpose,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub research_program_hash: ContentHash,
    pub sample_count: u64,
    pub decision_interval_secs: u64,
    pub trade_policy: Option<ModelServingTradePolicyBinding>,
}

#[derive(Clone, Copy)]
struct ModelDatasetExampleSet<'a> {
    scope: &'a str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    sample_count: u64,
    decision_interval_secs: u64,
    prediction_horizon_secs: u64,
    category: MarketCategory,
    factor_serving_plane: &'a FactorServingPlane,
    include_trade_policy_labels: bool,
}

#[derive(Clone, Copy)]
struct ModelDatasetExampleSeed<'a> {
    scope: &'a str,
    ordinal: usize,
    decision_at: DateTime<Utc>,
    prediction_horizon_secs: u64,
    category: MarketCategory,
    factor_serving_plane: &'a FactorServingPlane,
    include_trade_policy_labels: bool,
}

/// Repository and object-store owner for a complete Dataset v3 test preimage.
pub struct ModelDatasetLedgerFixture;

impl ModelDatasetLedgerFixture {
    /// Persist canonical Dataset Parquet and Source Slice objects before
    /// committing the matching Ready ledger.
    pub async fn persist(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        seed: ModelDatasetLedgerSeed,
    ) -> QuantResult<TrainingDatasetInfo> {
        let ModelDatasetLedgerSeed {
            scope,
            model_spec_id,
            model_family,
            model_spec_definition_hash,
            factor_serving_plane,
            feature_schema_version,
            feature_schema_hash,
            decision_policy_snapshot_id,
            profile_ref,
            prediction_horizon_secs,
            purpose,
            window_start,
            window_end,
            research_program_hash,
            sample_count,
            decision_interval_secs,
            trade_policy,
        } = seed;
        let sample_sources = TrainingSampleSources::default();
        let label_schema_hash = ResearchHasher::label_schema(&label_names_for_sources(
            sample_sources.as_slice(),
            trade_policy.is_some(),
        ))?;
        if window_start >= window_end || sample_count == 0 {
            return Err(ResearchError::DatasetBuild {
                detail: "model Dataset fixture requires a non-empty bounded window".to_owned(),
            }
            .into());
        }
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("resolve model Dataset ResearchProfile: {detail}"),
            })?;
        if profile.spec.target_horizon_secs != prediction_horizon_secs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model Dataset horizon {prediction_horizon_secs}s differs from profile {}s",
                    profile.spec.target_horizon_secs
                ),
            }
            .into());
        }
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        let examples = Self::examples(ModelDatasetExampleSet {
            scope: &scope,
            window_start,
            window_end,
            sample_count,
            decision_interval_secs,
            prediction_horizon_secs,
            category: profile.spec.category.unwrap_or(MarketCategory::Sports),
            factor_serving_plane: &factor_serving_plane,
            include_trade_policy_labels: trade_policy.is_some(),
        })?;
        let source_window_end = window_end
            .checked_add_signed(Duration::seconds(
                i64::try_from(prediction_horizon_secs).map_err(|error| {
                    ResearchError::DatasetBuild {
                        detail: format!("model Dataset horizon overflow: {error}"),
                    }
                })?,
            ))
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "model Dataset Source Slice terminal bound overflow".to_owned(),
            })?;
        let stored_source = persist_replayable_source_slice(
            store,
            &examples,
            ReplayableSourceSliceFixture {
                profile_ref,
                evaluation_track: if purpose == DatasetPurpose::PolicyFit {
                    profile.spec.activation_eligibility
                } else {
                    ResearchEvaluationTrack::ResearchOnly
                },
                research_program_hash,
                decision_policy_snapshot_id,
                runtime_config_hash: policy.snapshot_hash,
                window_start,
                window_end: source_window_end,
            },
        )
        .await?;
        let source_lineage = seed_source_manifest(db, &stored_source).await?;
        let cohort_manifest = if purpose == DatasetPurpose::Evaluation {
            Some(model_learning_cohort(
                &scope,
                &source_lineage,
                window_start,
                window_end,
                sample_count,
            )?)
        } else {
            None
        };
        let semantic_dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id: &model_spec_id,
                model_family,
                window_start,
                window_end,
                purpose,
                feature_schema_hash: &feature_schema_hash,
                factor_serving_plane: &factor_serving_plane,
                label_schema_hash: &label_schema_hash,
            },
            &examples,
        )?;
        let source_fingerprint = dataset_source_fingerprint(&examples)?;
        let training_dataset_id = TrainingDatasetId::from_v7();
        let mut ledger = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
            training_dataset_id,
            model_spec_id,
            model_family,
            model_spec_definition_hash,
            factor_serving_plane,
            source_lineage,
            cohort_manifest,
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs: 60,
            sample_interval_secs: decision_interval_secs,
            horizons_secs: vec![prediction_horizon_secs],
            feature_schema_version,
            sample_sources: Some(sample_sources),
            feature_schema_hash,
            label_schema_hash,
            semantic_dataset_hash,
            source_fingerprint,
            sample_count,
        })?;
        Self::bind_trade_policy(&mut ledger, trade_policy)?;
        Self::persist_ledger(db, store, ledger, &examples).await
    }

    fn bind_trade_policy(
        ledger: &mut DatasetLedgerFixture,
        trade_policy: Option<ModelServingTradePolicyBinding>,
    ) -> QuantResult<()> {
        let Some(binding) = trade_policy else {
            return Ok(());
        };
        ledger.manifest.trade_policy_artifact_id = Some(binding.artifact_id);
        ledger.manifest.trade_policy_hash = Some(binding.content_hash);
        ledger
            .manifest
            .validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model-serving dataset policy binding is invalid: {error}"),
            })
            .map_err(Into::into)
    }

    async fn persist_ledger(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        ledger: DatasetLedgerFixture,
        examples: &[TrainingExample],
    ) -> QuantResult<TrainingDatasetInfo> {
        let training_dataset_id = ledger.plan.training_dataset_id;
        let bytes = DatasetParquetCodec::encode(examples, &ledger.manifest)?;
        let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let parquet_uri = store
            .put(
                ArtifactKey::new(
                    ArtifactNamespace::Dataset,
                    training_dataset_id.as_uuid().simple().to_string(),
                    "parquet",
                )?,
                &bytes,
            )
            .await?;
        let persisted_bytes = store.get(&parquet_uri).await?;
        if persisted_bytes != bytes {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model Dataset {training_dataset_id} changed during object persistence"
                ),
            }
            .into());
        }
        DatasetParquetCodec::decode_with_manifest(&persisted_bytes)?;
        let repository = PgTrainingDatasetRepository::new(db.clone());
        repository.create_plan(ledger.plan.clone()).await?;
        repository.start_build(&training_dataset_id).await?;
        repository
            .complete_build(
                &training_dataset_id,
                ledger.completion(
                    TrainingDatasetStatus::Ready,
                    artifact_bytes_hash,
                    parquet_uri,
                    ledger.coverage(),
                    None,
                )?,
            )
            .await?;
        repository
            .find_by_id(&training_dataset_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "quant_training_dataset",
                    id: training_dataset_id.to_string(),
                }
                .into()
            })
    }

    fn examples(input: ModelDatasetExampleSet<'_>) -> QuantResult<Vec<TrainingExample>> {
        let ModelDatasetExampleSet {
            scope,
            window_start,
            window_end,
            sample_count,
            decision_interval_secs,
            prediction_horizon_secs,
            category,
            factor_serving_plane,
            include_trade_policy_labels,
        } = input;
        let sample_count =
            usize::try_from(sample_count).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model Dataset sample count overflow: {error}"),
            })?;
        let window_secs = window_end.signed_duration_since(window_start).num_seconds();
        let interval =
            i64::try_from(decision_interval_secs).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model Dataset decision interval overflow: {error}"),
            })?;
        if interval == 0 {
            return Err(ResearchError::DatasetBuild {
                detail: "model Dataset decision interval must be positive".to_owned(),
            }
            .into());
        }
        let terminal_group =
            sample_count
                .checked_sub(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "model Dataset sample count must be positive".to_owned(),
                })?
                / MODEL_DATASET_GROUP_SIZE;
        let required_secs = i64::try_from(terminal_group)
            .ok()
            .and_then(|group| group.checked_mul(interval))
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "model Dataset decision timeline overflow".to_owned(),
            })?;
        if required_secs > window_secs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model Dataset fixture needs at least {required_secs}s for its decision groups"
                ),
            }
            .into());
        }
        let aligned_window_start = DateTime::from_timestamp_millis(window_start.timestamp_millis())
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "model Dataset window cannot be millisecond-aligned".to_owned(),
            })?;
        (0..sample_count)
            .map(|ordinal| {
                Self::example(ModelDatasetExampleSeed {
                    scope,
                    ordinal,
                    decision_at: aligned_window_start
                        + Duration::seconds(
                            i64::try_from(ordinal / MODEL_DATASET_GROUP_SIZE)
                                .map_err(|error| ResearchError::DatasetBuild {
                                    detail: format!(
                                        "model Dataset decision-group ordinal overflow: {error}"
                                    ),
                                })?
                                .checked_mul(interval)
                                .ok_or_else(|| ResearchError::DatasetBuild {
                                    detail: "model Dataset decision offset overflow".to_owned(),
                                })?,
                        ),
                    prediction_horizon_secs,
                    category,
                    factor_serving_plane,
                    include_trade_policy_labels,
                })
            })
            .collect()
    }

    fn example(seed: ModelDatasetExampleSeed<'_>) -> QuantResult<TrainingExample> {
        let ModelDatasetExampleSeed {
            scope,
            ordinal,
            decision_at,
            prediction_horizon_secs,
            category,
            factor_serving_plane,
            include_trade_policy_labels,
        } = seed;
        let strength = Decimal::from(u64::try_from(ordinal % 9 + 1).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("model Dataset strength overflow: {error}"),
            }
        })?) / dec!(10);
        let liquidity = Decimal::from(u64::try_from(ordinal + 1).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("model Dataset liquidity overflow: {error}"),
            }
        })?) * dec!(1000);
        let market_id = MarketId::new(format!("{scope}-market-{ordinal}"));
        let token_id = TokenId::new(
            seeded_uuid(&format!("{scope}:token:{ordinal}"))
                .as_u128()
                .to_string(),
        );
        let secondary_token_id = TokenId::new(
            seeded_uuid(&format!("{scope}:token:no:{ordinal}"))
                .as_u128()
                .to_string(),
        );
        let factor_values = Self::factor_values(factor_serving_plane, strength)?;
        let horizon =
            Duration::seconds(i64::try_from(prediction_horizon_secs).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("model Dataset label horizon overflow: {error}"),
                }
            })?);
        let matured_at =
            decision_at
                .checked_add_signed(horizon)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "model Dataset label maturity overflow".to_owned(),
                })?;
        let labels = Self::labels(strength, matured_at, include_trade_policy_labels);
        let mut example = TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            selected_market: SelectedMarket {
                market_id: market_id.clone(),
                event_id: EventId::new(format!("{scope}-event-{ordinal}")),
                category,
                primary_token_id: token_id.clone(),
                secondary_token_id: Some(secondary_token_id),
                liquidity_usd: Some(Usd::new(liquidity)),
                volume_24h_usd: Some(Usd::new(liquidity * dec!(2))),
                source_refs: Vec::new(),
            },
            decision_boundary: DecisionClock::new(60).boundary(decision_at)?,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id,
                token_id: Some(token_id),
                decision_at,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::from([
                    (
                        MID,
                        FeatureCell::observed(
                            FeatureValue::Probability(Probability::new(dec!(0.5))),
                            None,
                            FeatureStaleness::Unknown,
                        ),
                    ),
                    (
                        CATEGORY,
                        FeatureCell::observed(
                            FeatureValue::Category(category),
                            None,
                            FeatureStaleness::Unknown,
                        ),
                    ),
                ]),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values,
            labels,
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        };
        bind_fixture_decision_capture(&mut example);
        Ok(example)
    }

    fn factor_values(
        plane: &FactorServingPlane,
        strength: Decimal,
    ) -> QuantResult<Vec<FactorValue>> {
        plane
            .definitions()
            .iter()
            .map(|revision| {
                let definition = revision.definition();
                let direction = definition.contribution_direction(strength).ok_or_else(|| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "model Dataset factor `{}` cannot project fixture strength",
                            definition.name
                        ),
                    }
                })?;
                Ok(FactorValue {
                    definition_id: revision.factor_definition_id(),
                    name: definition.name.clone(),
                    family: definition.family,
                    raw_value: Some(strength),
                    normalization: NormalizedFactor::cross_section(Probability::new(strength)),
                    direction,
                    confidence: Probability::ONE,
                    explanation: FactorExplanation {
                        headline: format!("model Dataset fixture {} rank", definition.name),
                        drivers: Vec::new(),
                    },
                    input_feature_refs: definition.input_features.clone(),
                })
            })
            .collect()
    }

    fn labels(
        strength: Decimal,
        matured_at: DateTime<Utc>,
        include_trade_policy: bool,
    ) -> Vec<TrainingLabel> {
        let positive = strength > dec!(0.5);
        let mut labels = vec![TrainingLabel {
            label_name: TOKEN_PAYOUT_RATIO,
            horizon_secs: 0,
            value: if positive {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            is_resolved: true,
            matured_at,
        }];
        if include_trade_policy {
            labels.extend([
                TrainingLabel {
                    label_name: POLICY_NET_RETURN_BPS,
                    horizon_secs: 0,
                    value: if positive { dec!(25) } else { dec!(-25) },
                    is_resolved: true,
                    matured_at,
                },
                TrainingLabel {
                    label_name: POLICY_NET_POSITIVE,
                    horizon_secs: 0,
                    value: if positive {
                        Decimal::ONE
                    } else {
                        Decimal::ZERO
                    },
                    is_resolved: true,
                    matured_at,
                },
                TrainingLabel {
                    label_name: POLICY_ENTRY_FILL_RATIO,
                    horizon_secs: 0,
                    value: Decimal::ONE,
                    is_resolved: true,
                    matured_at,
                },
                TrainingLabel {
                    label_name: POLICY_EXIT_FILL_RATIO,
                    horizon_secs: 0,
                    value: Decimal::ONE,
                    is_resolved: true,
                    matured_at,
                },
            ]);
        }
        labels.sort_unstable_by(|left, right| {
            left.label_name
                .cmp(&right.label_name)
                .then_with(|| left.horizon_secs.cmp(&right.horizon_secs))
        });
        labels
    }

    /// Allocate an isolated local object store for repository-only fixtures.
    #[must_use]
    pub fn local_store() -> Arc<dyn ArtifactStore> {
        static STORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

        let inner: Arc<dyn ArtifactStore> =
            Arc::new(LocalArtifactStore::new(env::temp_dir().join(format!(
                "qp_model_fixture_{}_{}_{}",
                process::id(),
                Utc::now().timestamp_nanos_opt().unwrap_or_default(),
                STORE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ))));
        Arc::new(VersionedArtifactStoreFixture::new(inner))
    }
}

/// Header-free model-payload constructors shared by serving fixtures.
pub struct ModelPayloadFixture;

impl ModelPayloadFixture {
    /// Build a weighted payload from the exact revision-bound factor plane.
    pub fn weighted(
        plane: &FactorServingPlane,
        factor_head: &FactorHeadConfig,
        input_contract: ModelInputContract,
        return_model: ReturnModelSpec,
        factor_cross_section: FactorCrossSectionConfig,
    ) -> QuantResult<ModelPayload> {
        Ok(ModelPayload::WeightedFactor(Box::new(
            WeightedFactorModelPayload {
                factor_head: FactorHeadSpec::from_config(plane, factor_head)?,
                input_contract,
                horizon_multipliers: HorizonMultipliers::conservative(),
                substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
                return_model,
                factor_cross_section,
                frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
            },
        )))
    }

    /// Build a Hold-vs-Exit payload with the exact four intrinsic inputs.
    pub fn sell(
        plane: &FactorServingPlane,
        factor_head: &FactorHeadConfig,
        sell_scorer: &SellScorerConfig,
        input_contract: ModelInputContract,
        factor_cross_section: FactorCrossSectionConfig,
    ) -> QuantResult<ModelPayload> {
        Ok(ModelPayload::SellScorer(Box::new(SellScorerPayload {
            factor_head: FactorHeadSpec::from_config(plane, factor_head)?,
            estimator: SellEstimatorSpec::try_from(sell_scorer)?,
            output_spec: SellScorerOutputSpec::try_from(sell_scorer)?,
            input_contract,
            factor_cross_section,
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        })))
    }

    /// Seal one deterministic `OutcomeAlpha` plane for narrow governance fixtures.
    pub fn single_alpha_plane(
        feature_schema_hash: ContentHash,
        feature_schema_version: SchemaVersion,
        factor: FactorName,
    ) -> QuantResult<FactorServingPlane> {
        let revision = FactorDefinitionRef::try_seal(
            FactorDefinitionDocument {
                name: factor,
                family: FactorFamily::Momentum,
                input_features: Vec::new(),
                output: FactorOutputSemantics::OutcomeAlpha {
                    orientation: FactorAlphaOrientation::CanonicalYes,
                },
                normalization: FactorNormalization::Rank,
                owner: "quant-pivot-system-tests".to_owned(),
                required: false,
                computation: FactorComputationContract {
                    semantic_version: 1,
                    semantic_key: format!(
                        "quant-pivot/system-test-model-alpha-{}@1",
                        feature_schema_hash.hex()
                    ),
                },
            },
            feature_schema_hash,
            feature_schema_version,
            SchemaVersion::FIRST,
        )
        .map_err(|error| ResearchError::InvalidModelArtifact {
            detail: format!("seal model fixture factor revision: {error}"),
        })?;
        FactorServingPlane::try_seal(vec![revision])
            .map_err(|error| ResearchError::InvalidModelArtifact {
                detail: format!("seal model fixture factor plane: {error}"),
            })
            .map_err(Into::into)
    }

    /// Derive the domain capabilities consumed directly by a factor plane.
    #[must_use]
    pub fn required_domains(
        plane: &FactorServingPlane,
        category_scope: Option<MarketCategory>,
    ) -> Vec<DomainFamily> {
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
                    || matches!(
                        (domain, category_scope),
                        (DomainFamily::Crypto, Some(MarketCategory::Crypto))
                            | (DomainFamily::Weather, Some(MarketCategory::Weather))
                    )
            })
            .collect()
    }
}

/// Complete inputs that vary per sealed model artifact.
pub struct ModelArtifactFixtureSeed {
    pub model_version_id: ModelVersionId,
    pub training_dataset_id: TrainingDatasetId,
    pub payload: ModelPayload,
    pub training_input_hash: ContentHash,
    pub category_scope: Option<MarketCategory>,
    pub calibration: Option<ModelServingCalibrationArtifactRef>,
    pub bias_table: Option<ModelServingCalibrationArtifactRef>,
}

/// Sole owner of the canonical Payload → Contract → Artifact fixture chain.
pub struct SealedModelFixture {
    artifact: ModelArtifact,
    artifact_hash: ContentHash,
}

struct ModelArtifactFixtureContext {
    seed: ModelArtifactFixtureSeed,
    dataset: TrainingDatasetInfo,
    spec: ModelSpecInfo,
    policy: DecisionPolicySnapshotInfo,
    prediction_horizon_secs: u64,
    trade_policy: Option<ModelServingTradePolicyBinding>,
}

impl ModelArtifactFixtureContext {
    async fn load(db: &DatabaseConnection, seed: ModelArtifactFixtureSeed) -> QuantResult<Self> {
        let dataset = Self::load_dataset(db, &seed.training_dataset_id).await?;
        let (spec, prediction_horizon_secs) =
            Self::load_spec(db, &dataset, seed.category_scope).await?;
        let policy = Self::load_policy(db, &dataset).await?;
        let trade_policy = Self::trade_policy(&dataset)?;
        Self::verify_bindings(db, &seed, trade_policy.as_ref()).await?;
        Ok(Self {
            seed,
            dataset,
            spec,
            policy,
            prediction_horizon_secs,
            trade_policy,
        })
    }

    async fn load_dataset(
        db: &DatabaseConnection,
        training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<TrainingDatasetInfo> {
        let dataset = PgTrainingDatasetRepository::new(db.clone())
            .find_by_id(training_dataset_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_training_dataset",
                id: training_dataset_id.to_string(),
            })?;
        if dataset.status != TrainingDatasetStatus::Ready {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture dataset {training_dataset_id} is not Ready: {:?}",
                    dataset.status
                ),
            }
            .into());
        }
        Self::materialization(&dataset)?;
        Ok(dataset)
    }

    async fn load_spec(
        db: &DatabaseConnection,
        dataset: &TrainingDatasetInfo,
        category_scope: Option<MarketCategory>,
    ) -> QuantResult<(ModelSpecInfo, u64)> {
        let spec = PgModelRegistryRepository::new(db.clone())
            .find_model_spec(&dataset.model_spec_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_model_spec",
                id: dataset.model_spec_id.to_string(),
            })?;
        let definition_matches = spec.definition_hash == dataset.model_spec_definition_hash;
        if spec.model_family != dataset.model_family || !definition_matches {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture dataset {} drifts from owning spec {}",
                    dataset.training_dataset_id, spec.model_spec_id
                ),
            }
            .into());
        }
        let prediction_horizon_secs =
            u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model fixture spec {} has invalid prediction horizon: {error}",
                        spec.model_spec_id
                    ),
                }
            })?;
        let profile = dataset
            .source_lineage
            .research_profile_artifact_id
            .profile_ref()
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("resolve model fixture ResearchProfile: {detail}"),
            })?;
        if prediction_horizon_secs != profile.spec.target_horizon_secs
            || category_scope != profile.spec.category
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture spec/profile binding drifts: horizon={prediction_horizon_secs}s/{}, category={category_scope:?}/{:?}",
                    profile.spec.target_horizon_secs, profile.spec.category,
                ),
            }
            .into());
        }
        Ok((spec, prediction_horizon_secs))
    }

    async fn load_policy(
        db: &DatabaseConnection,
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<DecisionPolicySnapshotInfo> {
        PgPolicyRepository::new(db.clone())
            .load_snapshot(&dataset.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::NotFound {
                    entity: "decision_policy_snapshot",
                    id: dataset.decision_policy_snapshot_id.to_string(),
                }
                .into()
            })
    }

    fn materialization(
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<TrainingDatasetMaterialization<'_>> {
        dataset.materialization().ok_or_else(|| {
            ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture dataset {} has no complete materialization",
                    dataset.training_dataset_id
                ),
            }
            .into()
        })
    }

    fn trade_policy(
        dataset: &TrainingDatasetInfo,
    ) -> QuantResult<Option<ModelServingTradePolicyBinding>> {
        let materialization = Self::materialization(dataset)?;
        match (
            materialization.manifest.trade_policy_artifact_id,
            materialization.manifest.trade_policy_hash,
        ) {
            (None, None) => Ok(None),
            (Some(artifact_id), Some(content_hash)) => Ok(Some(ModelServingTradePolicyBinding {
                artifact_id,
                content_hash,
            })),
            _ => Err(ResearchError::InvalidModelArtifact {
                detail: "model fixture dataset has an incomplete trade-policy binding".to_owned(),
            }
            .into()),
        }
    }

    async fn verify_bindings(
        db: &DatabaseConnection,
        seed: &ModelArtifactFixtureSeed,
        trade_policy: Option<&ModelServingTradePolicyBinding>,
    ) -> QuantResult<()> {
        if let Some(binding) = trade_policy {
            SealedModelFixture::verify_trade_policy(db, binding).await?;
        }
        if let Some(binding) = &seed.calibration {
            SealedModelFixture::verify_calibration(db, binding, "model-score").await?;
        }
        if let Some(binding) = &seed.bias_table {
            SealedModelFixture::verify_calibration(db, binding, "bias-table").await?;
        }
        Ok(())
    }

    fn payload_transform(&self) -> QuantResult<(ContentHash, ContentHash)> {
        let (input_contract, input_transform_hash) = match &self.seed.payload {
            ModelPayload::WeightedFactor(weighted) => {
                (&weighted.input_contract, weighted.input_transform_hash()?)
            }
            ModelPayload::SellScorer(exit_payload) => (
                &exit_payload.input_contract,
                exit_payload.input_transform_hash()?,
            ),
            ModelPayload::Classical(classical) => (
                &classical.input_contract,
                classical.input_transform.transform_hash()?,
            ),
        };
        if input_contract != &self.spec.input_contract {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture payload input contract drifts from owning spec {}",
                    self.spec.model_spec_id
                ),
            }
            .into());
        }
        Ok((
            model_input_contract_hash(input_contract)?,
            input_transform_hash,
        ))
    }

    fn seal(self) -> QuantResult<SealedModelFixture> {
        let materialization = Self::materialization(&self.dataset)?;
        let profile_artifacts = self
            .policy
            .snapshot
            .profile_artifacts
            .references()
            .map_err(|error| ResearchError::InvalidModelArtifact {
                detail: format!("model fixture profile artifacts are invalid: {error}"),
            })?;
        let estimator = self
            .seed
            .payload
            .serving_estimator_binding(materialization.factor_serving_plane)?;
        let (input_contract_hash, input_transform_hash) = self.payload_transform()?;
        let required_domain_families = ModelPayloadFixture::required_domains(
            materialization.factor_serving_plane,
            self.seed.category_scope,
        );
        let contract = ModelServingContract::try_seal(ModelServingBindings {
            policy_snapshot: ModelServingPolicySnapshotBinding {
                decision_policy_snapshot_id: self.policy.decision_policy_snapshot_id,
                snapshot_hash: self.policy.snapshot_hash,
                profile_artifacts,
            },
            required_domain_families,
            capability_registry_hashes: self
                .dataset
                .source_lineage
                .capability_registry_hashes
                .clone(),
            factors: ModelServingFactorBinding {
                plane: materialization.factor_serving_plane.clone(),
                bias_table: self.seed.bias_table,
            },
            schemas: ModelServingSchemaBinding {
                feature_schema_hash: *materialization.feature_schema_hash,
                label_schema_hash: *materialization.label_schema_hash,
            },
            transform: ModelServingTransformBinding {
                input_contract_hash,
                input_transform_hash,
                training_input_hash: self.seed.training_input_hash,
                training_dataset_hash: *materialization.dataset_hash,
            },
            model: ModelServingModelBinding {
                model_version_id: self.seed.model_version_id,
                model_spec_id: self.dataset.model_spec_id,
                model_spec_definition_hash: self.dataset.model_spec_definition_hash,
                model_family: self.dataset.model_family,
                category_scope: self.seed.category_scope,
                profile_ref: self
                    .dataset
                    .source_lineage
                    .research_profile_artifact_id
                    .profile_ref(),
                prediction_horizon_secs: self.prediction_horizon_secs,
                estimator,
                calibration: self.seed.calibration,
            },
            trade_policy: self.trade_policy,
            dataset: ModelServingDatasetBinding {
                manifest: materialization.manifest.clone(),
                manifest_hash: *materialization.manifest_hash,
                artifact_bytes_hash: *materialization.artifact_bytes_hash,
            },
        })
        .map_err(|error| ResearchError::InvalidModelArtifact {
            detail: format!("seal system-test model serving contract: {error}"),
        })?;
        let artifact = ModelArtifact::try_seal(contract, self.seed.payload)?;
        let artifact_hash = artifact.content_hash()?;
        Ok(SealedModelFixture {
            artifact,
            artifact_hash,
        })
    }
}

impl SealedModelFixture {
    /// Load exact persisted policy/dataset preimages and seal one artifact.
    pub async fn seal(
        db: &DatabaseConnection,
        seed: ModelArtifactFixtureSeed,
    ) -> QuantResult<Self> {
        ModelArtifactFixtureContext::load(db, seed).await?.seal()
    }

    async fn verify_calibration(
        db: &DatabaseConnection,
        binding: &ModelServingCalibrationArtifactRef,
        role: &'static str,
    ) -> QuantResult<()> {
        let persisted = PgCalibrationArtifactRepository::new(db.clone())
            .find_by_id(&binding.artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_calibration_artifact",
                id: binding.artifact_id.to_string(),
            })?;
        if persisted.kind != binding.kind
            || persisted.content_hash != binding.content_hash
            || !persisted.active
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture {role} binding {} does not exactly match an active persisted artifact",
                    binding.artifact_id
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn verify_trade_policy(
        db: &DatabaseConnection,
        binding: &ModelServingTradePolicyBinding,
    ) -> QuantResult<()> {
        let persisted = PgTradePolicyRepository::new(db.clone())
            .find(&binding.artifact_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "quant_trade_policy_artifact",
                id: binding.artifact_id.to_string(),
            })?;
        if persisted.content_hash != binding.content_hash
            || persisted.status != TradePolicyStatus::Published
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "model fixture trade-policy binding {} does not exactly match a Published persisted artifact",
                    binding.artifact_id
                ),
            }
            .into());
        }
        Ok(())
    }

    #[must_use]
    pub const fn artifact(&self) -> &ModelArtifact {
        &self.artifact
    }

    #[must_use]
    pub const fn artifact_hash(&self) -> ContentHash {
        self.artifact_hash
    }

    #[must_use]
    pub const fn serving_contract(&self) -> &ModelServingContract {
        self.artifact.header().serving_contract()
    }

    #[must_use]
    pub const fn training_dataset_id(&self) -> TrainingDatasetId {
        self.artifact
            .header()
            .serving_contract()
            .bindings()
            .dataset
            .manifest
            .training_dataset_id
    }

    /// Store exact canonical bytes at the artifact's content-addressed key.
    pub async fn store(&self, store: &Arc<dyn ArtifactStore>) -> QuantResult<()> {
        store
            .put(
                ModelArtifact::artifact_key(&self.artifact_hash)?,
                &self.artifact.to_bytes()?,
            )
            .await?;
        Ok(())
    }
}

/// Typed immutable dependency bindings used by model-serving fixtures.
pub struct ModelBindingFixture;

impl ModelBindingFixture {
    /// Construct a model-score calibration binding from a persisted artifact.
    #[must_use]
    pub const fn score_calibration(
        artifact_id: CalibrationArtifactId,
        content_hash: ContentHash,
    ) -> ModelServingCalibrationArtifactRef {
        ModelServingCalibrationArtifactRef {
            artifact_id,
            kind: CalibrationKind::ModelScore,
            content_hash,
        }
    }

    /// Construct a trade-policy binding from a persisted artifact.
    #[must_use]
    pub const fn trade_policy(
        artifact_id: TradePolicyArtifactId,
        content_hash: ContentHash,
    ) -> ModelServingTradePolicyBinding {
        ModelServingTradePolicyBinding {
            artifact_id,
            content_hash,
        }
    }
}

/// Variable identity inputs for one exact repository-backed model version.
pub struct ModelVersionFixtureSeed {
    pub scope: String,
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub training_input_hash: ContentHash,
}

impl ModelVersionFixtureSeed {
    /// Build a root-training fixture seed. A canonical Dataset v3 ledger is
    /// created when `training_dataset_id` remains `None`.
    #[must_use]
    pub fn training(
        scope: impl Into<String>,
        model_version_id: ModelVersionId,
        model_spec_id: ModelSpecId,
        training_input_hash: ContentHash,
    ) -> Self {
        Self {
            scope: scope.into(),
            model_version_id,
            model_spec_id,
            training_dataset_id: None,
            training_input_hash,
        }
    }
}

/// Canonical Dataset → Payload → Contract → Version fixture chain.
///
/// This is intentionally limited to factor-native families. Classical
/// versions must be produced from real fitted model bytes and must never be
/// represented by a synthetic repository fixture.
pub struct ModelVersionFixture;

impl ModelVersionFixture {
    /// Prepare one internally complete model version without persisting it.
    pub fn prepare<'a>(
        db: &'a DatabaseConnection,
        seed: ModelVersionFixtureSeed,
    ) -> Pin<Box<dyn Future<Output = QuantResult<NewModelVersion>> + Send + 'a>> {
        Box::pin(async move {
            let ModelVersionFixtureSeed {
                scope,
                model_version_id,
                model_spec_id,
                training_dataset_id,
                training_input_hash,
            } = seed;
            let registry = PgModelRegistryRepository::new(db.clone());
            let spec = registry
                .find_model_spec(&model_spec_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "quant_model_spec",
                    id: model_spec_id.to_string(),
                })?;
            let dataset = match training_dataset_id {
                Some(training_dataset_id) => PgTrainingDatasetRepository::new(db.clone())
                    .find_by_id(&training_dataset_id)
                    .await?
                    .ok_or_else(|| StorageError::NotFound {
                        entity: "quant_training_dataset",
                        id: training_dataset_id.to_string(),
                    })?,
                None => Box::pin(Self::persist_dataset(db, &scope, &spec)).await?,
            };
            let definition_matches = dataset.model_spec_definition_hash == spec.definition_hash;
            if dataset.model_spec_id != spec.model_spec_id
                || dataset.model_family != spec.model_family
                || !definition_matches
            {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model fixture dataset {} does not belong to spec {}",
                        dataset.training_dataset_id, spec.model_spec_id
                    ),
                }
                .into());
            }
            let materialization =
                dataset
                    .materialization()
                    .ok_or_else(|| ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "model fixture dataset {} has no complete materialization",
                            dataset.training_dataset_id
                        ),
                    })?;
            let policy = PgPolicyRepository::new(db.clone())
                .load_snapshot(&dataset.decision_policy_snapshot_id)
                .await?
                .ok_or_else(|| StorageError::NotFound {
                    entity: "decision_policy_snapshot",
                    id: dataset.decision_policy_snapshot_id.to_string(),
                })?;
            let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
            let profile = dataset
                .source_lineage
                .research_profile_artifact_id
                .profile_ref()
                .resolve_builtin_research_profile()
                .map_err(|detail| ResearchError::InvalidModelArtifact {
                    detail: format!("resolve model fixture ResearchProfile: {detail}"),
                })?;
            let payload = match spec.model_family {
                ModelFamily::WeightedFactor => ModelPayloadFixture::weighted(
                    materialization.factor_serving_plane,
                    &scoring.factor_head,
                    spec.input_contract.clone(),
                    ReturnModelSpec::heuristic_default(),
                    scoring.cross_section.clone(),
                )?,
                ModelFamily::HoldVsExitWeighted => ModelPayloadFixture::sell(
                    materialization.factor_serving_plane,
                    &scoring.factor_head,
                    &scoring.sell_scorer,
                    spec.input_contract.clone(),
                    scoring.cross_section.clone(),
                )?,
                model_family => {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: format!(
                            "model fixture requires real fitted bytes for family {model_family:?}"
                        ),
                    }
                    .into());
                }
            };
            let fixture = SealedModelFixture::seal(
                db,
                ModelArtifactFixtureSeed {
                    model_version_id,
                    training_dataset_id: dataset.training_dataset_id,
                    payload,
                    training_input_hash,
                    category_scope: profile.spec.category,
                    calibration: None,
                    bias_table: None,
                },
            )
            .await?;
            let serving_contract = fixture.serving_contract().clone();
            let bindings = serving_contract.bindings();
            let bound_category_scope = bindings.model.category_scope;
            let profile_ref = bindings.model.profile_ref.clone();
            let bound_training_dataset_id = bindings.dataset.manifest.training_dataset_id;
            let trade_policy = bindings
                .trade_policy
                .as_ref()
                .map(|binding| (binding.artifact_id, binding.content_hash));
            Ok(NewModelVersion {
                model_version_id,
                model_spec_id,
                version: 0,
                artifact_hash: fixture.artifact_hash(),
                serving_contract,
                category_scope: bound_category_scope,
                profile_ref,
                training_dataset_id: Some(bound_training_dataset_id),
                trade_policy_artifact_id: trade_policy.map(|binding| binding.0),
                trade_policy_hash: trade_policy.map(|binding| binding.1),
                publish_path_set_id: None,
                derivation: NewModelVersion::training_derivation(),
                metrics: ModelVersionMetrics::not_measured("test fixture"),
                training_objective: ModelTrainingObjective::hand_authored("test fixture"),
                quality_gate_report: None,
                publication_status: PublicationStatus::Candidate,
                published_at: None,
                retired_at: None,
            })
        })
    }

    /// Persist and publish a prepared Candidate through the exact frozen
    /// model/dataset parity proof and atomic global-latch transaction.
    pub fn persist_published<'a>(
        db: &'a DatabaseConnection,
        version: NewModelVersion,
    ) -> Pin<Box<dyn Future<Output = QuantResult<ModelVersionInfo>> + Send + 'a>> {
        Box::pin(async move {
            let registry = PgModelRegistryRepository::new(db.clone());
            let candidate = registry.create_model_version(version).await?;
            let contract = candidate.verified_serving_contract().map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "published model fixture has an invalid persisted serving contract: {error}"
                    ),
                }
            })?;
            let training_dataset_id = candidate.training_dataset_id.ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_model_version"),
                    format!(
                        "published model fixture {} has no training dataset",
                        candidate.model_version_id
                    ),
                )
            })?;
            let dataset = PgTrainingDatasetRepository::new(db.clone())
                .find_by_id(&training_dataset_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found("quant_training_dataset", training_dataset_id)
                })?;
            let materialization = dataset.materialization().ok_or_else(|| {
                StorageError::state_conflict(
                    "quant_training_dataset",
                    Some(&training_dataset_id),
                    "published model fixture dataset has no complete materialization",
                )
            })?;
            let run_id = FeatureParityRunId::from_v7();
            let parity = PgFeatureParityRepository::new(db.clone());
            parity
                .create_frozen_model_run(
                    NewFeatureParityRun {
                        run_id,
                        kind: FeatureParityRunKind::Full,
                        status: FeatureParityRunStatus::Queued,
                        window_start: dataset.window_start,
                        window_end: dataset.window_end,
                        report_id: None,
                        model_version_id: Some(candidate.model_version_id),
                        training_dataset_id: Some(training_dataset_id),
                        triggered_by: "model-serving-fixture".to_owned(),
                        requested_by: None,
                        acting_role: RoleCode::new("system"),
                        reason: "prove exact immutable model fixture before publication".to_owned(),
                        total_count: 0,
                        compared_count: 0,
                        matched_count: 0,
                        mismatched_count: 0,
                        pending_materialization_count: 0,
                        feature_contract_hash: Some(
                            contract.bindings().schemas.feature_schema_hash,
                        ),
                        transform_hash: None,
                        failure_code: None,
                        failure_detail: None,
                        started_at: None,
                        pending_since: None,
                        containment_completed_at: None,
                        finished_at: None,
                    },
                    NewFrozenModelParitySubject {
                        model_version_id: candidate.model_version_id,
                        training_dataset_id,
                        subject_generation: candidate.artifact_hash,
                        evidence_hash: ModelVersionParityEvidence {
                            model_version_id: &candidate.model_version_id,
                            model_spec_id: &candidate.model_spec_id,
                            artifact_hash: &candidate.artifact_hash,
                            training_dataset_id: &training_dataset_id,
                            dataset_hash: materialization.dataset_hash,
                            manifest_hash: materialization.manifest_hash,
                            artifact_bytes_hash: materialization.artifact_bytes_hash,
                        }
                        .content_hash()?,
                    },
                )
                .await?;
            parity.mark_running(&run_id).await?;
            parity
                .complete_run(
                    &run_id,
                    CompleteFeatureParityRun {
                        status: FeatureParityRunStatus::Passed,
                        total_count: 1,
                        compared_count: 1,
                        matched_count: 1,
                        mismatched_count: 0,
                        pending_materialization_count: 0,
                        feature_contract_hash: Some(
                            contract.bindings().schemas.feature_schema_hash,
                        ),
                        transform_hash: Some(contract.bindings().transform.input_transform_hash),
                        failure_code: None,
                        failure_detail: None,
                    },
                )
                .await?;
            let feature_parity_permit = match parity.current_state().await? {
                Some(state) => PublishFeatureParityPermit::ExistingGeneration(state.state_id),
                None => PublishFeatureParityPermit::InitializeFromProof {
                    actor: "model-serving-fixture".to_owned(),
                    acting_role: Some(RoleCode::new("system")),
                    reason: "bootstrap exact model publication parity generation".to_owned(),
                },
            };
            registry
                .publish_model_version(PublishModelVersionCommit {
                    model_spec_id: &candidate.model_spec_id,
                    model_version_id: &candidate.model_version_id,
                    feature_parity_permit,
                    feature_parity_run_id: &run_id,
                })
                .await
                .map(|result| result.published)
                .map_err(Into::into)
        })
    }

    async fn persist_dataset(
        db: &DatabaseConnection,
        scope: &str,
        spec: &ModelSpecInfo,
    ) -> QuantResult<TrainingDatasetInfo> {
        let decision_policy_snapshot_id = bootstrap_default_policy_bundle(
            db,
            "model-version-fixture",
            "persist exact model-serving fixture dependencies",
        )
        .await;
        let policy = PgPolicyRepository::new(db.clone())
            .load_snapshot(&decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: decision_policy_snapshot_id.to_string(),
            })?;
        let profiles =
            builtin_research_profiles().map_err(|detail| ResearchError::InvalidModelArtifact {
                detail: format!("load built-in research profiles for model fixture: {detail}"),
            })?;
        let prediction_horizon_secs =
            u64::try_from(spec.prediction_horizon_secs).map_err(|error| {
                ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "model fixture spec {} has invalid prediction horizon: {error}",
                        spec.model_spec_id
                    ),
                }
            })?;
        let profile = profiles
            .into_iter()
            .find(|profile| profile.spec.target_horizon_secs == prediction_horizon_secs)
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "no built-in research profile matches model horizon {prediction_horizon_secs}"
                ),
            })?;
        let features = &policy.snapshot.profile_artifacts.features.definition;
        let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
        let domain = &policy.snapshot.profile_artifacts.domain.definition;
        let feature_schema = FeatureSchema::build(features)?;
        let feature_schema_hash = ResearchHasher::feature_schema(&feature_schema)?;
        let factor_serving_plane =
            FactorEngine::for_model_scope(scoring, features, domain, profile.spec.category, None)
                .serving_plane()?
                .clone();
        // Leave one full governed embargo interval before any fixture
        // Calibration split so its terminal run can never finish in the
        // future relative to wall-clock time.
        let window_end = Utc::now() - Duration::days(2);
        let window_start = window_end - Duration::days(1);
        let research_program_hash = CanonicalDigest::content_hash_json(&(
            "model-version-fixture-program-v1",
            scope,
            spec.definition_hash,
            factor_serving_plane.factor_schema_hash(),
        ))?;
        let store = ModelDatasetLedgerFixture::local_store();
        ModelDatasetLedgerFixture::persist(
            db,
            &store,
            ModelDatasetLedgerSeed {
                scope: format!("{scope}:{}", spec.model_spec_id),
                model_spec_id: spec.model_spec_id,
                model_family: spec.model_family,
                model_spec_definition_hash: spec.definition_hash,
                factor_serving_plane,
                feature_schema_version: spec.feature_schema_version,
                feature_schema_hash,
                decision_policy_snapshot_id,
                profile_ref: profile.profile_ref,
                prediction_horizon_secs,
                purpose: DatasetPurpose::Training,
                window_start,
                window_end,
                research_program_hash,
                sample_count: 32,
                decision_interval_secs: 1,
                trade_policy: None,
            },
        )
        .await
    }
}
