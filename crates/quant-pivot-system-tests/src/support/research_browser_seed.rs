//! Coherent research lineage for the real-binary browser fixture.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionClock,
        ports::{
            FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
            FeedbackComparisonContract, FeedbackDatasetBuildRequest,
        },
        quant::{
            FeedbackCohortWindow, FeedbackCycleKey, FeedbackCycleKeyInput, FeedbackStageEventInput,
            NewBacktestReport, NewFeedbackCycle, NewFeedbackStageEvent, NewModelRun,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CalibrationMethod, DataQualityStatus, DatasetPurpose, DownsideSource, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily, ModelRunKind, TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, EventId, FeatureCell,
        FeatureStaleness, FeatureValue, FeedbackCycleId, MarketId, ModelRunId, ModelSpecId,
        ModelTrainingContract, ModelVersionId, Probability, ResearchEvaluationTrack, SchemaVersion,
        TokenId, TrainingDatasetId, TrainingExampleId, TrainingSampleSource, TrainingSampleSources,
        Usd,
        backtest::{
            CategoryMetric, CategoryMetrics, ExpectedVsRealized, PnlCurvePoint, PnlSimulation,
        },
        factor::{FactorExplanation, FactorServingPlane},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgFeedbackCycleRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, FeedbackCycleCasOutcome, FeedbackCycleGeneration,
        FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackStageWriteOutcome,
        ModelRegistryRepository, ModelRunRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::{FactorValue, NormalizedFactor},
    features::{
        FeatureVector,
        names::{book::MID, market::CATEGORY},
    },
    hashing::ResearchHasher,
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_source_fingerprint, label_names_for_sources,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use super::{
    execution_pg_seed::{
        CalibratedModelSeed, SharedDemoInfra, fixture_profile_ref, seed_calibrated_model,
    },
    model_spec_fixtures::new_model_spec_fixture,
    research_fixtures::{
        DatasetLedgerFixture, DatasetLedgerSeed, ReplayableSourceSliceFixture,
        bind_fixture_decision_capture, model_learning_cohort, persist_replayable_source_slice,
        seed_source_manifest,
    },
    seeded_uuid,
};

const TICK_COUNT: i64 = 4;
const MARKETS_PER_TICK: usize = 20;
const TICK_INTERVAL_SECS: i64 = 3_600;
const KNOWLEDGE_LAG_SECS: u64 = 10;

/// Stable identifiers exposed through the browser fixture's real REST API.
pub struct BrowserResearchFixture {
    pub evaluation_dataset_id: TrainingDatasetId,
    pub backtest_report_id: BacktestReportId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub model_version_id: ModelVersionId,
}

struct PersistedDataset {
    id: TrainingDatasetId,
    semantic_hash: ContentHash,
    feedback_request: FeedbackDatasetBuildRequest,
}

struct FeedbackCycleSeed<'a> {
    training: &'a PersistedDataset,
    evaluation: &'a PersistedDataset,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    observed_at: DateTime<Utc>,
}

impl PersistedDataset {
    fn try_new(
        id: TrainingDatasetId,
        semantic_hash: ContentHash,
        fixture: &DatasetLedgerFixture,
    ) -> QuantResult<Self> {
        let feedback_request = FeedbackDatasetBuildRequest {
            training_dataset_id: id,
            model_spec_id: fixture.plan.model_spec_id,
            model_spec_definition_hash: fixture.plan.model_spec_definition_hash,
            source_lineage: fixture.plan.source_lineage.clone(),
            window: FeedbackCohortWindow::try_new(
                fixture.plan.research_profile_artifact_id.profile_ref(),
                fixture.plan.window_start,
                fixture.plan.window_end,
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("freeze browser Dataset feedback window: {error}"),
            })?,
            purpose: fixture.plan.purpose,
        };
        feedback_request.validate()?;
        Ok(Self {
            id,
            semantic_hash,
            feedback_request,
        })
    }

    fn calibration_request(&self) -> QuantResult<FeedbackDatasetBuildRequest> {
        let window_start = self.feedback_request.window.cutoff() + Duration::seconds(1);
        let window_end = self.feedback_request.source_lineage.pit_cutoff;
        if window_start >= window_end {
            return Err(ResearchError::DatasetBuild {
                detail: "browser Training source lineage has no calibration embargo window"
                    .to_owned(),
            }
            .into());
        }
        let request = FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id: self.feedback_request.model_spec_id,
            model_spec_definition_hash: self.feedback_request.model_spec_definition_hash,
            source_lineage: self.feedback_request.source_lineage.clone(),
            window: FeedbackCohortWindow::try_new(
                self.feedback_request.window.profile_ref().clone(),
                window_start,
                window_end,
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("freeze browser Calibration window: {error}"),
            })?,
            purpose: DatasetPurpose::Calibration,
        };
        request.validate()?;
        Ok(request)
    }
}

struct DatasetSeed {
    scope: String,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_config_hash: ContentHash,
    purpose: DatasetPurpose,
    examples: Vec<TrainingExample>,
    feature_schema_hash: ContentHash,
    label_schema_hash: ContentHash,
}

struct BrowserCandidateSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    registry: &'a PgModelRegistryRepository,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    training: &'a PersistedDataset,
}

impl BrowserCandidateSeed<'_> {
    async fn persist(&self) -> QuantResult<ModelVersionId> {
        let model_version_id = ModelVersionId::from_v7();
        let training_input_hash = CanonicalDigest::content_hash_json(&(
            "browser-research-training-input-v1",
            self.training.semantic_hash,
            self.model_spec_definition_hash,
        ))?;
        let fixture = Box::pin(seed_calibrated_model(
            self.db,
            self.store,
            CalibratedModelSeed {
                model_version_id,
                training_dataset_id: self.training.id,
                training_input_hash,
            },
        ))
        .await;
        let version = self
            .registry
            .next_version_for_spec(&self.model_spec_id)
            .await?;
        self.registry
            .create_model_version(fixture.version(self.model_spec_id, version))
            .await?;
        Ok(model_version_id)
    }
}

/// Seed a loadable candidate, reusable Evaluation dataset, and immutable report.
pub async fn seed_browser_research(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
) -> QuantResult<BrowserResearchFixture> {
    let registry = PgModelRegistryRepository::new(db.clone());
    let template = registry
        .find_model_version(&infra.model_version_id)
        .await?
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "browser research template model {} is missing",
                infra.model_version_id
            ),
        })?;
    let model_spec = registry
        .find_model_spec(&template.model_spec_id)
        .await?
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "browser research model spec {} is missing",
                template.model_spec_id
            ),
        })?;
    let browser_model_spec_id = ModelSpecId::from_v7();
    let browser_model_spec = new_model_spec_fixture(
        browser_model_spec_id,
        "browser-research-model",
        model_spec.model_family,
        model_spec.prediction_horizon_secs,
        model_spec.input_contract.clone(),
        ModelTrainingContract::settlement_default(),
    );
    let browser_model_spec_definition_hash = browser_model_spec.definition_hash;
    registry.create_model_spec(browser_model_spec).await?;
    let template_bindings = template.serving_contract.bindings();
    let feature_schema_hash = template_bindings.schemas.feature_schema_hash;
    let factor_serving_plane = template_bindings.factors.plane.clone();
    let sample_sources = TrainingSampleSources::default();
    let label_schema_hash =
        ResearchHasher::label_schema(&label_names_for_sources(sample_sources.as_slice(), false))?;
    let observed_at = Utc::now();
    let now = DateTime::from_timestamp_millis(observed_at.timestamp_millis()).ok_or_else(|| {
        ResearchError::DatasetBuild {
            detail: "browser research fixture time does not fit millisecond precision".to_owned(),
        }
    })?;
    let policy_snapshot_hash = template_bindings.policy_snapshot.snapshot_hash;
    let training = persist_dataset(
        db,
        store,
        DatasetSeed {
            scope: "browser-research-training".to_owned(),
            model_spec_id: browser_model_spec_id,
            model_family: model_spec.model_family,
            model_spec_definition_hash: browser_model_spec_definition_hash,
            factor_serving_plane: factor_serving_plane.clone(),
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: policy_snapshot_hash,
            purpose: DatasetPurpose::Training,
            examples: model_examples("training", now - Duration::days(120), &factor_serving_plane)?,
            feature_schema_hash,
            label_schema_hash,
        },
    )
    .await?;
    let model_version_id = Box::pin(
        BrowserCandidateSeed {
            db,
            store,
            registry: &registry,
            model_spec_id: browser_model_spec_id,
            model_spec_definition_hash: browser_model_spec_definition_hash,
            training: &training,
        }
        .persist(),
    )
    .await?;
    let evaluation = persist_dataset(
        db,
        store,
        DatasetSeed {
            scope: "browser-research-evaluation".to_owned(),
            model_spec_id: browser_model_spec_id,
            model_family: model_spec.model_family,
            model_spec_definition_hash: browser_model_spec_definition_hash,
            factor_serving_plane: factor_serving_plane.clone(),
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: policy_snapshot_hash,
            purpose: DatasetPurpose::Evaluation,
            examples: model_examples(
                "evaluation",
                now - Duration::days(30),
                &factor_serving_plane,
            )?,
            feature_schema_hash,
            label_schema_hash,
        },
    )
    .await?;
    let report = seed_backtest_report(
        db,
        model_version_id,
        &evaluation,
        infra.decision_policy_snapshot_id,
        now,
    )
    .await?;
    let candidate = registry
        .find_model_version(&model_version_id)
        .await?
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!("browser candidate model {model_version_id} is missing"),
        })?;
    let feedback_cycle_id = FeedbackCycleSeed {
        training: &training,
        evaluation: &evaluation,
        champion_model_version_id: candidate.model_version_id,
        champion_serving_contract_hash: candidate.serving_contract_hash,
        observed_at: now,
    }
    .persist(db)
    .await?;
    Ok(BrowserResearchFixture {
        evaluation_dataset_id: evaluation.id,
        backtest_report_id: report,
        feedback_cycle_id,
        model_version_id,
    })
}

impl FeedbackCycleSeed<'_> {
    async fn persist(self, db: &DatabaseConnection) -> QuantResult<FeedbackCycleId> {
        let profile_ref = fixture_profile_ref();
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("resolve browser feedback profile: {detail}"),
            })?;
        let comparison_contract =
            FeedbackComparisonContract::try_from_policy(&profile.spec.feedback_policy)?;
        let recipe = FeedbackCandidateRecipe::try_seal(
            self.training.feedback_request.clone(),
            self.training.calibration_request()?,
            CalibrationMethod::Platt,
            DownsideSource::MfeMae,
            self.training
                .feedback_request
                .source_lineage
                .decision_policy_snapshot_id,
        )?;
        let candidate_family = FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
            shared_evaluation: self.evaluation.feedback_request.clone(),
            comparison_contract,
            candidates: vec![recipe],
        })?;
        let feedback_policy_hash =
            profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("hash browser feedback policy: {error}"),
                })?;
        let cycle =
            NewFeedbackCycle::try_seal(FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
                trigger_family: FeedbackTriggerFamily::Manual,
                profile_ref,
                feedback_policy_hash,
                label_cutoff: self.evaluation.feedback_request.source_lineage.pit_cutoff,
                capability_registry_hashes: self
                    .evaluation
                    .feedback_request
                    .source_lineage
                    .capability_registry_hashes
                    .clone(),
                champion_model_version_id: self.champion_model_version_id,
                champion_serving_contract_hash: self.champion_serving_contract_hash,
                candidate_family,
            })?)?;
        let feedback_cycle_id = cycle.feedback_cycle_id();
        let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            research_job_id: None,
            actor: Some("browser_fixture".to_owned()),
            reason_code: Some("browser_fixture".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: self.observed_at,
        })?;
        let repository = PgFeedbackCycleRepository::new(db.clone());
        let (cycle_outcome, stage_outcome) = repository.record_trigger(cycle, trigger).await?;
        let (
            (
                FeedbackCycleWriteOutcome::Inserted(persisted),
                FeedbackStageWriteOutcome::Inserted(_),
            )
            | (
                FeedbackCycleWriteOutcome::AlreadyPresent(persisted),
                FeedbackStageWriteOutcome::AlreadyPresent(_),
            ),
        ) = ((cycle_outcome, stage_outcome),)
        else {
            return Err(ResearchError::DatasetBuild {
                detail: "browser feedback trigger outcomes are inconsistent".to_owned(),
            }
            .into());
        };
        let cancellation = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 2,
            stage: FeedbackStage::Coverage,
            event_kind: FeedbackStageEventKind::CancellationRequested,
            research_job_id: None,
            actor: Some("browser_fixture".to_owned()),
            reason_code: Some("browser_fixture_cancelled".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: self.observed_at,
        })?;
        let (cycle_outcome, stage_outcome) = repository
            .request_cancel(FeedbackCycleGeneration::from(&persisted), cancellation)
            .await?;
        if !matches!(
            (cycle_outcome, stage_outcome),
            (
                FeedbackCycleCasOutcome::Applied(_),
                FeedbackStageWriteOutcome::Inserted(_)
            ) | (
                FeedbackCycleCasOutcome::AlreadyApplied(_),
                FeedbackStageWriteOutcome::AlreadyPresent(_)
            )
        ) {
            return Err(ResearchError::DatasetBuild {
                detail: "browser feedback cancellation outcomes are inconsistent".to_owned(),
            }
            .into());
        }
        Ok(feedback_cycle_id)
    }
}

fn model_examples(
    scope: &str,
    window_start: DateTime<Utc>,
    factor_serving_plane: &FactorServingPlane,
) -> QuantResult<Vec<TrainingExample>> {
    let mut examples = Vec::with_capacity(
        usize::try_from(TICK_COUNT).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("browser tick count is invalid: {error}"),
        })? * MARKETS_PER_TICK,
    );
    for tick in 0..TICK_COUNT {
        let decision_at = window_start + Duration::seconds(tick * TICK_INTERVAL_SECS);
        for ordinal in 0..MARKETS_PER_TICK {
            examples.push(model_example(
                scope,
                tick,
                ordinal,
                decision_at,
                factor_serving_plane,
            )?);
        }
    }
    Ok(examples)
}

fn model_example(
    scope: &str,
    tick: i64,
    ordinal: usize,
    decision_at: DateTime<Utc>,
    factor_serving_plane: &FactorServingPlane,
) -> QuantResult<TrainingExample> {
    let strength = Decimal::from(u64::try_from(ordinal % 9 + 1).map_err(|error| {
        ResearchError::DatasetBuild {
            detail: format!("browser example ordinal is invalid: {error}"),
        }
    })?) / dec!(10);
    let liquidity = Decimal::from(
        u64::try_from(ordinal + 1).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("browser liquidity ordinal is invalid: {error}"),
        })? * 1_000,
    );
    let market_id = MarketId::new(format!("browser-{scope}-market-{tick}-{ordinal}"));
    let token_id = TokenId::new(
        seeded_uuid(&format!("browser:{scope}:token:{tick}:{ordinal}"))
            .as_u128()
            .to_string(),
    );
    let secondary_token_id = TokenId::new(
        seeded_uuid(&format!("browser:{scope}:token:no:{tick}:{ordinal}"))
            .as_u128()
            .to_string(),
    );
    let feature_vector = FeatureVector {
        market_id: market_id.clone(),
        token_id: Some(token_id.clone()),
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
                    FeatureValue::Category(MarketCategory::Weather),
                    None,
                    FeatureStaleness::Unknown,
                ),
            ),
        ]),
        domain: None,
        data_quality: DataQualityStatus::Fresh,
    };
    let factor_values = factor_serving_plane
        .definitions()
        .iter()
        .map(|revision| {
            let definition = revision.definition();
            let direction = definition.contribution_direction(strength).ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "browser factor `{}` cannot project fixture strength",
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
                    headline: format!("browser fixture {} rank", definition.name),
                    drivers: Vec::new(),
                },
                input_feature_refs: definition.input_features.clone(),
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let mut example = TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        selected_market: SelectedMarket {
            market_id,
            event_id: EventId::new(format!("browser-{scope}-event")),
            category: MarketCategory::Weather,
            primary_token_id: token_id,
            secondary_token_id: Some(secondary_token_id),
            liquidity_usd: Some(Usd::new(liquidity)),
            volume_24h_usd: Some(Usd::new(liquidity * dec!(2))),
            source_refs: Vec::new(),
        },
        decision_boundary: DecisionClock::new(KNOWLEDGE_LAG_SECS).boundary(decision_at)?,
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector,
        factor_values,
        labels: vec![TrainingLabel {
            label_name: TOKEN_PAYOUT_RATIO,
            horizon_secs: 0,
            value: if strength > dec!(0.5) {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
            is_resolved: true,
            matured_at: decision_at + Duration::seconds(1),
        }],
        source_refs: Vec::new(),
        decision_capture: None,
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    };
    bind_fixture_decision_capture(&mut example);
    Ok(example)
}

async fn persist_dataset(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    seed: DatasetSeed,
) -> QuantResult<PersistedDataset> {
    let window_start = seed
        .examples
        .iter()
        .map(TrainingExample::decision_at)
        .min()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "browser Dataset fixture requires examples".to_owned(),
        })?;
    let window_end = seed
        .examples
        .iter()
        .map(TrainingExample::decision_at)
        .max()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "browser Dataset fixture requires examples".to_owned(),
        })?
        + Duration::hours(1);
    let semantic_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: &seed.model_spec_id,
            model_family: seed.model_family,
            window_start,
            window_end,
            purpose: seed.purpose,
            feature_schema_hash: &seed.feature_schema_hash,
            factor_serving_plane: &seed.factor_serving_plane,
            label_schema_hash: &seed.label_schema_hash,
        },
        &seed.examples,
    )?;
    let profile_ref = fixture_profile_ref();
    let profile = profile_ref
        .resolve_builtin_research_profile()
        .map_err(|detail| ResearchError::DatasetBuild {
            detail: format!("resolve browser ResearchProfile: {detail}"),
        })?;
    let source_window_end = window_end
        .checked_add_signed(Duration::seconds(
            i64::try_from(profile.spec.target_horizon_secs).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("browser profile horizon overflow: {error}"),
                }
            })?,
        ))
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "browser Source Slice terminal bound overflow".to_owned(),
        })?;
    let stored_source = persist_replayable_source_slice(
        store,
        &seed.examples,
        ReplayableSourceSliceFixture {
            profile_ref,
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash: CanonicalDigest::content_hash_json(&(
                &seed.scope,
                "research-program",
            ))?,
            decision_policy_snapshot_id: seed.decision_policy_snapshot_id,
            runtime_config_hash: seed.runtime_config_hash,
            window_start,
            window_end: source_window_end,
        },
    )
    .await?;
    let source_lineage = seed_source_manifest(db, &stored_source).await?;
    let sample_count =
        u64::try_from(seed.examples.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("browser Dataset sample count overflow: {error}"),
        })?;
    let cohort_manifest = if seed.purpose == DatasetPurpose::Evaluation {
        Some(model_learning_cohort(
            &seed.scope,
            &source_lineage,
            window_start,
            window_end,
            sample_count,
        )?)
    } else {
        None
    };
    let dataset_id = TrainingDatasetId::from_v7();
    let fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id: dataset_id,
        model_spec_id: seed.model_spec_id,
        model_family: seed.model_family,
        model_spec_definition_hash: seed.model_spec_definition_hash,
        factor_serving_plane: seed.factor_serving_plane.clone(),
        source_lineage,
        cohort_manifest,
        window_start,
        window_end,
        purpose: seed.purpose,
        knowledge_lag_secs: KNOWLEDGE_LAG_SECS,
        sample_interval_secs: u64::try_from(TICK_INTERVAL_SECS).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("browser Dataset interval is invalid: {error}"),
            }
        })?,
        horizons_secs: vec![0, profile.spec.target_horizon_secs],
        feature_schema_version: SchemaVersion::FIRST,
        sample_sources: Some(TrainingSampleSources::default()),
        feature_schema_hash: seed.feature_schema_hash,
        label_schema_hash: seed.label_schema_hash,
        semantic_dataset_hash: semantic_hash,
        source_fingerprint: dataset_source_fingerprint(&seed.examples)?,
        sample_count,
    })?;
    fixture
        .manifest
        .validate()
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("browser model dataset policy binding is invalid: {error}"),
        })?;
    let bytes = DatasetParquetCodec::encode(&seed.examples, &fixture.manifest)?;
    let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::Dataset,
        dataset_id.as_uuid().simple().to_string(),
        "parquet",
    )?;
    let uri = store.put(key, &bytes).await?;
    let repository = PgTrainingDatasetRepository::new(db.clone());
    repository.create_plan(fixture.plan.clone()).await?;
    repository.start_build(&dataset_id).await?;
    repository
        .complete_build(
            &dataset_id,
            fixture.completion(
                TrainingDatasetStatus::Ready,
                artifact_bytes_hash,
                uri,
                fixture.coverage(),
                None,
            )?,
        )
        .await?;
    PersistedDataset::try_new(dataset_id, semantic_hash, &fixture)
}

async fn seed_backtest_report(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    evaluation: &PersistedDataset,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    now: DateTime<Utc>,
) -> QuantResult<BacktestReportId> {
    let model_run_id = ModelRunId::from_v7();
    let window_start = now - Duration::days(30);
    let window_end = window_start + Duration::hours(TICK_COUNT * TICK_INTERVAL_SECS / 3_600);
    let model_runs = PgModelRunRepository::new(db.clone());
    model_runs
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash: evaluation.semantic_hash,
        })
        .await?;
    let backtest_report_id = BacktestReportId::from_v7();
    let curve = [(0, -18), (45, 4), (90, 16), (135, 11), (180, 31), (225, 47)]
        .into_iter()
        .map(|(minutes, pnl)| PnlCurvePoint {
            decision_at: window_start + Duration::minutes(minutes),
            cumulative_realized_pnl_usd: Decimal::from(pnl),
        })
        .collect();
    let mut report = NewBacktestReport {
        backtest_report_id,
        model_version_id,
        evaluation_dataset_id: evaluation.id,
        model_run_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
        coverage: dec!(0.975),
        sample_count: 80,
        missing_feature_count: 0,
        rank_ic: dec!(0.143),
        sharpe: dec!(1.18),
        hit_rate: Probability::new(dec!(0.625)),
        expected_vs_realized: ExpectedVsRealized {
            mean_expected_bps: dec!(92),
            mean_realized_bps: dec!(84),
            correlation: dec!(0.61),
            bias_bps: dec!(8),
        },
        max_drawdown: dec!(0.072),
        turnover: dec!(0.19),
        liquidity_feasibility: Probability::new(dec!(0.94)),
        category_breakdown: CategoryMetrics::from(vec![CategoryMetric {
            category: MarketCategory::Weather,
            sample_count: 80,
            rank_ic: dec!(0.143),
            hit_rate: Probability::new(dec!(0.625)),
            mean_realized_bps: dec!(84),
        }]),
        tail_loss: dec!(-42),
        report_pnl_simulation: PnlSimulation {
            total_allocated_usd: dec!(1_000),
            realized_pnl_usd: dec!(47),
            gross_return: dec!(0.047),
            pnl_curve: curve,
        },
        report_hash: ContentHash::from_bytes([0; 32]),
        parquet_uri: None,
    };
    report.report_hash =
        report
            .recomputed_hash()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("seal browser backtest report: {detail}"),
            })?;
    let report_hash = report.report_hash;
    PgBacktestReportRepository::new(db.clone())
        .create(report)
        .await?;
    model_runs
        .succeed(&model_run_id, report_hash, Some(model_version_id))
        .await?;
    Ok(backtest_report_id)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{FactorServingPlane, model_example};
    use quant_pivot_models::{
        clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
        types::{ContentHash, EvmBlockHash, EvmTransactionHash, PayoutRatio},
    };

    #[test]
    fn resolution_tokens_are_canonical() {
        let factor_serving_plane = FactorServingPlane::try_empty().expect("empty factor plane");
        let example = model_example("contract", 0, 0, Utc::now(), &factor_serving_plane)
            .expect("browser research example");
        let secondary_token_id = example
            .selected_market
            .secondary_token_id
            .expect("secondary token");

        MarketResolutionRow::seal(MarketResolutionFactInput {
            market_id: example.market_id,
            token_ids: [example.token_id, secondary_token_id],
            payout_ratios: [PayoutRatio::ONE, PayoutRatio::ZERO],
            resolved_at: 100,
            observed_at: 110,
            source_block_number: 42,
            source_block_hash: EvmBlockHash::parse(format!("0x{}", "11".repeat(32)))
                .expect("block hash"),
            source_transaction_hash: EvmTransactionHash::parse(format!("0x{}", "22".repeat(32)))
                .expect("transaction hash"),
            source_log_index: 3,
            source_checkpoint_hash: ContentHash::from_bytes([0x33; 32]),
        })
        .expect("canonical resolution tokens");
    }
}
