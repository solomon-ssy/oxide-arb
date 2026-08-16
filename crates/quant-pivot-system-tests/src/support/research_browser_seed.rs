//! Coherent research lineage for the real-binary browser fixture.

use std::{collections::BTreeMap, sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::app::ports::feedback_mutation::FeedbackCycleFreezePlan;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        api::BuildTrainingDatasetRequest,
        data_plane::DecisionClock,
        ports::FeedbackDatasetBuildRequest,
        quant::{
            FeedbackCohortWindow, FeedbackCycleInfo, FeedbackCycleKey, FeedbackCycleKeyInput,
            FeedbackStageEventInput, ModelSpecInfo, ModelVersionInfo, NewBacktestReport,
            NewFeedbackCycle, NewFeedbackStageEvent, NewModelRun, NewResearchJob,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            DataQualityStatus, DatasetPurpose, FeedbackEvaluationMode, FeedbackStage,
            FeedbackStageEventKind, FeedbackTriggerFamily, ModelRunKind, ResearchJobKind,
            ResearchJobStatus, TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, EventId, FeatureCell,
        FeatureStaleness, FeatureValue, FeedbackCycleId, MarketId, ModelRunId, ModelSpecId,
        ModelTrainingContract, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchEvaluationTrack, ResearchJobId, ResearchJobParams, RoleCode, SchemaVersion,
        TokenId, TrainingDatasetId, TrainingExampleId, TrainingSampleSource, TrainingSampleSources,
        Usd, WorkerId,
        backtest::{
            BacktestPortfolioFunnel, CategoryMetric, CategoryMetrics, ExpectedVsRealized,
            PnlCurvePoint, PnlSimulation,
        },
        factor::{FactorExplanation, FactorServingPlane},
        model_serving::ModelServingTradePolicyBinding,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgFeedbackCycleRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgResearchJobRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, FeedbackCycleCasOutcome, FeedbackCycleGeneration,
        FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackStageWriteOutcome,
        FeedbackTriggerCommit, FeedbackTriggerWriteOutcome, ModelRegistryRepository,
        ModelRunRepository, ResearchJobRepository, ResearchJobRetryOutcome,
        TrainingDatasetRepository,
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
        DatasetHashContract, DatasetParquetCodec, TrainingDatasetArtifact, TrainingExample,
        dataset_source_fingerprint, label_names_for_sources,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use super::{
    execution_pg_seed::{
        CalibratedModelHead, CalibratedModelSeed, SharedDemoInfra, fixture_profile_ref,
        seed_calibrated_model,
    },
    model_serving_fixtures::{ModelDatasetLedgerFixture, ModelVersionFixture},
    model_spec_fixtures::new_model_spec_fixture,
    research_fixtures::{
        DatasetLedgerFixture, DatasetLedgerSeed, ReplayableSourceSliceFixture,
        bind_fixture_decision_capture, model_learning_cohort, persist_replayable_source_slice,
        seed_source_manifest,
    },
    seeded_uuid,
};
use crate::postgres::PostgresClock;

const TICK_COUNT: i64 = 4;
const MARKETS_PER_TICK: usize = 20;
const TICK_INTERVAL_SECS: i64 = 3_600;
const KNOWLEDGE_LAG_SECS: u64 = 10;

/// Stable identifiers exposed through the browser fixture's real REST API.
pub struct BrowserResearchFixture {
    pub evaluation_dataset_id: TrainingDatasetId,
    pub backtest_report_id: BacktestReportId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub governed_cancellation_cycle_id: Option<FeedbackCycleId>,
    pub cancellable_research_job_id: ResearchJobId,
    pub model_version_id: ModelVersionId,
}

struct PersistedDataset {
    id: TrainingDatasetId,
    semantic_hash: ContentHash,
    feedback_request: FeedbackDatasetBuildRequest,
}

struct FeedbackCycleSeed<'a> {
    evaluation: &'a PersistedDataset,
    cancellation_evaluation: Option<&'a PersistedDataset>,
    model_family: ModelFamily,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    observed_at: DateTime<Utc>,
}

struct FeedbackCycleSeedInput<'a> {
    cancellation_evaluation: Option<&'a PersistedDataset>,
    db: &'a DatabaseConnection,
    evaluation: &'a PersistedDataset,
    fixture: FeedbackCycleFixture,
    model_version_id: ModelVersionId,
    observed_at: DateTime<Utc>,
    registry: &'a PgModelRegistryRepository,
}

struct PersistedFeedbackCycles {
    governed_cancellation_cycle_id: Option<FeedbackCycleId>,
    historical_cycle_id: FeedbackCycleId,
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
    trade_policy: Option<ModelServingTradePolicyBinding>,
}

struct BrowserEvaluationSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    factor_serving_plane: &'a FactorServingPlane,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_config_hash: ContentHash,
    observed_at: DateTime<Utc>,
    feature_schema_hash: ContentHash,
    label_schema_hash: ContentHash,
    trade_policy: Option<ModelServingTradePolicyBinding>,
}

#[derive(Clone, Copy)]
enum BrowserModelTarget {
    Candidate,
    Active(ModelVersionId),
}

struct BrowserModelSpec {
    id: ModelSpecId,
    definition_hash: ContentHash,
}

impl BrowserModelSpec {
    async fn resolve(
        registry: &PgModelRegistryRepository,
        template: &ModelVersionInfo,
        source: &ModelSpecInfo,
        target: BrowserModelTarget,
    ) -> QuantResult<Self> {
        match target {
            BrowserModelTarget::Candidate => {
                let id = ModelSpecId::from_v7();
                let spec = new_model_spec_fixture(
                    id,
                    "browser-research-model",
                    source.model_family,
                    source.prediction_horizon_secs,
                    source.input_contract.clone(),
                    ModelTrainingContract::outcome_default(),
                );
                let definition_hash = spec.definition_hash;
                registry.create_model_spec(spec).await?;
                Ok(Self {
                    id,
                    definition_hash,
                })
            }
            BrowserModelTarget::Active(model_version_id) => {
                if template.model_version_id != model_version_id {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "active browser research model {model_version_id} differs from complete template {}",
                            template.model_version_id
                        ),
                    }
                    .into());
                }
                Ok(Self {
                    id: template.model_spec_id,
                    definition_hash: source.definition_hash,
                })
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FeedbackSourceSeed {
    Fixture,
    Production,
}

#[derive(Clone, Copy)]
enum FeedbackCycleFixture {
    HistoricalOnly,
    GovernedCancellation,
}

struct BrowserModelSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    registry: &'a PgModelRegistryRepository,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    training: &'a PersistedDataset,
    target: BrowserModelTarget,
    head: CalibratedModelHead,
}

struct FeedbackTriggerSourceSeed<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_config_hash: ContentHash,
    factor_serving_plane: &'a FactorServingPlane,
    include_trade_policy_labels: bool,
}

impl BrowserModelSeed<'_> {
    async fn persist(&self) -> QuantResult<ModelVersionId> {
        let model_version_id = match self.target {
            BrowserModelTarget::Candidate => ModelVersionId::from_v7(),
            BrowserModelTarget::Active(model_version_id) => model_version_id,
        };
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
                head: self.head.clone(),
            },
        ))
        .await;
        let version = self
            .registry
            .next_version_for_spec(&self.model_spec_id)
            .await?;
        let version = fixture.version(self.model_spec_id, version);
        match self.target {
            BrowserModelTarget::Candidate => {
                self.registry.create_model_version(version).await?;
            }
            BrowserModelTarget::Active(_) => {
                ModelVersionFixture::persist_route_candidate(self.db, version).await?;
            }
        }
        Ok(model_version_id)
    }
}

impl BrowserEvaluationSeed<'_> {
    async fn persist(
        &self,
        feedback_cycle_fixture: FeedbackCycleFixture,
    ) -> QuantResult<(PersistedDataset, Option<PersistedDataset>)> {
        let evaluation = persist_dataset(
            self.db,
            self.store,
            DatasetSeed {
                scope: "browser-research-evaluation".to_owned(),
                model_spec_id: self.model_spec_id,
                model_family: self.model_family,
                model_spec_definition_hash: self.model_spec_definition_hash,
                factor_serving_plane: self.factor_serving_plane.clone(),
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                runtime_config_hash: self.runtime_config_hash,
                purpose: DatasetPurpose::Evaluation,
                examples: model_examples(
                    "evaluation",
                    self.observed_at - Duration::days(30),
                    self.factor_serving_plane,
                    self.trade_policy.is_some(),
                )?,
                feature_schema_hash: self.feature_schema_hash,
                label_schema_hash: self.label_schema_hash,
                trade_policy: self.trade_policy.clone(),
            },
        )
        .await?;
        let cancellation_evaluation = match feedback_cycle_fixture {
            FeedbackCycleFixture::HistoricalOnly => None,
            FeedbackCycleFixture::GovernedCancellation => Some(
                persist_dataset(
                    self.db,
                    self.store,
                    DatasetSeed {
                        scope: "browser-research-cancellation-evaluation".to_owned(),
                        model_spec_id: self.model_spec_id,
                        model_family: self.model_family,
                        model_spec_definition_hash: self.model_spec_definition_hash,
                        factor_serving_plane: self.factor_serving_plane.clone(),
                        decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                        runtime_config_hash: self.runtime_config_hash,
                        purpose: DatasetPurpose::Evaluation,
                        examples: model_examples(
                            "cancellation-evaluation",
                            self.observed_at - Duration::days(45),
                            self.factor_serving_plane,
                            self.trade_policy.is_some(),
                        )?,
                        feature_schema_hash: self.feature_schema_hash,
                        label_schema_hash: self.label_schema_hash,
                        trade_policy: self.trade_policy.clone(),
                    },
                )
                .await?,
            ),
        };
        Ok((evaluation, cancellation_evaluation))
    }
}

impl FeedbackTriggerSourceSeed<'_> {
    async fn persist(&self) -> QuantResult<()> {
        let profile_ref = fixture_profile_ref();
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("resolve governed-feedback profile: {detail}"),
            })?;
        let plan = FeedbackCycleFreezePlan::derive(
            &profile,
            self.model_spec_id,
            self.model_spec_definition_hash,
            self.decision_policy_snapshot_id,
            self.runtime_config_hash,
            self.db.statement_time().await,
        )?;
        let examples = model_examples(
            "governed-feedback-trigger",
            plan.source_start(),
            self.factor_serving_plane,
            self.include_trade_policy_labels,
        )?;
        let stored = persist_replayable_source_slice(
            self.store,
            &examples,
            ReplayableSourceSliceFixture {
                profile_ref,
                evaluation_track: profile.spec.activation_eligibility,
                research_program_hash: plan.research_program_hash(),
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                runtime_config_hash: self.runtime_config_hash,
                window_start: plan.source_start(),
                window_end: plan.label_cutoff(),
            },
        )
        .await?;
        seed_source_manifest(self.db, &stored).await?;
        Ok(())
    }

    async fn persist_for(
        &self,
        model_target: BrowserModelTarget,
        source_seed: FeedbackSourceSeed,
    ) -> QuantResult<()> {
        match (model_target, source_seed) {
            (BrowserModelTarget::Active(_), FeedbackSourceSeed::Fixture) => self.persist().await,
            (BrowserModelTarget::Candidate, FeedbackSourceSeed::Fixture) => {
                Err(ResearchError::DatasetBuild {
                    detail: "candidate-only browser research cannot seed an active-route feedback Source Slice"
                        .to_owned(),
                }
                .into())
            }
            (
                BrowserModelTarget::Candidate | BrowserModelTarget::Active(_),
                FeedbackSourceSeed::Production,
            ) => Ok(()),
        }
    }
}

/// Seed a loadable candidate, reusable Evaluation dataset, and immutable report.
pub async fn seed_browser_research(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
) -> QuantResult<BrowserResearchFixture> {
    Box::pin(seed_research(
        db,
        store,
        infra,
        BrowserModelTarget::Candidate,
        FeedbackSourceSeed::Production,
        FeedbackCycleFixture::HistoricalOnly,
    ))
    .await
}

/// Seed the browser research graph with the exact active-route model already
/// reserved by the governed-feedback policy fixture.
pub async fn seed_governed_feedback_research(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    champion_model_version_id: ModelVersionId,
) -> QuantResult<BrowserResearchFixture> {
    Box::pin(seed_research(
        db,
        store,
        infra,
        BrowserModelTarget::Active(champion_model_version_id),
        FeedbackSourceSeed::Fixture,
        FeedbackCycleFixture::GovernedCancellation,
    ))
    .await
}

/// Seed browser research around the active route while leaving the canonical
/// feedback Source Slice for the production `DatasetSeal` stage to materialize.
pub async fn seed_closure_feedback_research(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    champion_model_version_id: ModelVersionId,
) -> QuantResult<BrowserResearchFixture> {
    Box::pin(seed_research(
        db,
        store,
        infra,
        BrowserModelTarget::Active(champion_model_version_id),
        FeedbackSourceSeed::Production,
        FeedbackCycleFixture::HistoricalOnly,
    ))
    .await
}

async fn seed_research(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    infra: &SharedDemoInfra,
    model_target: BrowserModelTarget,
    feedback_source_seed: FeedbackSourceSeed,
    feedback_cycle_fixture: FeedbackCycleFixture,
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
    let browser_spec =
        BrowserModelSpec::resolve(&registry, &template, &model_spec, model_target).await?;
    let browser_model_spec_id = browser_spec.id;
    let browser_model_spec_definition_hash = browser_spec.definition_hash;
    let template_bindings = template.serving_contract.bindings();
    let feature_schema_hash = template_bindings.schemas.feature_schema_hash;
    let factor_serving_plane = template_bindings.factors.plane.clone();
    let trade_policy = match model_target {
        BrowserModelTarget::Candidate => None,
        BrowserModelTarget::Active(_) => template_bindings.trade_policy.clone(),
    };
    let sample_sources = TrainingSampleSources::default();
    let label_schema_hash = ResearchHasher::label_schema(&label_names_for_sources(
        sample_sources.as_slice(),
        trade_policy.is_some(),
    ))?;
    let observed_at = Utc::now();
    let now = DateTime::from_timestamp_millis(observed_at.timestamp_millis()).ok_or_else(|| {
        ResearchError::DatasetBuild {
            detail: "browser research fixture time does not fit millisecond precision".to_owned(),
        }
    })?;
    let policy_snapshot_hash = template_bindings.policy_snapshot.snapshot_hash;
    let model_version_id = match model_target {
        BrowserModelTarget::Candidate => {
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
                    examples: model_examples(
                        "training",
                        now - Duration::days(120),
                        &factor_serving_plane,
                        false,
                    )?,
                    feature_schema_hash,
                    label_schema_hash,
                    trade_policy: None,
                },
            )
            .await?;
            Box::pin(
                BrowserModelSeed {
                    db,
                    store,
                    registry: &registry,
                    model_spec_id: browser_model_spec_id,
                    model_spec_definition_hash: browser_model_spec_definition_hash,
                    training: &training,
                    target: model_target,
                    head: CalibratedModelHead::Policy,
                }
                .persist(),
            )
            .await?
        }
        BrowserModelTarget::Active(model_version_id) => model_version_id,
    };
    FeedbackTriggerSourceSeed {
        db,
        store,
        model_spec_id: browser_model_spec_id,
        model_spec_definition_hash: browser_model_spec_definition_hash,
        decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
        runtime_config_hash: policy_snapshot_hash,
        factor_serving_plane: &factor_serving_plane,
        include_trade_policy_labels: trade_policy.is_some(),
    }
    .persist_for(model_target, feedback_source_seed)
    .await?;
    let (evaluation, cancellation_evaluation) = BrowserEvaluationSeed {
        db,
        store,
        model_spec_id: browser_model_spec_id,
        model_family: model_spec.model_family,
        model_spec_definition_hash: browser_model_spec_definition_hash,
        factor_serving_plane: &factor_serving_plane,
        decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
        runtime_config_hash: policy_snapshot_hash,
        observed_at: now,
        feature_schema_hash,
        label_schema_hash,
        trade_policy,
    }
    .persist(feedback_cycle_fixture)
    .await?;
    let report = seed_backtest_report(
        db,
        model_version_id,
        &evaluation,
        infra.decision_policy_snapshot_id,
        now,
    )
    .await?;
    let feedback_cycles = FeedbackCycleSeed::persist_model(FeedbackCycleSeedInput {
        cancellation_evaluation: cancellation_evaluation.as_ref(),
        db,
        evaluation: &evaluation,
        fixture: feedback_cycle_fixture,
        model_version_id,
        observed_at: now,
        registry: &registry,
    })
    .await?;
    let cancellable_research_job_id = seed_cancellable_job(
        db,
        browser_model_spec_id,
        infra.decision_policy_snapshot_id,
        now,
    )
    .await?;
    Ok(BrowserResearchFixture {
        evaluation_dataset_id: evaluation.id,
        backtest_report_id: report,
        feedback_cycle_id: feedback_cycles.historical_cycle_id,
        governed_cancellation_cycle_id: feedback_cycles.governed_cancellation_cycle_id,
        cancellable_research_job_id,
        model_version_id,
    })
}

async fn seed_cancellable_job(
    db: &DatabaseConnection,
    model_spec_id: ModelSpecId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    observed_at: DateTime<Utc>,
) -> QuantResult<ResearchJobId> {
    let repository = PgResearchJobRepository::new(db.clone());
    let job_id = ResearchJobId::from_v7();
    repository
        .enqueue(NewResearchJob {
            job_id,
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::DatasetBuild,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(model_spec_id),
            decision_policy_snapshot_id: Some(decision_policy_snapshot_id),
            params_json: ResearchJobParams::DatasetBuild(BuildTrainingDatasetRequest {
                model_spec_id,
                profile_ref: fixture_profile_ref(),
                purpose: DatasetPurpose::Evaluation,
                decision_policy_snapshot_id,
                fit_seal_id: seeded_uuid("browser-cancellable-fit-seal").into(),
                fit_seal_hash: CanonicalDigest::content_hash_json(&(
                    "browser-cancellable-fit-seal",
                    observed_at,
                ))?,
                window_start: observed_at - Duration::hours(2),
                window_end: observed_at - Duration::hours(1),
                pit_cutoff: observed_at,
                sample_interval_secs: 60,
                horizons_secs: vec![3_600],
                knowledge_lag_secs: KNOWLEDGE_LAG_SECS,
                feature_schema_version: SchemaVersion::FIRST,
                sample_sources: TrainingSampleSources::default(),
                reason: "UI release-closure cancellable research fixture".to_owned(),
                training_dataset_id: Some(TrainingDatasetId::from_v7()),
            }),
            requested_by: Some("ui-release-closure".to_owned()),
            acting_role: RoleCode::new("admin"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        })
        .await?;

    let worker = WorkerId::from_v7();
    let leased = repository
        .lease_next(
            &[ResearchJobKind::DatasetBuild],
            &worker,
            observed_at + Duration::minutes(5),
        )
        .await?
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "cancellable research fixture could not acquire its lease".to_owned(),
        })?;
    if leased.job_id != job_id {
        return Err(ResearchError::DatasetBuild {
            detail: format!(
                "cancellable research fixture leased {}, expected {job_id}",
                leased.job_id
            ),
        }
        .into());
    }
    let scheduled = repository
        .retry_transient(
            &job_id,
            &worker,
            "fixture-controlled retry window".to_owned(),
            StdDuration::from_hours(24),
        )
        .await?;
    match scheduled {
        ResearchJobRetryOutcome::Scheduled(job) if job.job_id == job_id => Ok(job_id),
        ResearchJobRetryOutcome::Scheduled(job) => Err(ResearchError::DatasetBuild {
            detail: format!(
                "cancellable research fixture scheduled {}, expected {job_id}",
                job.job_id
            ),
        }
        .into()),
        ResearchJobRetryOutcome::Exhausted(job) => Err(ResearchError::DatasetBuild {
            detail: format!(
                "cancellable research fixture exhausted unexpectedly: {}",
                job.job_id
            ),
        }
        .into()),
    }
}

impl<'a> FeedbackCycleSeed<'a> {
    async fn persist_model(
        input: FeedbackCycleSeedInput<'a>,
    ) -> QuantResult<PersistedFeedbackCycles> {
        let candidate = input
            .registry
            .find_model_version(&input.model_version_id)
            .await?
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: format!(
                    "browser candidate model {} is missing",
                    input.model_version_id
                ),
            })?;
        Self {
            evaluation: input.evaluation,
            cancellation_evaluation: input.cancellation_evaluation,
            model_family: candidate.model_family,
            champion_model_version_id: candidate.model_version_id,
            champion_serving_contract_hash: candidate.serving_contract_hash,
            observed_at: input.observed_at,
        }
        .persist(input.db, input.fixture)
        .await
    }

    fn seal_cycle(
        &self,
        feedback_policy_hash: ContentHash,
        evaluation: &PersistedDataset,
    ) -> QuantResult<NewFeedbackCycle> {
        let request = &evaluation.feedback_request;
        let source = &request.source_lineage;
        NewFeedbackCycle::try_seal(FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
            profile_ref: fixture_profile_ref(),
            feedback_policy_hash,
            label_cutoff: source.pit_cutoff,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            champion_model_spec_id: request.model_spec_id,
            champion_model_spec_definition_hash: request.model_spec_definition_hash,
            champion_model_family: self.model_family,
            route: BuyModelRoute::Weather,
            decision_policy_snapshot_id: source.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: source.runtime_config_hash,
            policy_bundle_generation: PolicyBundleGeneration::FIRST,
            route_generation: 1,
            evaluation_mode: FeedbackEvaluationMode::Conditional,
            parent_cycle_id: None,
            forced_idempotency_key: None,
        })?)
        .map_err(Into::into)
    }

    async fn persist_trigger(
        &self,
        repository: &PgFeedbackCycleRepository,
        cycle: NewFeedbackCycle,
        trigger_family: FeedbackTriggerFamily,
        actor: &str,
        reason_code: &str,
    ) -> QuantResult<FeedbackCycleInfo> {
        let feedback_cycle_id = cycle.feedback_cycle_id();
        let trigger = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(trigger_family),
            research_job_id: None,
            actor: Some(actor.to_owned()),
            reason_code: Some(reason_code.to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: self.observed_at,
        })?;
        let commit = repository.record_trigger(cycle, trigger).await?;
        match commit {
            FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::Inserted(persisted),
                stage: FeedbackStageWriteOutcome::Inserted(_),
                trigger: FeedbackTriggerWriteOutcome::Inserted(_),
            }
            | FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::AlreadyPresent(persisted),
                stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
                trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
            } => Ok(persisted),
            _ => Err(ResearchError::DatasetBuild {
                detail: format!(
                    "browser feedback trigger outcomes are inconsistent for {feedback_cycle_id}"
                ),
            }
            .into()),
        }
    }

    async fn persist(
        self,
        db: &DatabaseConnection,
        feedback_cycle_fixture: FeedbackCycleFixture,
    ) -> QuantResult<PersistedFeedbackCycles> {
        let profile_ref = fixture_profile_ref();
        let profile = profile_ref
            .resolve_builtin_research_profile()
            .map_err(|detail| ResearchError::DatasetBuild {
                detail: format!("resolve browser feedback profile: {detail}"),
            })?;
        let feedback_policy_hash =
            profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("hash browser feedback policy: {error}"),
                })?;
        let repository = PgFeedbackCycleRepository::new(db.clone());
        let historical = self
            .persist_trigger(
                &repository,
                self.seal_cycle(feedback_policy_hash, self.evaluation)?,
                FeedbackTriggerFamily::Manual,
                "browser_fixture",
                "browser_fixture",
            )
            .await?;
        let feedback_cycle_id = historical.feedback_cycle_id;
        let cancellation_occurred_at = db.statement_time().await;
        let cancellation = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id,
            event_sequence: 2,
            stage: FeedbackStage::Coverage,
            event_kind: FeedbackStageEventKind::CancellationRequested,
            trigger_family: None,
            research_job_id: None,
            actor: Some("browser_fixture".to_owned()),
            reason_code: Some("browser_fixture_cancelled".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: cancellation_occurred_at,
        })?;
        let (cycle_outcome, stage_outcome) = repository
            .request_cancel(FeedbackCycleGeneration::from(&historical), cancellation)
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
        let governed_cancellation_cycle_id = match feedback_cycle_fixture {
            FeedbackCycleFixture::HistoricalOnly => None,
            FeedbackCycleFixture::GovernedCancellation => {
                let cancellation_evaluation =
                    self.cancellation_evaluation
                        .ok_or_else(|| ResearchError::DatasetBuild {
                            detail:
                                "governed browser fixture is missing cancellation evaluation truth"
                                    .to_owned(),
                        })?;
                let scheduled = self
                    .persist_trigger(
                        &repository,
                        self.seal_cycle(feedback_policy_hash, cancellation_evaluation)?,
                        FeedbackTriggerFamily::Scheduled,
                        "browser_fixture_scheduler",
                        "browser_fixture_scheduled",
                    )
                    .await?;
                Some(scheduled.feedback_cycle_id)
            }
        };
        Ok(PersistedFeedbackCycles {
            governed_cancellation_cycle_id,
            historical_cycle_id: feedback_cycle_id,
        })
    }
}

fn model_examples(
    scope: &str,
    window_start: DateTime<Utc>,
    factor_serving_plane: &FactorServingPlane,
    include_trade_policy_labels: bool,
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
                include_trade_policy_labels,
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
    include_trade_policy_labels: bool,
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
        labels: ModelDatasetLedgerFixture::labels(
            strength,
            decision_at + Duration::seconds(1),
            include_trade_policy_labels,
        ),
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
    let mut fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
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
    ModelDatasetLedgerFixture::bind_trade_policy(&mut fixture, seed.trade_policy)?;
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
        coverage: dec!(0.975609756098),
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
        portfolio_funnel: BacktestPortfolioFunnel {
            schema_version: 1,
            decision_tick_count: 4,
            emitted_candidate_count: 82,
            candidate_without_executable_tier_count: 0,
            executable_tier_count: 82,
            admission_rejected_tier_count: 0,
            admitted_tier_count: 82,
            selected_tier_count: 82,
            executed_entry_count: 82,
            resolved_allocation_count: 80,
            no_candidate_tick_count: 0,
            no_executable_tier_tick_count: 0,
            no_selection_tick_count: 0,
            selected_tick_count: 4,
            tier_exclusion_reasons: Vec::new(),
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
    use std::collections::BTreeMap;

    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{FactorServingPlane, model_example};
    use crate::support::execution_pg_seed::CalibratedModelHead;
    use quant_pivot_models::{
        clickhouse::{MarketResolutionFactInput, MarketResolutionRow},
        types::{
            ContentHash, EvmBlockHash, EvmTransactionHash, PayoutRatio, stable_name::FactorName,
        },
    };

    #[test]
    fn resolution_tokens_are_canonical() {
        let factor_serving_plane = FactorServingPlane::try_empty().expect("empty factor plane");
        let example = model_example("contract", 0, 0, Utc::now(), &factor_serving_plane, false)
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

    #[test]
    fn production_control_is_constrained() {
        let CalibratedModelHead::AlphaSimplex(weights) = CalibratedModelHead::feedback_control()
        else {
            panic!("production control must freeze an explicit alpha simplex");
        };
        let expected = BTreeMap::from([
            (FactorName::new("momentum_ema_slope"), dec!(0.03)),
            (FactorName::new("momentum_macd"), dec!(0.15)),
            (FactorName::new("momentum_roc"), dec!(0.03)),
            (FactorName::new("momentum_vol_adjusted"), dec!(0.18)),
            (
                FactorName::new("struct.resolution_proximity_regime"),
                dec!(0.17),
            ),
            (FactorName::new("struct.reversal_after_shock"), dec!(0.44)),
        ]);

        assert_eq!(weights, expected);
        assert_eq!(weights.values().copied().sum::<Decimal>(), Decimal::ONE);
        assert!(!weights.contains_key(&FactorName::new("mean_reversion")));
    }
}
