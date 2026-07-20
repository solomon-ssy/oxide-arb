//! Phase 10.5 research-catalog demo fixtures for full-stack UI validation.
//!
//! Populates training datasets, model specs/versions, backtest + comparison
//! reports, and factor definitions using repository code paths (not raw SQL).
//! Rows are tagged with the `ui-demo-research-*` prefix so they are easy to spot
//! in the admin catalog pages.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        CompleteTrainingDatasetBuild, NewBacktestReport, NewFactorDefinition,
        NewModelComparisonReport, NewModelRun, NewModelVersion, NewTrainingDatasetPlan,
    },
    entities::{
        quant_backtest_report, quant_factor_definition, quant_model_comparison_report,
        quant_model_spec, quant_model_version, quant_training_dataset,
    },
    enums::{
        common::MarketCategory,
        factor::{FactorDefinitionScope, FactorFamily, FactorNormalization},
        model::ModelFamily,
        quant::{
            DatasetPurpose, FactorDirection, ModelRunKind, ModelRunStatus, PublicationStatus,
            TrainingDatasetStatus,
        },
    },
    types::{
        ArtifactUri, BacktestReportId, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DatasetCoverage, DatasetManifest, DecisionPolicySnapshotId, FactorDefinitionId,
        ModelComparisonReportId, ModelInputContract, ModelRunId, ModelSpecId,
        ModelTrainingContract, ModelVersionId, Probability, SchemaVersion, TrainingDatasetId,
        TrainingHorizonsSecs, TrainingSampleSources,
        backtest::{
            CategoryMetric, CategoryMetrics, CategoryRankIcDelta, CategoryRankIcDeltas,
            ExpectedVsRealized, PnlCurvePoint, PnlSimulation,
        },
        default_sample_sources,
        factor::{FactorDefinitionDocument, FactorOutputKind, factor_definition_content_hash},
        model_metrics::{
            HeldOutMetricKind, LearningToRankInSampleMetrics, ModelArtifactTrainingLineage,
            ModelValidationMetrics, ModelVersionMetrics, ObjectiveComponentMetrics,
            RankingDiagnosticsMetrics,
        },
        model_training::{ModelTrainingObjective, TrainingObjectiveSpec},
        stable_name::FactorName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgFactorRepository, PgModelComparisonReportRepository,
        PgModelRegistryRepository, PgModelRunRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, FactorRepository, ModelComparisonReportRepository,
        ModelRegistryRepository, ModelRunRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{hashing::ResearchHasher, training::dataset_manifest_hash};
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};

use crate::{
    execution_pg_seed::{SharedDemoInfra, fixture_profile_ref, source_slice_ref},
    model_spec_fixtures::new_model_spec_fixture,
    seeded_uuid,
};

const RESEARCH_MARKER_SPEC: &str = "ui-demo-research-spec-secondary";
const PRIMARY_SPEC_NAME: &str = "ui-demo-seed-model";

/// Summary printed after seeding the research catalog fixtures.
#[derive(Debug, Default, Clone)]
pub struct ResearchUiSeedSummary {
    /// `true` when fixtures were already present and creation was skipped.
    pub skipped: bool,
    pub datasets: usize,
    pub model_specs: usize,
    pub model_versions: usize,
    pub backtest_reports: usize,
    pub comparison_reports: usize,
    pub factors: usize,
    pub primary_model_spec_id: Option<ModelSpecId>,
    pub dataset_ready_id: Option<TrainingDatasetId>,
    pub baseline_model_version_id: Option<ModelVersionId>,
    pub candidate_model_version_id: Option<ModelVersionId>,
    pub shadow_model_version_id: Option<ModelVersionId>,
    pub retired_model_version_id: Option<ModelVersionId>,
    pub candidate_backtest_report_id: Option<BacktestReportId>,
    pub comparison_report_id: Option<ModelComparisonReportId>,
    pub draft_factor_id: Option<FactorDefinitionId>,
    pub published_factor_id: Option<FactorDefinitionId>,
    pub retired_factor_id: Option<FactorDefinitionId>,
}

/// Seed Postgres research-catalog fixtures for Phase 10.5 UI pages.
pub async fn seed_research_ui_demo_pg(
    db: &DatabaseConnection,
    infra: &SharedDemoInfra,
) -> ResearchUiSeedSummary {
    if research_already_seeded(db).await {
        return load_existing_summary(db).await;
    }

    let registry = PgModelRegistryRepository::new(db.clone());
    let datasets = PgTrainingDatasetRepository::new(db.clone());
    let model_runs = PgModelRunRepository::new(db.clone());
    let backtests = PgBacktestReportRepository::new(db.clone());
    let comparisons = PgModelComparisonReportRepository::new(db.clone());
    let factors = PgFactorRepository::new(db.clone());

    let primary_spec_id = primary_model_spec_id(db).await;
    let primary_spec_definition_hash = registry
        .find_model_spec_by_id(&primary_spec_id)
        .await
        .expect("load primary model spec")
        .expect("primary model spec")
        .definition_hash;
    let mut summary = ResearchUiSeedSummary {
        primary_model_spec_id: Some(primary_spec_id.clone()),
        baseline_model_version_id: Some(infra.model_version_id.clone()),
        ..ResearchUiSeedSummary::default()
    };

    summary.model_specs += seed_secondary_model_spec(&registry).await;
    summary.datasets = seed_training_datasets(
        &datasets,
        &primary_spec_id,
        &primary_spec_definition_hash,
        &infra.decision_policy_snapshot_id,
        &mut summary,
    )
    .await;
    summary.model_versions = seed_model_versions(&registry, &primary_spec_id, &mut summary).await;
    summary.backtest_reports =
        seed_backtest_reports(&backtests, &model_runs, infra, &mut summary).await;
    summary.comparison_reports =
        seed_comparison_report(db, &comparisons, &model_runs, infra, &mut summary).await;
    summary.factors = seed_factor_definitions(&factors, &mut summary).await;

    summary
}

async fn research_already_seeded(db: &DatabaseConnection) -> bool {
    quant_model_spec::Entity::find()
        .filter(quant_model_spec::Column::Name.eq(RESEARCH_MARKER_SPEC))
        .one(db)
        .await
        .expect("probe research marker spec")
        .is_some()
}

async fn primary_model_spec_id(db: &DatabaseConnection) -> ModelSpecId {
    quant_model_spec::Entity::find()
        .filter(quant_model_spec::Column::Name.eq(PRIMARY_SPEC_NAME))
        .one(db)
        .await
        .expect("load primary model spec")
        .expect("ui-demo-seed-model spec must exist — run seed_shared_demo_infra first")
        .model_spec_id
}

async fn load_existing_summary(db: &DatabaseConnection) -> ResearchUiSeedSummary {
    let mut summary = ResearchUiSeedSummary {
        skipped: true,
        ..ResearchUiSeedSummary::default()
    };
    load_existing_primary(db, &mut summary).await;
    load_existing_datasets(db, &mut summary).await;
    load_existing_versions_and_backtests(db, &mut summary).await;
    load_existing_comparison(db, &mut summary).await;
    load_existing_factors(db, &mut summary).await;
    summary
}

async fn load_existing_primary(db: &DatabaseConnection, summary: &mut ResearchUiSeedSummary) {
    if let Some(spec) = quant_model_spec::Entity::find()
        .filter(quant_model_spec::Column::Name.eq(PRIMARY_SPEC_NAME))
        .one(db)
        .await
        .expect("load primary spec")
    {
        summary.primary_model_spec_id = Some(spec.model_spec_id.clone());
        if let Some(baseline) = quant_model_version::Entity::find()
            .filter(quant_model_version::Column::ModelSpecId.eq(spec.model_spec_id))
            .filter(quant_model_version::Column::Version.eq(1))
            .one(db)
            .await
            .expect("load baseline version")
        {
            summary.baseline_model_version_id = Some(baseline.model_version_id);
        }
    }

    summary.model_specs = row_count(
        quant_model_spec::Entity::find()
            .filter(quant_model_spec::Column::Name.starts_with("ui-demo-research"))
            .count(db)
            .await
            .expect("count research specs"),
    );
}

async fn load_existing_datasets(db: &DatabaseConnection, summary: &mut ResearchUiSeedSummary) {
    let dataset_rows = quant_training_dataset::Entity::find()
        .filter(quant_training_dataset::Column::ParquetUri.like("%ui-demo-research%"))
        .all(db)
        .await
        .expect("list research datasets");
    summary.datasets = dataset_rows.len();
    for row in &dataset_rows {
        if row.status == TrainingDatasetStatus::Ready {
            summary.dataset_ready_id = Some(row.training_dataset_id.clone());
        }
    }
}

async fn load_existing_versions_and_backtests(
    db: &DatabaseConnection,
    summary: &mut ResearchUiSeedSummary,
) {
    let Some(primary_spec_id) = summary.primary_model_spec_id.clone() else {
        return;
    };

    let version_rows = quant_model_version::Entity::find()
        .filter(quant_model_version::Column::ModelSpecId.eq(primary_spec_id))
        .filter(quant_model_version::Column::Version.gt(1))
        .all(db)
        .await
        .expect("list research model versions");
    summary.model_versions = version_rows.len();
    for row in &version_rows {
        match row.publication_status {
            PublicationStatus::Candidate => {
                summary.candidate_model_version_id = Some(row.model_version_id.clone());
            }
            PublicationStatus::Shadow => {
                summary.shadow_model_version_id = Some(row.model_version_id.clone());
            }
            PublicationStatus::Retired => {
                summary.retired_model_version_id = Some(row.model_version_id.clone());
            }
            _ => {}
        }
    }

    let mut version_ids: Vec<ModelVersionId> = version_rows
        .iter()
        .map(|row| row.model_version_id.clone())
        .collect();
    if let Some(baseline_id) = &summary.baseline_model_version_id {
        version_ids.push(baseline_id.clone());
    }
    summary.backtest_reports = row_count(
        quant_backtest_report::Entity::find()
            .filter(quant_backtest_report::Column::ModelVersionId.is_in(version_ids))
            .count(db)
            .await
            .expect("count research backtests"),
    );
}

async fn load_existing_comparison(db: &DatabaseConnection, summary: &mut ResearchUiSeedSummary) {
    let comparison_hash = content_hash("comparison-research-pair");
    if let Some(row) = quant_model_comparison_report::Entity::find()
        .filter(quant_model_comparison_report::Column::ComparisonHash.eq(comparison_hash))
        .one(db)
        .await
        .expect("load comparison")
    {
        summary.comparison_reports = 1;
        summary.comparison_report_id = Some(row.comparison_report_id);
        summary.candidate_backtest_report_id = Some(row.candidate_report_id);
    }
}

async fn load_existing_factors(db: &DatabaseConnection, summary: &mut ResearchUiSeedSummary) {
    let factor_rows = quant_factor_definition::Entity::find()
        .filter(quant_factor_definition::Column::Name.starts_with("ui-demo-research"))
        .all(db)
        .await
        .expect("list research factors");
    summary.factors = factor_rows.len();
    for row in &factor_rows {
        if row.name.contains("spread-tightness") {
            summary.draft_factor_id = Some(row.factor_definition_id.clone());
        } else if row.name.contains("liquidity-depth") {
            summary.published_factor_id = Some(row.factor_definition_id.clone());
        } else if row.name.contains("retired-momentum") {
            summary.retired_factor_id = Some(row.factor_definition_id.clone());
        }
    }
}

fn row_count(count: u64) -> usize {
    usize::try_from(count).expect("row count fits in usize")
}

async fn seed_secondary_model_spec(registry: &PgModelRegistryRepository) -> usize {
    let spec = new_model_spec_fixture(
        ModelSpecId::from_v7(),
        RESEARCH_MARKER_SPEC,
        ModelFamily::ClassicalLogisticRegression,
        43_200,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    );
    registry
        .create_model_spec(spec)
        .await
        .expect("secondary model spec");
    1
}

struct TrainingDatasetSeedContext<'a> {
    repo: &'a PgTrainingDatasetRepository,
    model_spec_id: &'a ModelSpecId,
    model_spec_definition_hash: &'a ContentHash,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
}

async fn seed_training_datasets(
    repo: &PgTrainingDatasetRepository,
    model_spec_id: &ModelSpecId,
    model_spec_definition_hash: &ContentHash,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
    let context = TrainingDatasetSeedContext {
        repo,
        model_spec_id,
        model_spec_definition_hash,
        decision_policy_snapshot_id,
    };
    let scenarios: [(&str, TrainingDatasetStatus, i64); 6] = [
        ("planned", TrainingDatasetStatus::Planned, 0),
        ("building", TrainingDatasetStatus::Building, 0),
        ("ready", TrainingDatasetStatus::Ready, 11_200),
        (
            "insufficient",
            TrainingDatasetStatus::InsufficientLabels,
            11_500,
        ),
        ("failed", TrainingDatasetStatus::Failed, 0),
        ("expired", TrainingDatasetStatus::Expired, 9_800),
    ];

    for (slug, status, built_samples) in scenarios {
        let dataset_id = seed_training_dataset_scenario(
            &context,
            slug,
            status,
            built_samples,
            DatasetPurpose::Training,
        )
        .await;

        if status == TrainingDatasetStatus::Ready {
            summary.dataset_ready_id = Some(dataset_id);
        }
    }

    seed_training_dataset_scenario(
        &context,
        "policy-fit-blocked",
        TrainingDatasetStatus::Ready,
        11_200,
        DatasetPurpose::PolicyFit,
    )
    .await;

    scenarios.len() + 1
}

async fn seed_training_dataset_scenario(
    context: &TrainingDatasetSeedContext<'_>,
    slug: &str,
    status: TrainingDatasetStatus,
    built_samples: i64,
    purpose: DatasetPurpose,
) -> TrainingDatasetId {
    let dataset_id = if purpose == DatasetPurpose::PolicyFit {
        TrainingDatasetId::new(seeded_uuid("ui-demo-policy-fit-blocked-dataset"))
    } else {
        TrainingDatasetId::from_v7()
    };
    let now = if purpose == DatasetPurpose::PolicyFit {
        "2026-07-14T00:00:00Z"
            .parse::<DateTime<Utc>>()
            .expect("fixed policy-fit fixture time")
    } else {
        Utc::now()
    };
    let (window_start, window_end) = if purpose == DatasetPurpose::PolicyFit {
        let end = now - ChronoDuration::days(1);
        (end - ChronoDuration::days(90), end)
    } else {
        let start = now - ChronoDuration::days(14);
        (start, start + ChronoDuration::days(7))
    };
    let horizons_secs = if purpose == DatasetPurpose::PolicyFit {
        vec![86_400]
    } else {
        vec![3_600, 86_400]
    };
    let hash = content_hash(&format!("dataset-{slug}"));
    context
        .repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: context.model_spec_id.clone(),
            model_spec_definition_hash: context.model_spec_definition_hash.clone(),
            window_start,
            window_end,
            purpose,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(horizons_secs.clone()),
            feature_schema_version: Some(SchemaVersion::new(7)),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            decision_policy_snapshot_id: context.decision_policy_snapshot_id.clone(),
        })
        .await
        .expect("create training dataset");

    if status != TrainingDatasetStatus::Planned {
        context
            .repo
            .start_build(&dataset_id)
            .await
            .expect("start training dataset build");
    }
    match status {
        TrainingDatasetStatus::Ready
        | TrainingDatasetStatus::InsufficientLabels
        | TrainingDatasetStatus::Expired => {
            let manifest = DatasetManifest {
                format_version: DATASET_ARTIFACT_FORMAT_VERSION,
                training_dataset_id: dataset_id.clone(),
                profile_ref: fixture_profile_ref(),
                research_program_hash: content_hash(&format!("dataset-program-{slug}")),
                source_slice: source_slice_ref('5'),
                model_spec_id: context.model_spec_id.clone(),
                model_spec_definition_hash: context.model_spec_definition_hash.clone(),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
                decision_policy_snapshot_id: context.decision_policy_snapshot_id.clone(),
                window_start,
                window_end,
                purpose,
                knowledge_lag_secs: 10,
                sample_interval_secs: 3_600,
                horizons_secs,
                feature_schema_hash: hash.clone(),
                factor_schema_hash: hash.clone(),
                label_schema_hash: hash.clone(),
                semantic_dataset_hash: hash.clone(),
                source_fingerprint: content_hash(&format!("dataset-sources-{slug}")),
                sample_count: u64::try_from(built_samples).expect("non-negative sample count"),
            };
            let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
            context
                .repo
                .complete_build(
                    &dataset_id,
                    CompleteTrainingDatasetBuild {
                        status: if status == TrainingDatasetStatus::Expired {
                            TrainingDatasetStatus::Ready
                        } else {
                            status
                        },
                        feature_schema_hash: hash.clone(),
                        factor_schema_hash: hash.clone(),
                        label_schema_hash: hash.clone(),
                        dataset_hash: hash,
                        manifest_hash,
                        manifest_json: manifest,
                        artifact_bytes_hash: content_hash(&format!("dataset-bytes-{slug}")),
                        parquet_uri: ArtifactUri::parse(format!(
                            "file:///tmp/ui-demo-research-{slug}.parquet"
                        ))
                        .expect("parquet uri"),
                        sample_count: built_samples,
                        coverage_json: dataset_coverage(slug, built_samples),
                        failure_detail: None,
                    },
                )
                .await
                .expect("complete training dataset build");
            if status == TrainingDatasetStatus::Expired {
                context
                    .repo
                    .expire(&dataset_id)
                    .await
                    .expect("expire training dataset");
            }
        }
        TrainingDatasetStatus::Failed => {
            context
                .repo
                .fail_build(&dataset_id, "seeded build failure".to_owned())
                .await
                .expect("fail training dataset build");
        }
        TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => {}
    }
    dataset_id
}

async fn seed_model_versions(
    registry: &PgModelRegistryRepository,
    model_spec_id: &ModelSpecId,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
    let ready_dataset = summary.dataset_ready_id.clone().expect("ready dataset id");

    let candidate_id = create_model_version(
        registry,
        model_spec_id,
        PublicationStatus::Candidate,
        Some(ready_dataset.clone()),
        "candidate",
        demo_metrics(dec!(0.31), dec!(0.58)),
    )
    .await;
    summary.candidate_model_version_id = Some(candidate_id.clone());

    let shadow_id = create_model_version(
        registry,
        model_spec_id,
        PublicationStatus::Candidate,
        Some(ready_dataset),
        "shadow",
        demo_metrics(dec!(0.27), dec!(0.55)),
    )
    .await;
    registry
        .promote_model_to_shadow(&shadow_id)
        .await
        .expect("promote shadow");
    summary.shadow_model_version_id = Some(shadow_id);

    let retired_id = create_model_version(
        registry,
        model_spec_id,
        PublicationStatus::Published,
        None,
        "retired",
        demo_metrics(dec!(0.22), dec!(0.52)),
    )
    .await;
    registry
        .retire_model_version(&retired_id)
        .await
        .expect("retire model version");
    summary.retired_model_version_id = Some(retired_id);

    3
}

async fn create_model_version(
    registry: &PgModelRegistryRepository,
    model_spec_id: &ModelSpecId,
    publication_status: PublicationStatus,
    training_dataset_id: Option<TrainingDatasetId>,
    artifact_seed: &str,
    metrics: ModelVersionMetrics,
) -> ModelVersionId {
    let version = registry
        .next_version_for_spec(model_spec_id)
        .await
        .expect("next version");
    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id: model_spec_id.clone(),
            version,
            artifact_hash: content_hash(&format!("artifact-research-{artifact_seed}")),
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics,
            training_objective: ModelTrainingObjective::learning_to_rank(
                TrainingObjectiveSpec::default(),
            ),
            quality_gate_report: None,
            publication_status,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("create model version");
    model_version_id
}

async fn seed_backtest_reports(
    backtests: &PgBacktestReportRepository,
    model_runs: &PgModelRunRepository,
    infra: &SharedDemoInfra,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
    let baseline_id = summary
        .baseline_model_version_id
        .clone()
        .expect("baseline version");
    let candidate_id = summary
        .candidate_model_version_id
        .clone()
        .expect("candidate version");

    seed_one_backtest(
        backtests,
        model_runs,
        &baseline_id,
        &infra.decision_policy_snapshot_id,
        BacktestSeedConfig {
            seed: "baseline",
            rank_ic: dec!(0.24),
            sharpe: dec!(0.85),
            hit_rate: dec!(0.54),
            sample_count: 8_400,
        },
    )
    .await;

    let candidate_report_id = seed_one_backtest(
        backtests,
        model_runs,
        &candidate_id,
        &infra.decision_policy_snapshot_id,
        BacktestSeedConfig {
            seed: "candidate",
            rank_ic: dec!(0.33),
            sharpe: dec!(1.12),
            hit_rate: dec!(0.61),
            sample_count: 11_200,
        },
    )
    .await;
    summary.candidate_backtest_report_id = Some(candidate_report_id);

    seed_one_backtest(
        backtests,
        model_runs,
        summary
            .shadow_model_version_id
            .as_ref()
            .expect("shadow version"),
        &infra.decision_policy_snapshot_id,
        BacktestSeedConfig {
            seed: "shadow",
            rank_ic: dec!(0.29),
            sharpe: dec!(0.97),
            hit_rate: dec!(0.57),
            sample_count: 10_100,
        },
    )
    .await;

    3
}

struct BacktestSeedConfig {
    seed: &'static str,
    rank_ic: rust_decimal::Decimal,
    sharpe: rust_decimal::Decimal,
    hit_rate: rust_decimal::Decimal,
    sample_count: i64,
}

async fn seed_one_backtest(
    backtests: &PgBacktestReportRepository,
    model_runs: &PgModelRunRepository,
    model_version_id: &ModelVersionId,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    config: BacktestSeedConfig,
) -> BacktestReportId {
    let model_run_id =
        seed_backtest_run(model_runs, model_version_id, decision_policy_snapshot_id).await;
    let backtest_report_id = BacktestReportId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(7);
    backtests
        .create(NewBacktestReport {
            backtest_report_id: backtest_report_id.clone(),
            model_version_id: model_version_id.clone(),
            model_run_id,
            decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
            window_start,
            window_end: window_start + ChronoDuration::days(7),
            coverage: dec!(0.94),
            sample_count: config.sample_count,
            missing_feature_count: 42,
            rank_ic: config.rank_ic,
            sharpe: config.sharpe,
            hit_rate: Probability::new(config.hit_rate),
            expected_vs_realized: expected_vs_realized(),
            max_drawdown: dec!(0.11),
            turnover: dec!(0.18),
            liquidity_feasibility: Probability::new(dec!(0.97)),
            category_breakdown: category_breakdown(),
            tail_loss: dec!(-125),
            report_pnl_simulation: pnl_simulation(window_start + ChronoDuration::days(7)),
            report_hash: content_hash(&format!("backtest-research-{}", config.seed)),
            parquet_uri: Some(format!(
                "file:///tmp/ui-demo-research-backtest-{}.parquet",
                config.seed
            )),
        })
        .await
        .expect("create backtest report");
    backtest_report_id
}

async fn seed_backtest_run(
    model_runs: &PgModelRunRepository,
    model_version_id: &ModelVersionId,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(7);
    model_runs
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id.clone()),
            decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::days(7),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash("backtest-run-input"),
            output_hash: Some(content_hash("backtest-run-output")),
            error_code: None,
            error_message: None,
            started_at: Utc::now() - ChronoDuration::hours(1),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("create backtest run");
    model_run_id
}

async fn seed_comparison_report(
    db: &DatabaseConnection,
    comparisons: &PgModelComparisonReportRepository,
    model_runs: &PgModelRunRepository,
    infra: &SharedDemoInfra,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
    let baseline_version = summary
        .baseline_model_version_id
        .clone()
        .expect("baseline version");
    let candidate_version = summary
        .candidate_model_version_id
        .clone()
        .expect("candidate version");
    let candidate_report = summary
        .candidate_backtest_report_id
        .clone()
        .expect("candidate backtest");

    let baseline_report = find_backtest_for_version(db, &baseline_version).await;

    let model_run_id = seed_backtest_run(
        model_runs,
        &candidate_version,
        &infra.decision_policy_snapshot_id,
    )
    .await;

    let comparison_report_id = ModelComparisonReportId::from_v7();
    comparisons
        .create(NewModelComparisonReport {
            comparison_report_id: comparison_report_id.clone(),
            baseline_model_version_id: baseline_version,
            candidate_model_version_id: candidate_version,
            baseline_report_id: baseline_report,
            candidate_report_id: candidate_report,
            model_run_id,
            rank_ic_delta: dec!(0.09),
            hit_rate_delta: dec!(0.07),
            realized_pnl_delta: dec!(420),
            score_correlation: dec!(0.91),
            side_disagreement_rate: dec!(0.12),
            common_samples: 9_800,
            category_breakdown_diff: category_breakdown_diff(),
            comparison_hash: content_hash("comparison-research-pair"),
        })
        .await
        .expect("create comparison report");
    summary.comparison_report_id = Some(comparison_report_id);
    1
}

async fn find_backtest_for_version(
    db: &DatabaseConnection,
    model_version_id: &ModelVersionId,
) -> BacktestReportId {
    quant_backtest_report::Entity::find()
        .filter(quant_backtest_report::Column::ModelVersionId.eq(model_version_id.clone()))
        .order_by_desc(quant_backtest_report::Column::CreatedAt)
        .one(db)
        .await
        .expect("load baseline backtest")
        .expect("baseline backtest row")
        .backtest_report_id
}

async fn seed_factor_definitions(
    factors: &PgFactorRepository,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
    let draft_id = seed_factor(
        factors,
        "ui-demo-research-spread-tightness",
        FactorFamily::Microstructure,
        FactorDefinitionScope::Generic,
        PublicationStatus::Draft,
    )
    .await;
    summary.draft_factor_id = Some(draft_id);

    let published_id = seed_factor(
        factors,
        "ui-demo-research-liquidity-depth",
        FactorFamily::Liquidity,
        FactorDefinitionScope::Generic,
        PublicationStatus::Draft,
    )
    .await;
    factors
        .publish_definition(&published_id)
        .await
        .expect("publish liquidity factor");
    summary.published_factor_id = Some(published_id);

    let retired_id = seed_factor(
        factors,
        "ui-demo-research-retired-momentum",
        FactorFamily::Momentum,
        FactorDefinitionScope::Generic,
        PublicationStatus::Draft,
    )
    .await;
    factors
        .publish_definition(&retired_id)
        .await
        .expect("publish momentum factor");
    factors
        .retire_definition(&retired_id)
        .await
        .expect("retire momentum factor");
    summary.retired_factor_id = Some(retired_id);

    3
}

async fn seed_factor(
    factors: &PgFactorRepository,
    name: &str,
    factor_family: FactorFamily,
    scope: FactorDefinitionScope,
    status: PublicationStatus,
) -> FactorDefinitionId {
    let feature_contract_hash = content_hash("ui-demo-feature-contract");
    let definition = FactorDefinitionDocument {
        name: FactorName::new(name),
        family: factor_family,
        input_features: Vec::new(),
        output_kind: FactorOutputKind::NormalizedScore,
        default_direction: FactorDirection::Positive,
        normalization: FactorNormalization::Rank,
        owner: "ui-demo-research-seed".to_owned(),
        quality_gates: Vec::new(),
    };
    let definition_hash = factor_definition_content_hash(&definition, &feature_contract_hash)
        .expect("canonical demo factor definition hash");
    let factor_definition_id = FactorDefinitionId::from_definition_hash(&definition_hash);
    let row = factors
        .create_definition(NewFactorDefinition {
            factor_definition_id: factor_definition_id.clone(),
            definition_hash,
            feature_contract_hash,
            name: name.to_owned(),
            factor_family,
            scope,
            input_schema_version: SchemaVersion::FIRST,
            output_schema_version: SchemaVersion::FIRST,
            definition,
            status,
            created_by: None,
        })
        .await
        .expect("create factor definition");
    row.factor_definition_id
}

fn content_hash(seed: &str) -> ContentHash {
    ResearchHasher::canonical(&seed).expect("canonical demo hash")
}

fn demo_metrics(
    rank_ic: rust_decimal::Decimal,
    hit_rate: rust_decimal::Decimal,
) -> ModelVersionMetrics {
    let components = ObjectiveComponentMetrics {
        rank_loss: dec!(0.024),
        tail_penalty: dec!(0.01),
        turnover_penalty: dec!(0.01),
        l2_penalty: dec!(0.004),
        total_loss: dec!(0.024),
        group_count: 42,
        rank_loss_group_count: 42,
        pair_count: 1_024,
    };
    let diagnostics = RankingDiagnosticsMetrics {
        mean_rank_ic: rank_ic,
        mean_ndcg_at_k: hit_rate,
        ndcg_k: 20,
        group_count: 42,
    };
    ModelVersionMetrics::learning_to_rank(
        LearningToRankInSampleMetrics {
            objective_value: -components.total_loss,
            components: components.clone(),
            diagnostics: Some(diagnostics.clone()),
            summary: "seeded UI demonstration metrics".to_owned(),
        },
        ModelValidationMetrics {
            held_out_objective: rank_ic,
            held_out_components: Some(components.clone()),
            held_out_diagnostics: Some(diagnostics),
            fold_objectives: vec![rank_ic],
            fold_components: vec![components],
            sample_count: 10_000,
            dropped_singleton_groups: 0,
            dropped_singleton_rows: 0,
            coordinate_search_effective_trials: 1,
            held_out_metric: HeldOutMetricKind::NegativeTotalLearningToRankLoss,
        },
        ModelArtifactTrainingLineage::FactorNative {
            training_dataset_hash: content_hash("ui-demo-metrics-dataset"),
            training_input_hash: content_hash("ui-demo-metrics-input"),
            input_contract_hash: content_hash("ui-demo-metrics-contract"),
            input_transform_hash: content_hash("ui-demo-metrics-transform"),
            factor_inputs: vec![FactorName::from_static("liquidity_depth")],
        },
    )
}

fn dataset_coverage(_slug: &str, built_samples: i64) -> DatasetCoverage {
    let built = u64::try_from(built_samples).expect("demo dataset sample count is non-negative");
    DatasetCoverage {
        planned_samples: 12_000,
        built_examples: built,
        markets: 42,
        labels_available: built,
        ..DatasetCoverage::default()
    }
}

const fn expected_vs_realized() -> ExpectedVsRealized {
    ExpectedVsRealized {
        mean_expected_bps: dec!(125),
        mean_realized_bps: dec!(116),
        correlation: dec!(0.72),
        bias_bps: dec!(9),
    }
}

fn category_breakdown() -> CategoryMetrics {
    vec![
        CategoryMetric {
            category: MarketCategory::Politics,
            rank_ic: dec!(0.35),
            hit_rate: Probability::new(dec!(0.62)),
            sample_count: 4_500,
            mean_realized_bps: dec!(132),
        },
        CategoryMetric {
            category: MarketCategory::Sports,
            rank_ic: dec!(0.28),
            hit_rate: Probability::new(dec!(0.58)),
            sample_count: 3_200,
            mean_realized_bps: dec!(104),
        },
        CategoryMetric {
            category: MarketCategory::Crypto,
            rank_ic: dec!(0.21),
            hit_rate: Probability::new(dec!(0.51)),
            sample_count: 2_100,
            mean_realized_bps: dec!(75),
        },
    ]
    .into()
}

fn category_breakdown_diff() -> CategoryRankIcDeltas {
    vec![
        CategoryRankIcDelta {
            category: MarketCategory::Politics,
            baseline_rank_ic: dec!(0.27),
            candidate_rank_ic: dec!(0.35),
            rank_ic_delta: dec!(0.08),
        },
        CategoryRankIcDelta {
            category: MarketCategory::Sports,
            baseline_rank_ic: dec!(0.25),
            candidate_rank_ic: dec!(0.28),
            rank_ic_delta: dec!(0.03),
        },
    ]
    .into()
}

fn pnl_simulation(decision_at: DateTime<Utc>) -> PnlSimulation {
    PnlSimulation {
        total_allocated_usd: dec!(10_000),
        realized_pnl_usd: dec!(1250.50),
        gross_return: dec!(0.12505),
        pnl_curve: vec![PnlCurvePoint {
            decision_at,
            cumulative_realized_pnl_usd: dec!(1250.50),
        }],
    }
}
