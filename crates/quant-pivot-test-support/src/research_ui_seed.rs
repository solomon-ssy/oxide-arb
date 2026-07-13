//! Phase 10.5 research-catalog demo fixtures for full-stack UI validation.
//!
//! Populates training datasets, model specs/versions, backtest + comparison
//! reports, and factor definitions using repository code paths (not raw SQL).
//! Rows are tagged with the `ui-demo-research-*` prefix so they are easy to spot
//! in the admin catalog pages.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        CompleteTrainingDatasetBuild, NewBacktestReport, NewFactorDefinition,
        NewModelComparisonReport, NewModelRun, NewModelSpec, NewModelVersion,
        NewTrainingDatasetPlan,
    },
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        model::ModelFamily,
        quant::{
            DatasetPurpose, ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus,
        },
    },
    types::{
        ArtifactUri, BacktestReportId, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION,
        DatasetCoverage, DatasetManifest, FactorDefinitionId, ModelComparisonReportId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        Probability, RuntimeConfigVersionId, SchemaVersion, TrainingDatasetId,
        TrainingHorizonsSecs, TrainingSampleSources, default_sample_sources,
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
use quant_pivot_research::training::dataset_manifest_hash;
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};

use crate::execution_pg_seed::SharedDemoInfra;

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
    let mut summary = ResearchUiSeedSummary {
        primary_model_spec_id: Some(primary_spec_id.clone()),
        baseline_model_version_id: Some(infra.model_version_id.clone()),
        ..ResearchUiSeedSummary::default()
    };

    summary.model_specs += seed_secondary_model_spec(&registry).await;
    summary.datasets = seed_training_datasets(
        &datasets,
        &primary_spec_id,
        &infra.runtime_config_version_id,
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
    use quant_pivot_models::entities::quant_model_spec;

    quant_model_spec::Entity::find()
        .filter(quant_model_spec::Column::Name.eq(RESEARCH_MARKER_SPEC))
        .one(db)
        .await
        .expect("probe research marker spec")
        .is_some()
}

async fn primary_model_spec_id(db: &DatabaseConnection) -> ModelSpecId {
    use quant_pivot_models::entities::quant_model_spec;

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
    use quant_pivot_models::entities::{quant_model_spec, quant_model_version};

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
    use quant_pivot_models::entities::quant_training_dataset;

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
    use quant_pivot_models::entities::{quant_backtest_report, quant_model_version};

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
    use quant_pivot_models::entities::quant_model_comparison_report;

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
    use quant_pivot_models::entities::quant_factor_definition;

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
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: ModelSpecId::from_v7(),
            name: RESEARCH_MARKER_SPEC.to_owned(),
            model_family: ModelFamily::ClassicalLogisticRegression,
            prediction_horizon_secs: 43_200,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({
                "ui_demo": true,
                "notes": "secondary spec for model-spec catalog filters"
            }),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Draft,
        })
        .await
        .expect("secondary model spec");
    1
}

async fn seed_training_datasets(
    repo: &PgTrainingDatasetRepository,
    model_spec_id: &ModelSpecId,
    runtime_config_version_id: &RuntimeConfigVersionId,
    summary: &mut ResearchUiSeedSummary,
) -> usize {
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
            repo,
            model_spec_id,
            runtime_config_version_id,
            slug,
            status,
            built_samples,
        )
        .await;

        if status == TrainingDatasetStatus::Ready {
            summary.dataset_ready_id = Some(dataset_id);
        }
    }

    scenarios.len()
}

async fn seed_training_dataset_scenario(
    repo: &PgTrainingDatasetRepository,
    model_spec_id: &ModelSpecId,
    runtime_config_version_id: &RuntimeConfigVersionId,
    slug: &str,
    status: TrainingDatasetStatus,
    built_samples: i64,
) -> TrainingDatasetId {
    let dataset_id = TrainingDatasetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(14);
    let hash = content_hash(&format!("dataset-{slug}"));
    repo.create_plan(NewTrainingDatasetPlan {
        training_dataset_id: dataset_id.clone(),
        model_spec_id: model_spec_id.clone(),
        window_start,
        window_end: window_start + ChronoDuration::days(7),
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3_600,
        horizons_secs: TrainingHorizonsSecs(vec![3_600, 86_400]),
        feature_schema_version: Some(SchemaVersion::new(6)),
        sample_sources: Some(TrainingSampleSources(default_sample_sources())),
        runtime_config_version_id: runtime_config_version_id.clone(),
    })
    .await
    .expect("create training dataset");

    if status != TrainingDatasetStatus::Planned {
        repo.start_build(&dataset_id)
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
                model_spec_id: model_spec_id.clone(),
                runtime_config_version_id: runtime_config_version_id.clone(),
                window_start,
                window_end: window_start + ChronoDuration::days(7),
                purpose: DatasetPurpose::Training,
                knowledge_lag_secs: 10,
                sample_interval_secs: 3_600,
                horizons_secs: vec![3_600, 86_400],
                feature_schema_hash: hash.clone(),
                factor_schema_hash: hash.clone(),
                label_schema_hash: hash.clone(),
                semantic_dataset_hash: hash.clone(),
                source_fingerprint: content_hash(&format!("dataset-sources-{slug}")),
                sample_count: u64::try_from(built_samples).expect("non-negative sample count"),
            };
            let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
            repo.complete_build(
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
                repo.expire(&dataset_id)
                    .await
                    .expect("expire training dataset");
            }
        }
        TrainingDatasetStatus::Failed => {
            repo.fail_build(&dataset_id, "seeded build failure".to_owned())
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
    metrics_json: serde_json::Value,
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
            training_dataset_id,
            publish_path_set_id: None,
            metrics_json,
            training_objective_json: serde_json::json!({
                "rank_loss": "rank_ic_weighted_ranknet",
                "optimizer": "coordinate_search",
                "lambda_tail": "0.5",
                "tail_fraction": "0.10",
                "lambda_turnover": "0.2",
                "lambda_l2": "0.01",
                "ndcg_k": 20,
                "pseudo_top_n": 20,
            }),
            quality_gate_report: serde_json::json!({
                "passed": true,
                "ui_demo": true,
                "artifact_seed": artifact_seed,
            }),
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
        &infra.runtime_config_version_id,
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
        &infra.runtime_config_version_id,
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
        &infra.runtime_config_version_id,
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
    runtime_config_version_id: &RuntimeConfigVersionId,
    config: BacktestSeedConfig,
) -> BacktestReportId {
    let model_run_id =
        seed_backtest_run(model_runs, model_version_id, runtime_config_version_id).await;
    let backtest_report_id = BacktestReportId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(7);
    backtests
        .create(NewBacktestReport {
            backtest_report_id: backtest_report_id.clone(),
            model_version_id: model_version_id.clone(),
            model_run_id,
            runtime_config_version_id: runtime_config_version_id.clone(),
            window_start,
            window_end: window_start + ChronoDuration::days(7),
            coverage: dec!(0.94),
            sample_count: config.sample_count,
            missing_feature_count: 42,
            rank_ic: config.rank_ic,
            sharpe: config.sharpe,
            hit_rate: Probability::new(config.hit_rate),
            expected_vs_realized: expected_vs_realized_json(config.seed),
            max_drawdown: dec!(0.11),
            turnover: dec!(0.18),
            liquidity_feasibility: Probability::new(dec!(0.97)),
            category_breakdown: category_breakdown_json(),
            tail_loss: dec!(-125),
            report_pnl_simulation: pnl_simulation_json(config.seed),
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
    runtime_config_version_id: &RuntimeConfigVersionId,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(7);
    model_runs
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id.clone()),
            runtime_config_version_id: runtime_config_version_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::days(7),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash("backtest-run-input"),
            output_hash: Some(content_hash("backtest-run-output")),
            metrics_json: serde_json::json!({ "ui_demo": true }),
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
        &infra.runtime_config_version_id,
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
            category_breakdown_diff: category_breakdown_diff_json(),
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
    use quant_pivot_models::entities::quant_backtest_report;

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
    let definition_hash = content_hash(&format!("factor-definition-{name}"));
    let factor_definition_id = FactorDefinitionId::from_definition_hash(&definition_hash);
    let row = factors
        .create_definition(NewFactorDefinition {
            factor_definition_id: factor_definition_id.clone(),
            definition_hash,
            feature_contract_hash: content_hash("ui-demo-feature-contract"),
            name: name.to_owned(),
            factor_family,
            scope,
            input_schema_version: SchemaVersion::FIRST,
            output_schema_version: SchemaVersion::FIRST,
            definition_json: serde_json::json!({
                "ui_demo": true,
                "name": name,
                "description": format!("UI demo factor `{name}` for catalog validation"),
            }),
            status,
            created_by: None,
        })
        .await
        .expect("create factor definition");
    row.factor_definition_id
}

fn content_hash(seed: &str) -> ContentHash {
    let mut hex = seed.as_bytes().iter().fold(String::new(), |mut acc, byte| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{byte:02x}");
        acc
    });
    if hex.len() > 64 {
        hex.truncate(64);
    } else {
        hex.push_str(&"0".repeat(64usize.saturating_sub(hex.len())));
    }
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

fn demo_metrics(
    rank_ic: rust_decimal::Decimal,
    hit_rate: rust_decimal::Decimal,
) -> serde_json::Value {
    serde_json::json!({
        "rank_ic": rank_ic,
        "hit_rate": hit_rate,
        "validation_loss": 0.024,
        "ui_demo": true,
    })
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

fn expected_vs_realized_json(seed: &str) -> serde_json::Value {
    serde_json::json!({
        "ui_demo_seed": seed,
        "correlation": 0.72,
        "buckets": [
            {"decile": 1, "expected_return_bps": 45, "realized_return_bps": 38, "samples": 980},
            {"decile": 5, "expected_return_bps": 120, "realized_return_bps": 112, "samples": 1020},
            {"decile": 10, "expected_return_bps": 210, "realized_return_bps": 198, "samples": 990},
        ],
    })
}

fn category_breakdown_json() -> serde_json::Value {
    serde_json::json!([
        {
            "category": "politics",
            "rank_ic": 0.35,
            "hit_rate": 0.62,
            "sample_count": 4500,
        },
        {
            "category": "sports",
            "rank_ic": 0.28,
            "hit_rate": 0.58,
            "sample_count": 3200,
        },
        {
            "category": "crypto",
            "rank_ic": 0.21,
            "hit_rate": 0.51,
            "sample_count": 2100,
        },
    ])
}

fn category_breakdown_diff_json() -> serde_json::Value {
    serde_json::json!([
        {
            "category": "politics",
            "rank_ic_delta": 0.08,
            "hit_rate_delta": 0.05,
            "sample_count": 4500,
        },
        {
            "category": "sports",
            "rank_ic_delta": 0.03,
            "hit_rate_delta": 0.02,
            "sample_count": 3200,
        },
    ])
}

fn pnl_simulation_json(seed: &str) -> serde_json::Value {
    serde_json::json!({
        "ui_demo_seed": seed,
        "cumulative_pnl_usd": 1250.50,
        "max_drawdown_pct": 0.08,
        "sharpe": 1.42,
        "win_rate": 0.59,
        "trade_count": 842,
    })
}
