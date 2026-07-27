//! Coherent research lineage for the real-binary browser fixture.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        data_plane::DecisionClock,
        quant::{NewBacktestReport, NewModelRun},
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{DataQualityStatus, DatasetPurpose, ModelRunKind, TrainingDatasetStatus},
    },
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, EventId, FeatureCell,
        FeatureStaleness, FeatureValue, MarketId, ModelRunId, ModelSpecId, ModelVersionId,
        Probability, ResearchEvaluationTrack, SchemaVersion, TokenId, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, TrainingSampleSources, Usd,
        backtest::{
            CategoryMetric, CategoryMetrics, ExpectedVsRealized, PnlCurvePoint, PnlSimulation,
        },
        factor::{FactorExplanation, FactorServingPlane},
        model_serving::ModelServingTradePolicyBinding,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, ModelRegistryRepository, ModelRunRepository,
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
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_source_fingerprint,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use super::{
    execution_pg_seed::{
        CalibratedModelSeed, SharedDemoInfra, fixture_profile_ref, seed_calibrated_model,
    },
    research_fixtures::{
        DatasetLedgerFixture, DatasetLedgerSeed, ReplayableSourceSliceFixture,
        bind_fixture_decision_capture, fixture_factor_plane, model_learning_cohort,
        persist_replayable_source_slice, seed_source_manifest,
    },
};

const TICK_COUNT: i64 = 4;
const MARKETS_PER_TICK: usize = 20;
const TICK_INTERVAL_SECS: i64 = 3_600;
const KNOWLEDGE_LAG_SECS: u64 = 10;

/// Stable identifiers exposed through the browser fixture's real REST API.
pub struct BrowserResearchFixture {
    pub evaluation_dataset_id: TrainingDatasetId,
    pub backtest_report_id: BacktestReportId,
    pub model_version_id: ModelVersionId,
}

struct PersistedDataset {
    id: TrainingDatasetId,
    semantic_hash: ContentHash,
}

struct DatasetSeed {
    scope: String,
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    factor_serving_plane: FactorServingPlane,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    runtime_config_hash: ContentHash,
    trade_policy: ModelServingTradePolicyBinding,
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
    let feature_schema_hash =
        CanonicalDigest::content_hash_json(&"browser-research-feature-schema-v1")?;
    let factor_serving_plane = if model_spec.model_family.is_classical() {
        FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
            detail: format!("browser classical factor plane is invalid: {error}"),
        })?
    } else {
        fixture_factor_plane(feature_schema_hash, SchemaVersion::FIRST)?
    };
    let label_schema_hash =
        CanonicalDigest::content_hash_json(&"browser-research-label-schema-v1")?;
    let observed_at = Utc::now();
    let now = DateTime::from_timestamp_millis(observed_at.timestamp_millis()).ok_or_else(|| {
        ResearchError::DatasetBuild {
            detail: "browser research fixture time does not fit millisecond precision".to_owned(),
        }
    })?;
    let policy_snapshot_hash = template
        .serving_contract
        .bindings()
        .policy_snapshot
        .snapshot_hash;
    let trade_policy = ModelServingTradePolicyBinding {
        artifact_id: infra.trade_policy.artifact_id,
        content_hash: infra.trade_policy.artifact_hash,
    };
    let training = persist_dataset(
        db,
        store,
        DatasetSeed {
            scope: "browser-research-training".to_owned(),
            model_spec_id: template.model_spec_id,
            model_family: model_spec.model_family,
            model_spec_definition_hash: model_spec.definition_hash,
            factor_serving_plane: factor_serving_plane.clone(),
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: policy_snapshot_hash,
            trade_policy: trade_policy.clone(),
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
            model_spec_id: template.model_spec_id,
            model_spec_definition_hash: model_spec.definition_hash,
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
            model_spec_id: template.model_spec_id,
            model_family: model_spec.model_family,
            model_spec_definition_hash: model_spec.definition_hash,
            factor_serving_plane: factor_serving_plane.clone(),
            decision_policy_snapshot_id: infra.decision_policy_snapshot_id,
            runtime_config_hash: policy_snapshot_hash,
            trade_policy,
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
        evaluation.id,
        infra.decision_policy_snapshot_id,
        now,
    )
    .await?;
    Ok(BrowserResearchFixture {
        evaluation_dataset_id: evaluation.id,
        backtest_report_id: report,
        model_version_id,
    })
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
    let token_id = TokenId::new(format!("browser-{scope}-token-{tick}-{ordinal}"));
    let secondary_token_id = TokenId::new(format!("browser-{scope}-token-no-{tick}-{ordinal}"));
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
    fixture.manifest.trade_policy_artifact_id = Some(seed.trade_policy.artifact_id);
    fixture.manifest.trade_policy_hash = Some(seed.trade_policy.content_hash);
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
    Ok(PersistedDataset {
        id: dataset_id,
        semantic_hash,
    })
}

async fn seed_backtest_report(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    evaluation_dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    now: DateTime<Utc>,
) -> QuantResult<BacktestReportId> {
    let model_run_id = ModelRunId::from_v7();
    let window_start = now - Duration::days(30);
    let window_end = window_start + Duration::hours(TICK_COUNT * TICK_INTERVAL_SECS / 3_600);
    let input_hash = CanonicalDigest::content_hash_json(&(
        "browser-research-backtest-input-v1",
        model_version_id,
        evaluation_dataset_id,
    ))?;
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
            input_hash,
        })
        .await?;
    let backtest_report_id = BacktestReportId::from_v7();
    let report_hash = CanonicalDigest::content_hash_json(&(
        "browser-research-backtest-report-v1",
        backtest_report_id,
        input_hash,
    ))?;
    let curve = [(0, -18), (45, 4), (90, 16), (135, 11), (180, 31), (225, 47)]
        .into_iter()
        .map(|(minutes, pnl)| PnlCurvePoint {
            decision_at: window_start + Duration::minutes(minutes),
            cumulative_realized_pnl_usd: Decimal::from(pnl),
        })
        .collect();
    PgBacktestReportRepository::new(db.clone())
        .create(NewBacktestReport {
            backtest_report_id,
            model_version_id,
            evaluation_dataset_id,
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
            report_hash,
            parquet_uri: None,
        })
        .await?;
    model_runs
        .succeed(&model_run_id, report_hash, Some(model_version_id))
        .await?;
    Ok(backtest_report_id)
}
