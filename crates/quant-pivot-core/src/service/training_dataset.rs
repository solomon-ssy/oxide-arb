//! Offline training-dataset orchestration (Phase 3.5).
//!
//! Plans a deterministic sample grid, batch-prefetches every historical fact the
//! build needs (book snapshots, microstructure, market metadata, settlements),
//! serves point-in-time lookups from an in-memory
//! [`MaterializedPitEngine`] so the build loop issues zero DB queries, then —
//! per `as_of` cross-section — runs the **same** feature builder + factor engine
//! the online path uses, attaches forward-looking labels, asserts no future
//! leakage, materializes a content-hashed Parquet artifact, and records the
//! ledger row. Features are bounded by `as_of - source_delay`; labels look
//! strictly forward; the dataset hash makes the whole thing reproducible.

use crate::{
    pipeline::historical_window::{
        HistoricalWindowLoader, Prefetched, ReplaySample, WindowSpec, forward_window,
        max_feature_lookback,
    },
    service::historical_replay::{
        CrossSectionRequest, ReplayConfig, ReplayCrossSection, materialize_cross_section,
    },
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ExitTrainingLotRow, FeatureVectorInfo, NewTrainingDataset, RecommendationAttributionInfo,
        RecommendationInfo,
    },
    enums::quant::{RecommendationAttributionOutcome, TrainingDatasetStatus},
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, TrainingConfig},
    types::{
        Bps, MarketId, Price, TrainingDatasetId, TrainingExampleId, TrainingSampleSource, Usd,
    },
};
use quant_pivot_repository::traits::{
    AttributionRepository, FeatureRepository, MarketRepository, PositionRepository,
    QuantFactReadRepository, RecommendationRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    execution_sim::BookFidelity,
    factors::{
        FactorEligibility, FactorEngine, FactorExplanation, FactorName, FactorValue,
        MarketFactorOutcome,
    },
    features::{
        ConfiguredFeatureBuilder, EvidenceSourceRef, FeatureName, FeatureValue, FeatureVector,
        SubstitutionAudit,
    },
    hashing::ResearchHasher,
    model::sell_scorer::{LotStateInput, position_state_factor_values, position_state_features},
    pit::PitQueryEngine,
    selection::SelectedMarket,
    training::{
        DatasetCoverage, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, DecisionBook,
        ExitDecisionLabelContext, ForwardWindow, HoldVsExitProceedsLabeler, LabelBuildInput,
        LabelBuildOutput, Labeler, LiquidityExitLabeler, LotSamplePlan, LotTerminalSnapshot,
        LotTrainingContext, MaxAdverseExcursionLabeler, MaxFavorableExcursionLabeler, PlanMarket,
        ReturnToHorizonLabeler, SamplePlan, SettlementOutcomeLabeler, TrainingDatasetArtifact,
        TrainingDatasetBuilder, TrainingDatasetPlanner, TrainingExample, TrainingLabel,
        assert_no_future_leakage, label_names_for_sources, plan_lot_timeline_samples, plan_samples,
        probe_matrix_coverage, remaining_shares_at,
    },
};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

const LIVE_ATTRIBUTION_SAMPLE_LIMIT: u64 = 10_000;

/// The default labeler set materialized by a dataset build.
#[must_use]
pub fn default_labelers() -> Vec<Box<dyn Labeler>> {
    vec![
        Box::new(ReturnToHorizonLabeler),
        Box::new(MaxFavorableExcursionLabeler),
        Box::new(MaxAdverseExcursionLabeler),
        Box::new(LiquidityExitLabeler),
        Box::new(SettlementOutcomeLabeler),
    ]
}

/// Labelers materialized only for [`TrainingSampleSource::ExitDecision`] rows.
#[must_use]
pub fn exit_decision_labelers() -> Vec<Box<dyn Labeler>> {
    vec![
        Box::new(HoldVsExitProceedsLabeler),
        Box::new(LiquidityExitLabeler),
    ]
}

/// Dependencies injected into [`TrainingDatasetService`].
pub struct TrainingDatasetServiceDeps {
    /// `ClickHouse` fact reader for batch prefetch.
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    /// Postgres market catalog.
    pub market_repo: Arc<dyn MarketRepository>,
    /// Content-addressed artifact store for Parquet output.
    pub artifact_store: Arc<dyn ArtifactStore>,
    /// Training-dataset ledger repository.
    pub dataset_repo: Arc<dyn TrainingDatasetRepository>,
    /// Final attribution rows used as live supervised samples.
    pub attribution_repo: Arc<dyn AttributionRepository>,
    /// Recommendation ledger for frozen evidence refs and factor breakdown.
    pub recommendation_repo: Arc<dyn RecommendationRepository>,
    /// Frozen feature-vector ledger referenced by recommendations.
    pub feature_repo: Arc<dyn FeatureRepository>,
    /// Position ledger for closed-lot `ExitDecision` sampling.
    pub position_repo: Arc<dyn PositionRepository>,
}

/// Frozen runtime-config snapshot bound to one dataset build.
pub struct TrainingDatasetBuildConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
    /// Offline training-dataset build parameters (from runtime `training` section).
    pub training: TrainingConfig,
    /// Labelers materialized per example.
    pub labelers: Vec<Box<dyn Labeler>>,
}

/// Orchestrates the offline training-dataset build for one frozen config.
pub struct TrainingDatasetService {
    fact_read: Arc<dyn QuantFactReadRepository>,
    market_repo: Arc<dyn MarketRepository>,
    artifact_store: Arc<dyn ArtifactStore>,
    dataset_repo: Arc<dyn TrainingDatasetRepository>,
    attribution_repo: Arc<dyn AttributionRepository>,
    recommendation_repo: Arc<dyn RecommendationRepository>,
    feature_repo: Arc<dyn FeatureRepository>,
    position_repo: Arc<dyn PositionRepository>,
    features: FeaturesConfig,
    factors: FactorsConfig,
    data_quality: DataQualityConfig,
    max_book_staleness: Duration,
    min_exit_depth_usd: Usd,
    labelers: Vec<Box<dyn Labeler>>,
}

impl TrainingDatasetService {
    /// Wire the service from boot-time dependencies and a frozen config snapshot.
    pub fn new(
        deps: TrainingDatasetServiceDeps,
        config: TrainingDatasetBuildConfig,
    ) -> QuantResult<Self> {
        let min_exit_depth_usd = config
            .training
            .min_exit_depth_usd_typed()
            .map_err(QuantError::config)?;
        let max_book_staleness = Duration::from_millis(config.training.max_book_staleness_ms);
        Ok(Self {
            fact_read: deps.fact_read,
            market_repo: deps.market_repo,
            artifact_store: deps.artifact_store,
            dataset_repo: deps.dataset_repo,
            attribution_repo: deps.attribution_repo,
            recommendation_repo: deps.recommendation_repo,
            feature_repo: deps.feature_repo,
            position_repo: deps.position_repo,
            features: config.features,
            factors: config.factors,
            data_quality: config.data_quality,
            max_book_staleness,
            min_exit_depth_usd,
            labelers: config.labelers,
        })
    }
}

#[async_trait]
impl TrainingDatasetPlanner for TrainingDatasetService {
    async fn plan(&self, request: DatasetPlanRequest) -> QuantResult<DatasetPlan> {
        if request.window_start >= request.window_end {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "window_start {} must precede window_end {}",
                    request.window_start, request.window_end
                ),
            }
            .into());
        }
        // Candidate markets created before the window end. (A time-ranged market
        // query for fully-historical backfill of long-resolved markets is a
        // follow-up; `find_active` covers the live + recently-resolved set.)
        let markets = self
            .market_repo
            .find_active()
            .await
            .map_err(QuantError::from)?;
        let plan_markets: Vec<PlanMarket> = markets
            .iter()
            .filter(|info| info.created_at < request.window_end)
            .map(|info| PlanMarket {
                market_id: info.market_id.clone(),
                token_id: info.yes_token_id.clone(),
                created_at: info.created_at,
                end_date: info.end_date,
            })
            .collect();
        let samples = plan_samples(&request, &plan_markets);
        let mut lot_samples = Vec::new();
        let mut exit_training_lots = Vec::new();
        if wants_sample_source(&request, TrainingSampleSource::ExitDecision) {
            exit_training_lots = self
                .position_repo
                .find_exit_training_lots(
                    request.window_start,
                    request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            lot_samples =
                plan_lot_timeline_samples(request.sample_interval_secs, &exit_training_lots);
        }
        let training_dataset_id = request
            .training_dataset_id
            .clone()
            .unwrap_or_else(TrainingDatasetId::from_v7);
        let label_names = label_names_for_sources(&request.sample_sources);
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            samples,
            lot_samples,
            exit_training_lots,
            label_names,
        })
    }
}

impl TrainingDatasetService {
    /// Dry-run sample count aligned with [`TrainingDatasetBuilder::build`] coverage.
    pub async fn count_planned_samples(&self, plan: &DatasetPlan) -> QuantResult<u64> {
        let mut total = planned_historical_samples(plan);
        if wants_sample_source(&plan.request, TrainingSampleSource::LiveAttribution) {
            let attributions = self
                .attribution_repo
                .find_label_available_between(
                    plan.request.window_start,
                    plan.request.window_end,
                    LIVE_ATTRIBUTION_SAMPLE_LIMIT,
                )
                .await
                .map_err(QuantError::from)?;
            total += attributions.len() as u64;
        }
        if wants_sample_source(&plan.request, TrainingSampleSource::ExitDecision) {
            total += plan.lot_samples.len() as u64;
        }
        Ok(total)
    }
}

#[async_trait]
impl TrainingDatasetBuilder for TrainingDatasetService {
    async fn build(&self, plan: DatasetPlan) -> QuantResult<TrainingDatasetArtifact> {
        self.ensure_factors_enabled()?;
        let context = ReplayContext::new(&plan, &self.features);
        let loader = self.window_loader();
        let window = loader.load(&context.window_spec(&plan)).await?;
        let mut coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            book_decode_failures: window.book_decode_failures,
            ..DatasetCoverage::default()
        };
        self.build_from_prefetched(
            plan,
            &window.pit,
            &window.prefetched,
            &context,
            &mut coverage,
        )
        .await
    }
}

impl TrainingDatasetService {
    /// Build a dataset using a caller-supplied PIT engine (integration tests only).
    ///
    /// Prefetch still runs against the configured fact reader; only point-in-time
    /// book/market resolution is overridden.
    #[doc(hidden)]
    pub async fn build_with_pit_source(
        &self,
        plan: DatasetPlan,
        pit: &dyn PitQueryEngine,
    ) -> QuantResult<TrainingDatasetArtifact> {
        self.ensure_factors_enabled()?;
        let context = ReplayContext::new(&plan, &self.features);
        let loader = self.window_loader();
        let prefetched = loader.prefetch(&context.window_spec(&plan)).await?;
        let mut coverage = DatasetCoverage {
            planned_samples: planned_historical_samples(&plan),
            ..DatasetCoverage::default()
        };
        self.build_from_prefetched(plan, pit, &prefetched, &context, &mut coverage)
            .await
    }

    /// Reject an empty factor set (no enabled families).
    fn ensure_factors_enabled(&self) -> QuantResult<()> {
        if FactorEngine::new(&self.factors, &self.features)
            .registry()
            .is_empty()
        {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }
        Ok(())
    }

    /// Assemble the historical-window loader from the frozen staleness bound.
    fn window_loader(&self) -> HistoricalWindowLoader {
        HistoricalWindowLoader::new(
            Arc::clone(&self.fact_read),
            Arc::clone(&self.market_repo),
            self.max_book_staleness,
        )
    }

    /// The frozen replay config (feature/factor/data-quality) for this build.
    fn replay_config(&self) -> ReplayConfig {
        ReplayConfig {
            features: self.features.clone(),
            factors: self.factors.clone(),
            data_quality: self.data_quality.clone(),
        }
    }

    async fn build_from_prefetched(
        &self,
        plan: DatasetPlan,
        pit: &dyn PitQueryEngine,
        prefetched: &Prefetched,
        context: &ReplayContext,
        coverage: &mut DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let builder = ConfiguredFeatureBuilder::new(&self.features);
        let engine = FactorEngine::new(&self.factors, &self.features);
        let replay_config = self.replay_config();

        let mut examples: Vec<TrainingExample> = Vec::new();
        let mut market_set: HashSet<MarketId> = HashSet::new();

        if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
            for (as_of, group) in group_samples(&plan.samples) {
                let replay_group: Vec<ReplaySample> = group
                    .iter()
                    .map(|sample| ReplaySample {
                        market_id: sample.market_id.clone(),
                        token_id: sample.token_id.clone(),
                    })
                    .collect();
                let Some(cross_section) = materialize_cross_section(
                    &builder,
                    &engine,
                    &replay_config,
                    &CrossSectionRequest {
                        pit,
                        prefetched,
                        as_of,
                        group: &replay_group,
                        source_delay: context.source_delay,
                        lookback: context.lookback,
                    },
                )
                .await?
                else {
                    continue;
                };
                coverage.samples_dropped_insufficient += cross_section.dropped_insufficient;
                self.append_examples(
                    &CrossSectionAppendInput {
                        cross_section: &cross_section,
                        prefetched,
                        request: &plan.request,
                        max_horizon_secs: context.max_horizon_secs,
                    },
                    &mut ExampleBuildSink {
                        coverage,
                        examples: &mut examples,
                        market_set: &mut market_set,
                    },
                );
            }
        }

        if wants_sample_source(&plan.request, TrainingSampleSource::LiveAttribution) {
            self.append_live_attribution_examples(&plan, coverage, &mut examples, &mut market_set)
                .await?;
        }

        if wants_sample_source(&plan.request, TrainingSampleSource::ExitDecision) {
            coverage.exit_decision_candidates = plan.lot_samples.len() as u64;
            coverage.planned_samples += plan.lot_samples.len() as u64;
            self.append_exit_decision_examples(
                ExitDecisionAppendInput {
                    plan: &plan,
                    pit,
                    prefetched,
                    context,
                },
                &mut ExampleBuildSink {
                    coverage,
                    examples: &mut examples,
                    market_set: &mut market_set,
                },
            )
            .await?;
        }

        coverage.built_examples = examples.len() as u64;
        coverage.markets = market_set.len() as u64;

        self.finalize(&builder, &engine, plan, examples, std::mem::take(coverage))
            .await
    }

    /// Append training examples (factors + forward labels) for one PIT-resolved
    /// cross-section.
    fn append_examples(
        &self,
        input: &CrossSectionAppendInput<'_>,
        sink: &mut ExampleBuildSink<'_>,
    ) {
        for (index, vector) in input.cross_section.vectors.iter().enumerate() {
            let market = &input.cross_section.markets[index];
            let entry_mid = input.cross_section.entry_mids[index];
            let outcome = &input.cross_section.outcomes[index];
            let factor_values = match &outcome.eligibility {
                FactorEligibility::Eligible => outcome
                    .factors
                    .iter()
                    .map(|scored| scored.value.clone())
                    .collect(),
                FactorEligibility::RejectCandidate { .. } => Vec::new(),
            };
            let forward = forward_window(
                input.cross_section.as_of,
                input.max_horizon_secs,
                input
                    .prefetched
                    .micro
                    .get(&market.primary_token_id)
                    .map_or(&[][..], Vec::as_slice),
                input
                    .prefetched
                    .resolutions
                    .get(&market.market_id)
                    .map_or(&[][..], Vec::as_slice),
            );
            let labels = self.build_labels(
                market,
                input.cross_section.as_of,
                entry_mid,
                input.request,
                &forward,
                sink.coverage,
            );
            sink.market_set.insert(market.market_id.clone());
            sink.examples.push(TrainingExample {
                example_id: TrainingExampleId::from_v7(),
                market_id: market.market_id.clone(),
                token_id: market.primary_token_id.clone(),
                as_of: input.cross_section.as_of,
                sample_source: TrainingSampleSource::HistoricalPit,
                feature_vector: vector.clone(),
                factor_values,
                labels,
                source_refs: vector.source_refs.clone(),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            });
        }
    }

    async fn append_live_attribution_examples(
        &self,
        plan: &DatasetPlan,
        coverage: &mut DatasetCoverage,
        examples: &mut Vec<TrainingExample>,
        market_set: &mut HashSet<MarketId>,
    ) -> QuantResult<()> {
        let attributions = self
            .attribution_repo
            .find_label_available_between(
                plan.request.window_start,
                plan.request.window_end,
                LIVE_ATTRIBUTION_SAMPLE_LIMIT,
            )
            .await?;
        coverage.live_attribution_candidates += attributions.len() as u64;
        coverage.planned_samples += attributions.len() as u64;

        for attribution in attributions {
            match self
                .materialize_live_attribution_example(&attribution)
                .await?
            {
                Some(example) => {
                    market_set.insert(example.market_id.clone());
                    coverage.labels_available += example.labels.len() as u64;
                    examples.push(example);
                }
                None => coverage.live_attribution_dropped_missing_evidence += 1,
            }
        }
        Ok(())
    }

    async fn materialize_live_attribution_example(
        &self,
        attribution: &RecommendationAttributionInfo,
    ) -> QuantResult<Option<TrainingExample>> {
        let Some(recommendation) = self
            .recommendation_repo
            .find_by_id(&attribution.recommendation_id)
            .await?
        else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: recommendation not found",
            );
            return Ok(None);
        };
        if recommendation.status.excluded_from_attribution() {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: recommendation revoked",
            );
            return Ok(None);
        }
        let Some(feature_info) = self
            .feature_repo
            .find_by_id(&recommendation.evidence_refs.feature_vector_id)
            .await?
        else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
                "live attribution sample dropped: frozen feature vector not found",
            );
            return Ok(None);
        };
        let Some(feature_vector) = frozen_feature_vector(&feature_info) else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                feature_vector_id = %recommendation.evidence_refs.feature_vector_id,
                "live attribution sample dropped: frozen feature vector payload is invalid",
            );
            return Ok(None);
        };
        let Some(factor_values) = frozen_factor_values(&recommendation) else {
            tracing::warn!(
                recommendation_id = %attribution.recommendation_id,
                "live attribution sample dropped: frozen factor definitions are incomplete",
            );
            return Ok(None);
        };

        let labels = attribution_labels(attribution, &recommendation);
        Ok(Some(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: recommendation.market_id.clone(),
            token_id: recommendation.token_id.clone(),
            as_of: feature_vector.as_of,
            sample_source: TrainingSampleSource::LiveAttribution,
            source_refs: feature_vector.source_refs.clone(),
            feature_vector,
            factor_values,
            labels,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }))
    }

    async fn append_exit_decision_examples(
        &self,
        input: ExitDecisionAppendInput<'_>,
        sink: &mut ExampleBuildSink<'_>,
    ) -> QuantResult<()> {
        let lot_by_intent: HashMap<_, _> = input
            .plan
            .exit_training_lots
            .iter()
            .map(|lot| (lot.order_intent_id.clone(), lot))
            .collect();
        let exit_labelers = exit_decision_labelers();
        let builder = ConfiguredFeatureBuilder::new(&self.features);
        let engine = FactorEngine::new(&self.factors, &self.features);
        let replay_config = self.replay_config();

        for (as_of, group) in group_lot_samples(&input.plan.lot_samples) {
            let Some(cross_section) = materialize_lot_cross_section(LotCrossSectionMaterialize {
                builder: &builder,
                engine: &engine,
                replay_config: &replay_config,
                pit: input.pit,
                prefetched: input.prefetched,
                as_of,
                group: &group,
                context: input.context,
            })
            .await?
            else {
                sink.coverage.samples_dropped_insufficient += group.len() as u64;
                continue;
            };
            sink.coverage.samples_dropped_insufficient += cross_section.dropped_insufficient;

            for sample in group {
                let Some(lot) = lot_by_intent.get(&sample.order_intent_id) else {
                    continue;
                };
                let Some(index) = cross_section_index_for_lot_sample(&cross_section, sample) else {
                    sink.coverage.samples_dropped_insufficient += 1;
                    continue;
                };
                self.append_exit_decision_sample(
                    ExitDecisionSampleBuild {
                        sample,
                        lot,
                        cross_section: &cross_section,
                        market_index: index,
                        request: &input.plan.request,
                        prefetched: input.prefetched,
                        pit: input.pit,
                        max_horizon_secs: input.context.max_horizon_secs,
                        labelers: &exit_labelers,
                    },
                    sink,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn append_exit_decision_sample(
        &self,
        input: ExitDecisionSampleBuild<'_>,
        sink: &mut ExampleBuildSink<'_>,
    ) -> QuantResult<()> {
        let market = &input.cross_section.markets[input.market_index];
        let vector = &input.cross_section.vectors[input.market_index];
        let entry_mid = input.cross_section.entry_mids[input.market_index];
        let outcome = &input.cross_section.outcomes[input.market_index];
        let factor_values = eligible_factor_values(outcome);
        let remaining =
            remaining_shares_at(&LotTerminalSnapshot::from(input.lot), input.sample.as_of);
        if !remaining.is_positive() {
            return Ok(());
        }
        let position_state = position_state_features(LotStateInput {
            avg_price: input.lot.avg_price.inner(),
            mark: entry_mid.map(Price::inner),
            opened_at: input.lot.opened_at,
            now: input.sample.as_of,
            max_hold_secs: input.lot.max_hold_secs,
            peak_mark: input.lot.peak_mark_price.map(Price::inner),
        });
        let (decision_book, book_fidelity) =
            decision_book_at(input.pit, &input.sample.token_id, input.sample.as_of).await?;
        let label_ctx = ExitDecisionLabelContext {
            remaining_shares: remaining,
            avg_price: input.lot.avg_price,
            fee_bps: Bps::ZERO,
            terminal: LotTerminalSnapshot::from(input.lot),
            decision_book,
        };
        let forward = forward_window(
            input.sample.as_of,
            input.max_horizon_secs,
            input
                .prefetched
                .micro
                .get(&input.sample.token_id)
                .map_or(&[][..], Vec::as_slice),
            input
                .prefetched
                .resolutions
                .get(&input.sample.market_id)
                .map_or(&[][..], Vec::as_slice),
        );
        let labels = self.build_labels_for(
            &LabelBuildParams {
                labelers: input.labelers,
                market,
                as_of: input.sample.as_of,
                entry_mid,
                request: input.request,
                forward: &forward,
                exit_decision: Some(&label_ctx),
            },
            sink.coverage,
        );
        record_exit_fill_fidelity(sink.coverage, book_fidelity);
        let mut merged_factors = factor_values;
        merged_factors.extend(position_state_factor_values(&position_state));
        sink.market_set.insert(input.sample.market_id.clone());
        sink.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: input.sample.market_id.clone(),
            token_id: input.sample.token_id.clone(),
            as_of: input.sample.as_of,
            sample_source: TrainingSampleSource::ExitDecision,
            feature_vector: vector.clone(),
            factor_values: merged_factors,
            labels,
            source_refs: vector.source_refs.clone(),
            lot_context: Some(LotTrainingContext {
                order_intent_id: input.sample.order_intent_id.clone(),
                position_id: input.sample.position_id.clone(),
                remaining_shares: remaining,
                avg_price: input.lot.avg_price,
                peak_mark: input.lot.peak_mark_price,
                opened_at: input.lot.opened_at,
                max_hold_secs: input.lot.max_hold_secs,
            }),
            position_state: Some(position_state),
            book_fidelity,
        });
        sink.coverage.exit_decision_built += 1;
        Ok(())
    }

    /// Assert leakage-freedom, hash the schemas + content, write the Parquet
    /// artifact, and record the ledger row.
    async fn finalize(
        &self,
        builder: &ConfiguredFeatureBuilder,
        engine: &FactorEngine,
        plan: DatasetPlan,
        examples: Vec<TrainingExample>,
        coverage: DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        // Hard, money-critical gate: no feature may observe state past its cutoff.
        assert_no_future_leakage(&examples, plan.request.source_delay_secs)?;

        let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
        let factor_schema_hash = ResearchHasher::factor_schema(&engine.factor_set())?;
        let label_schema_hash = ResearchHasher::label_schema(&plan.label_names)?;
        let mut coverage = coverage;
        if !examples.is_empty() {
            let horizon_secs = plan.request.horizons_secs.first().copied().unwrap_or(0);
            coverage.matrix_probe = Some(probe_matrix_coverage(
                &examples,
                builder.schema(),
                ReturnToHorizonLabeler.label_name(),
                horizon_secs,
            )?);
        }
        let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            &plan.request.model_spec_id,
            plan.request.window_start,
            plan.request.window_end,
            &feature_schema_hash,
            &factor_schema_hash,
            &label_schema_hash,
            &examples,
        )?;

        let parquet_bytes = DatasetParquetCodec::encode(&examples)?;
        let key = ArtifactKey::new(
            ArtifactNamespace::Dataset,
            plan.training_dataset_id.as_uuid().to_string(),
            "parquet",
        )?;
        let parquet_uri = self.artifact_store.put(key, &parquet_bytes).await?;

        let status = if coverage.built_examples == 0 {
            TrainingDatasetStatus::Failed
        } else if coverage.labels_available == 0 {
            TrainingDatasetStatus::InsufficientLabels
        } else {
            TrainingDatasetStatus::Built
        };
        let coverage_json =
            serde_json::to_value(&coverage).map_err(|error| ResearchError::Serialization {
                detail: format!("dataset coverage serialization failed: {error}"),
            })?;
        self.dataset_repo
            .create(NewTrainingDataset {
                training_dataset_id: plan.training_dataset_id.clone(),
                model_spec_id: plan.request.model_spec_id.clone(),
                window_start: plan.request.window_start,
                window_end: plan.request.window_end,
                status,
                feature_schema_hash: feature_schema_hash.clone(),
                factor_schema_hash: factor_schema_hash.clone(),
                label_schema_hash: label_schema_hash.clone(),
                dataset_hash: dataset_hash.clone(),
                parquet_uri: parquet_uri.clone(),
                sample_count: i64::try_from(examples.len()).unwrap_or(i64::MAX),
                source_delay_secs: i64::try_from(plan.request.source_delay_secs)
                    .unwrap_or(i64::MAX),
                sample_interval_secs: i64::try_from(plan.request.sample_interval_secs)
                    .unwrap_or(i64::MAX),
                horizons_secs: serde_json::to_value(&plan.request.horizons_secs)
                    .unwrap_or_else(|_| serde_json::json!([])),
                coverage_json,
                runtime_config_version_id: plan.request.runtime_config_version_id.clone(),
            })
            .await
            .map_err(QuantError::from)?;

        Ok(TrainingDatasetArtifact {
            training_dataset_id: plan.training_dataset_id,
            model_spec_id: plan.request.model_spec_id,
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            examples,
            feature_schema_hash,
            factor_schema_hash,
            label_schema_hash,
            dataset_hash,
            parquet_uri,
            coverage,
        })
    }

    /// Build every label (labeler × horizon) for one example, accounting coverage.
    fn build_labels(
        &self,
        market: &SelectedMarket,
        as_of: DateTime<Utc>,
        entry_mid: Option<Price>,
        request: &DatasetPlanRequest,
        forward: &ForwardWindow,
        coverage: &mut DatasetCoverage,
    ) -> Vec<TrainingLabel> {
        self.build_labels_for(
            &LabelBuildParams {
                labelers: &self.labelers,
                market,
                as_of,
                entry_mid,
                request,
                forward,
                exit_decision: None,
            },
            coverage,
        )
    }

    fn build_labels_for(
        &self,
        params: &LabelBuildParams<'_>,
        coverage: &mut DatasetCoverage,
    ) -> Vec<TrainingLabel> {
        let mut labels = Vec::new();
        for labeler in params.labelers {
            let horizons: Vec<u64> = if labeler.is_horizon_dependent() {
                params.request.horizons_secs.clone()
            } else {
                vec![0]
            };
            for horizon_secs in horizons {
                let input = LabelBuildInput {
                    market_id: &params.market.market_id,
                    token_id: &params.market.primary_token_id,
                    yes_token_id: &params.market.primary_token_id,
                    as_of: params.as_of,
                    entry_mid: params.entry_mid,
                    horizon_secs,
                    min_exit_depth_usd: self.min_exit_depth_usd,
                    forward: params.forward,
                    exit_decision: params.exit_decision,
                };
                match labeler.build_label(&input) {
                    LabelBuildOutput::Available(label) => {
                        coverage.labels_available += 1;
                        labels.push(label);
                    }
                    LabelBuildOutput::NotMature { .. } => coverage.labels_not_mature += 1,
                    LabelBuildOutput::Unavailable { .. } => coverage.labels_unavailable += 1,
                }
            }
        }
        labels
    }
}

fn frozen_feature_vector(info: &FeatureVectorInfo) -> Option<FeatureVector> {
    let values = info.payload.get("values")?.clone();
    let substitutions = info
        .payload
        .get("substitutions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    Some(FeatureVector {
        market_id: info.market_id.clone(),
        token_id: info.token_id.clone(),
        as_of: info.as_of,
        schema_version: info.feature_schema_version,
        values: serde_json::from_value::<BTreeMap<FeatureName, FeatureValue>>(values).ok()?,
        substitutions: serde_json::from_value::<Vec<SubstitutionAudit>>(substitutions).ok()?,
        data_quality: info.data_quality,
        staleness_ms: u64::try_from(info.staleness_ms.max(0)).ok()?,
        source_refs: serde_json::from_value::<Vec<EvidenceSourceRef>>(info.source_refs.clone())
            .ok()?,
    })
}

fn frozen_factor_values(recommendation: &RecommendationInfo) -> Option<Vec<FactorValue>> {
    let mut values = Vec::with_capacity(recommendation.factor_breakdown.0.len());
    for (index, entry) in recommendation.factor_breakdown.0.iter().enumerate() {
        let definition_id = recommendation
            .evidence_refs
            .factor_definition_versions
            .get(index)?
            .clone();
        values.push(FactorValue {
            definition_id,
            name: FactorName::new(entry.factor_name.clone()),
            family: entry.family,
            raw_value: entry.raw_value,
            normalized_score: entry.normalized_score,
            direction: entry.direction,
            confidence: entry.confidence,
            explanation: FactorExplanation {
                headline: entry.explanation.clone(),
                drivers: Vec::new(),
                clamp: None,
            },
            input_feature_refs: Vec::new(),
        });
    }
    Some(values)
}

fn attribution_labels(
    attribution: &RecommendationAttributionInfo,
    recommendation: &RecommendationInfo,
) -> Vec<TrainingLabel> {
    let mut labels = Vec::new();
    if let Some(realized_pnl) = attribution.realized_pnl_usd {
        push_label(&mut labels, "realized_pnl_usd", realized_pnl.inner());
        if !recommendation.sizing_plan.suggested_usd.is_zero() {
            push_label(
                &mut labels,
                "realized_return_bps",
                realized_pnl.inner() / recommendation.sizing_plan.suggested_usd.inner()
                    * Decimal::from(10_000),
            );
        }
    }
    push_label(
        &mut labels,
        "entry_filled",
        if attribution.entry_outcome_json.entry_filled {
            Decimal::ONE
        } else {
            Decimal::ZERO
        },
    );
    if let Some(slippage) = attribution.entry_outcome_json.entry_slippage_bps {
        push_label(&mut labels, "entry_slippage_bps", slippage.inner());
    }
    if let Some(mfe) = attribution.max_favorable_excursion_bps {
        push_label(&mut labels, "max_favorable_excursion_bps", mfe);
    }
    if let Some(mae) = attribution.max_adverse_excursion_bps {
        push_label(&mut labels, "max_adverse_excursion_bps", mae);
    }
    push_label(
        &mut labels,
        "missed_return_bps",
        if attribution.entry_outcome_json.entry_filled {
            Decimal::ZERO
        } else {
            recommendation.expected_return_bps.inner()
        },
    );
    push_label(
        &mut labels,
        "recommendation_outcome",
        recommendation_outcome_code(attribution.outcome),
    );
    labels
}

fn push_label(labels: &mut Vec<TrainingLabel>, name: &'static str, value: Decimal) {
    labels.push(TrainingLabel {
        label_name: quant_pivot_research::training::LabelName::from_static(name),
        horizon_secs: 0,
        value,
        is_resolved: true,
    });
}

fn recommendation_outcome_code(outcome: RecommendationAttributionOutcome) -> Decimal {
    match outcome {
        RecommendationAttributionOutcome::FilledExited => Decimal::ONE,
        RecommendationAttributionOutcome::FilledSettled => Decimal::from(2),
        RecommendationAttributionOutcome::ExpiredUnfilled => Decimal::NEGATIVE_ONE,
        RecommendationAttributionOutcome::CancelledUnfilled => Decimal::from(-2),
        RecommendationAttributionOutcome::FailedUnfilled => Decimal::from(-3),
    }
}

fn wants_sample_source(request: &DatasetPlanRequest, source: TrainingSampleSource) -> bool {
    request.sample_sources.contains(&source)
}

fn planned_historical_samples(plan: &DatasetPlan) -> u64 {
    if wants_sample_source(&plan.request, TrainingSampleSource::HistoricalPit) {
        plan.samples.len() as u64
    } else {
        0
    }
}

/// Group sample instants by `as_of` (ascending) so each cross-section is scored
/// together (cross-sectional factor normalization needs the full same-`as_of`
/// set).
fn group_samples(samples: &[SamplePlan]) -> BTreeMap<DateTime<Utc>, Vec<&SamplePlan>> {
    let mut groups: BTreeMap<DateTime<Utc>, Vec<&SamplePlan>> = BTreeMap::new();
    for sample in samples {
        groups.entry(sample.as_of).or_default().push(sample);
    }
    groups
}

fn group_lot_samples(samples: &[LotSamplePlan]) -> BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> {
    let mut groups: BTreeMap<DateTime<Utc>, Vec<&LotSamplePlan>> = BTreeMap::new();
    for sample in samples {
        groups.entry(sample.as_of).or_default().push(sample);
    }
    groups
}

async fn decision_book_at(
    pit: &dyn PitQueryEngine,
    token_id: &quant_pivot_models::types::TokenId,
    as_of: DateTime<Utc>,
) -> QuantResult<(Option<DecisionBook>, Option<BookFidelity>)> {
    let Some(snapshot) = pit.book_at(token_id, as_of).await? else {
        return Ok((None, None));
    };
    if !snapshot.bids.is_empty() {
        return Ok((
            Some(DecisionBook::L2 {
                bids: Arc::clone(&snapshot.bids),
            }),
            Some(BookFidelity::L2),
        ));
    }
    Ok((None, None))
}

struct CrossSectionAppendInput<'a> {
    cross_section: &'a ReplayCrossSection,
    prefetched: &'a Prefetched,
    request: &'a DatasetPlanRequest,
    max_horizon_secs: u64,
}

struct LabelBuildParams<'a> {
    labelers: &'a [Box<dyn Labeler>],
    market: &'a SelectedMarket,
    as_of: DateTime<Utc>,
    entry_mid: Option<Price>,
    request: &'a DatasetPlanRequest,
    forward: &'a ForwardWindow,
    exit_decision: Option<&'a ExitDecisionLabelContext>,
}

struct ExitDecisionAppendInput<'a> {
    plan: &'a DatasetPlan,
    pit: &'a dyn PitQueryEngine,
    prefetched: &'a Prefetched,
    context: &'a ReplayContext,
}

struct LotCrossSectionMaterialize<'a> {
    builder: &'a ConfiguredFeatureBuilder,
    engine: &'a FactorEngine,
    replay_config: &'a ReplayConfig,
    pit: &'a dyn PitQueryEngine,
    prefetched: &'a Prefetched,
    as_of: DateTime<Utc>,
    group: &'a [&'a LotSamplePlan],
    context: &'a ReplayContext,
}

struct ExitDecisionSampleBuild<'a> {
    sample: &'a LotSamplePlan,
    lot: &'a ExitTrainingLotRow,
    cross_section: &'a ReplayCrossSection,
    market_index: usize,
    request: &'a DatasetPlanRequest,
    prefetched: &'a Prefetched,
    pit: &'a dyn PitQueryEngine,
    max_horizon_secs: u64,
    labelers: &'a [Box<dyn Labeler>],
}

struct ExampleBuildSink<'a> {
    coverage: &'a mut DatasetCoverage,
    examples: &'a mut Vec<TrainingExample>,
    market_set: &'a mut HashSet<MarketId>,
}

/// Derived replay parameters for one dataset build (shared across cross-sections).
struct ReplayContext {
    source_delay: Duration,
    lookback: Duration,
    max_horizon_secs: u64,
}

impl ReplayContext {
    /// Derive the source delay, feature lookback, and max forward horizon.
    fn new(plan: &DatasetPlan, features: &FeaturesConfig) -> Self {
        Self {
            source_delay: Duration::from_secs(plan.request.source_delay_secs),
            lookback: max_feature_lookback(features),
            max_horizon_secs: plan
                .request
                .horizons_secs
                .iter()
                .copied()
                .max()
                .unwrap_or(0),
        }
    }

    /// The prefetch window spec for this build's sample set.
    fn window_spec(&self, plan: &DatasetPlan) -> WindowSpec {
        WindowSpec {
            window_start: plan.request.window_start,
            window_end: plan.request.window_end,
            samples: plan
                .samples
                .iter()
                .map(|sample| ReplaySample {
                    market_id: sample.market_id.clone(),
                    token_id: sample.token_id.clone(),
                })
                .chain(plan.lot_samples.iter().map(|sample| ReplaySample {
                    market_id: sample.market_id.clone(),
                    token_id: sample.token_id.clone(),
                }))
                .collect(),
            lookback: self.lookback,
            source_delay: self.source_delay,
            max_horizon_secs: self.max_horizon_secs,
        }
    }
}

fn cross_section_index_for_lot_sample(
    cross_section: &ReplayCrossSection,
    sample: &LotSamplePlan,
) -> Option<usize> {
    cross_section
        .markets
        .iter()
        .enumerate()
        .find_map(|(index, market)| {
            if market.market_id != sample.market_id {
                return None;
            }
            if market.primary_token_id != sample.token_id {
                return None;
            }
            Some(index)
        })
}

async fn materialize_lot_cross_section(
    input: LotCrossSectionMaterialize<'_>,
) -> QuantResult<Option<ReplayCrossSection>> {
    let replay_group: Vec<ReplaySample> = input
        .group
        .iter()
        .map(|sample| ReplaySample {
            market_id: sample.market_id.clone(),
            token_id: sample.token_id.clone(),
        })
        .collect();
    materialize_cross_section(
        input.builder,
        input.engine,
        input.replay_config,
        &CrossSectionRequest {
            pit: input.pit,
            prefetched: input.prefetched,
            as_of: input.as_of,
            group: &replay_group,
            source_delay: input.context.source_delay,
            lookback: input.context.lookback,
        },
    )
    .await
}

fn eligible_factor_values(outcome: &MarketFactorOutcome) -> Vec<FactorValue> {
    match &outcome.eligibility {
        FactorEligibility::Eligible => outcome
            .factors
            .iter()
            .map(|scored| scored.value.clone())
            .collect(),
        FactorEligibility::RejectCandidate { .. } => Vec::new(),
    }
}

fn record_exit_fill_fidelity(coverage: &mut DatasetCoverage, book_fidelity: Option<BookFidelity>) {
    if book_fidelity == Some(BookFidelity::L2) {
        coverage.exit_fill_l2_rows += 1;
    } else if book_fidelity.is_some() {
        coverage.exit_fill_fallback_rows += 1;
    }
}
