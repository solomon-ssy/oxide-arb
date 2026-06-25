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
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::NewTrainingDataset,
    enums::quant::TrainingDatasetStatus,
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, TrainingConfig},
    types::{MarketId, Price, TrainingDatasetId, TrainingExampleId, Usd},
};
use quant_pivot_repository::traits::{
    MarketRepository, QuantFactReadRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::{FactorEligibility, FactorEngine},
    features::ConfiguredFeatureBuilder,
    hashing::ResearchHasher,
    pit::PitQueryEngine,
    selection::SelectedMarket,
    training::{
        DatasetCoverage, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, ForwardWindow,
        LabelBuildInput, LabelBuildOutput, Labeler, LiquidityExitLabeler,
        MaxAdverseExcursionLabeler, MaxFavorableExcursionLabeler, PlanMarket,
        ReturnToHorizonLabeler, SamplePlan, SettlementOutcomeLabeler, TrainingDatasetArtifact,
        TrainingDatasetBuilder, TrainingDatasetPlanner, TrainingExample, TrainingLabel,
        assert_no_future_leakage, label_names, plan_samples, probe_matrix_coverage,
    },
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

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
            return Err(quant_pivot_error::research::ResearchError::DatasetPlan {
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
        let training_dataset_id = request
            .training_dataset_id
            .clone()
            .unwrap_or_else(TrainingDatasetId::from_v7);
        Ok(DatasetPlan {
            request,
            training_dataset_id,
            samples,
            label_names: label_names(),
        })
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
            planned_samples: plan.samples.len() as u64,
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
            planned_samples: plan.samples.len() as u64,
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
                &cross_section,
                prefetched,
                &plan.request,
                context.max_horizon_secs,
                coverage,
                &mut examples,
                &mut market_set,
            );
        }

        coverage.built_examples = examples.len() as u64;
        coverage.markets = market_set.len() as u64;

        self.finalize(&builder, &engine, plan, examples, std::mem::take(coverage))
            .await
    }

    /// Append training examples (factors + forward labels) for one PIT-resolved
    /// cross-section.
    #[allow(clippy::too_many_arguments)]
    fn append_examples(
        &self,
        cross_section: &ReplayCrossSection,
        prefetched: &Prefetched,
        request: &DatasetPlanRequest,
        max_horizon_secs: u64,
        coverage: &mut DatasetCoverage,
        examples: &mut Vec<TrainingExample>,
        market_set: &mut HashSet<MarketId>,
    ) {
        for (index, vector) in cross_section.vectors.iter().enumerate() {
            let market = &cross_section.markets[index];
            let entry_mid = cross_section.entry_mids[index];
            let outcome = &cross_section.outcomes[index];
            let factor_values = match &outcome.eligibility {
                FactorEligibility::Eligible => outcome
                    .factors
                    .iter()
                    .map(|scored| scored.value.clone())
                    .collect(),
                FactorEligibility::RejectCandidate { .. } => Vec::new(),
            };
            let forward = forward_window(
                cross_section.as_of,
                max_horizon_secs,
                prefetched
                    .micro
                    .get(&market.primary_token_id)
                    .map_or(&[][..], Vec::as_slice),
                prefetched
                    .resolutions
                    .get(&market.market_id)
                    .map_or(&[][..], Vec::as_slice),
            );
            let labels = self.build_labels(
                market,
                cross_section.as_of,
                entry_mid,
                request,
                &forward,
                coverage,
            );
            market_set.insert(market.market_id.clone());
            examples.push(TrainingExample {
                example_id: TrainingExampleId::from_v7(),
                market_id: market.market_id.clone(),
                token_id: market.primary_token_id.clone(),
                as_of: cross_section.as_of,
                feature_vector: vector.clone(),
                factor_values,
                labels,
                source_refs: vector.source_refs.clone(),
            });
        }
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
        let coverage_json = serde_json::to_value(&coverage).map_err(|error| {
            QuantError::Internal(format!("dataset coverage serialization failed: {error}"))
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
        let mut labels = Vec::new();
        for labeler in &self.labelers {
            let horizons: Vec<u64> = if labeler.is_horizon_dependent() {
                request.horizons_secs.clone()
            } else {
                vec![0]
            };
            for horizon_secs in horizons {
                let input = LabelBuildInput {
                    market_id: &market.market_id,
                    token_id: &market.primary_token_id,
                    yes_token_id: &market.primary_token_id,
                    as_of,
                    entry_mid,
                    horizon_secs,
                    min_exit_depth_usd: self.min_exit_depth_usd,
                    forward,
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
                .collect(),
            lookback: self.lookback,
            source_delay: self.source_delay,
            max_horizon_secs: self.max_horizon_secs,
        }
    }
}
