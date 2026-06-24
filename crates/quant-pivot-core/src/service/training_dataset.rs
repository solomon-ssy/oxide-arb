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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use futures_util::future::try_join_all;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, ChBps, ChDecimal64, ChPrice, ChUsd,
        MarketResolutionRow,
    },
    domain::{MarketInfo, NewTrainingDataset},
    enums::{
        common::MarketCategory,
        market::MarketStatus,
        quant::{DataQualityStatus, TrainingDatasetStatus},
    },
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, TrainingConfig},
    types::{MarketId, Price, TokenId, TrainingDatasetId, TrainingExampleId, Usd},
};
use quant_pivot_repository::traits::{
    MarketRepository, QuantFactReadRepository, TrainingDatasetRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::{FactorEligibility, FactorEngine, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, FeatureVector, MarketWindowSnapshot, MicrostructureBucket,
        PitView, ResolvedBook, ResolvedInputs,
    },
    hashing::ResearchHasher,
    pit::{BookSnapshotAt, MarketContextAt, MaterializedPitEngine, PitQueryEngine},
    selection::SelectedMarket,
    training::{
        DatasetCoverage, DatasetParquetCodec, DatasetPlan, DatasetPlanRequest, ForwardSample,
        ForwardWindow, LabelBuildInput, LabelBuildOutput, Labeler, LiquidityExitLabeler,
        MarketResolution as ResearchMarketResolution, MaxAdverseExcursionLabeler,
        MaxFavorableExcursionLabeler, PlanMarket, ReturnToHorizonLabeler, SamplePlan,
        SettlementOutcomeLabeler, TrainingDatasetArtifact, TrainingDatasetBuilder,
        TrainingDatasetPlanner, TrainingExample, TrainingLabel, assert_no_future_leakage,
        label_names, plan_samples, probe_matrix_coverage,
    },
};

use crate::pipeline::historical_pit::snapshot_from_row;

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
        let engine = FactorEngine::new(&self.factors, &self.features);
        if engine.registry().is_empty() {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }

        let source_delay = Duration::from_secs(plan.request.source_delay_secs);
        let lookback = max_feature_lookback(&self.features);
        let max_horizon_secs = plan
            .request
            .horizons_secs
            .iter()
            .copied()
            .max()
            .unwrap_or(0);

        let prefetched = self
            .prefetch(&plan, lookback, source_delay, max_horizon_secs)
            .await?;
        let mut coverage = DatasetCoverage {
            planned_samples: plan.samples.len() as u64,
            ..DatasetCoverage::default()
        };
        let materialized =
            build_materialized_pit(&prefetched, self.max_book_staleness, &mut coverage);
        self.build_from_prefetched(plan, &materialized, prefetched, coverage)
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
        if FactorEngine::new(&self.factors, &self.features)
            .registry()
            .is_empty()
        {
            return Err(QuantError::config(
                "no factors enabled: factors.enabled_factor_families selects an empty factor set",
            ));
        }

        let source_delay = Duration::from_secs(plan.request.source_delay_secs);
        let lookback = max_feature_lookback(&self.features);
        let max_horizon_secs = plan
            .request
            .horizons_secs
            .iter()
            .copied()
            .max()
            .unwrap_or(0);

        let prefetched = self
            .prefetch(&plan, lookback, source_delay, max_horizon_secs)
            .await?;
        let coverage = DatasetCoverage {
            planned_samples: plan.samples.len() as u64,
            ..DatasetCoverage::default()
        };
        self.build_from_prefetched(plan, pit, prefetched, coverage)
            .await
    }

    async fn build_from_prefetched(
        &self,
        plan: DatasetPlan,
        pit: &dyn PitQueryEngine,
        prefetched: Prefetched,
        mut coverage: DatasetCoverage,
    ) -> QuantResult<TrainingDatasetArtifact> {
        let builder = ConfiguredFeatureBuilder::new(&self.features);
        let engine = FactorEngine::new(&self.factors, &self.features);
        let source_delay = Duration::from_secs(plan.request.source_delay_secs);
        let lookback = max_feature_lookback(&self.features);
        let max_horizon_secs = plan
            .request
            .horizons_secs
            .iter()
            .copied()
            .max()
            .unwrap_or(0);

        let mut examples: Vec<TrainingExample> = Vec::new();
        let mut market_set: HashSet<MarketId> = HashSet::new();

        for (as_of, group) in group_samples(&plan.samples) {
            let section = CrossSectionBuild {
                pit,
                prefetched: &prefetched,
                plan: &plan,
                as_of,
                group: &group,
                source_delay,
                lookback,
                max_horizon_secs,
            };
            let mut output = CrossSectionOutput {
                examples: &mut examples,
                coverage: &mut coverage,
                market_set: &mut market_set,
            };
            self.materialize_group(&builder, &engine, section, &mut output)
                .await?;
        }

        coverage.built_examples = examples.len() as u64;
        coverage.markets = market_set.len() as u64;

        self.finalize(&builder, &engine, plan, examples, coverage)
            .await
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

    /// Batch-read every historical fact the build will consume.
    async fn prefetch(
        &self,
        plan: &DatasetPlan,
        lookback: Duration,
        source_delay: Duration,
        max_horizon_secs: u64,
    ) -> QuantResult<Prefetched> {
        let mut tokens: Vec<TokenId> = Vec::new();
        let mut markets: Vec<MarketId> = Vec::new();
        let mut seen_tokens: HashSet<TokenId> = HashSet::new();
        let mut seen_markets: HashSet<MarketId> = HashSet::new();
        for sample in &plan.samples {
            if seen_tokens.insert(sample.token_id.clone()) {
                tokens.push(sample.token_id.clone());
            }
            if seen_markets.insert(sample.market_id.clone()) {
                markets.push(sample.market_id.clone());
            }
        }

        let book_from =
            (plan.request.window_start - to_chrono(self.max_book_staleness)).timestamp_millis();
        let book_to = plan.request.window_end.timestamp_millis();
        let micro_from =
            (plan.request.window_start - to_chrono(lookback) - to_chrono(source_delay))
                .timestamp_millis();
        let micro_to = (plan.request.window_end
            + ChronoDuration::seconds(i64::try_from(max_horizon_secs).unwrap_or(i64::MAX)))
        .timestamp_millis();
        let resolution_to = micro_to;

        let book_rows = self
            .fact_read
            .book_snapshots_between(tokens.clone(), book_from, book_to)
            .await
            .map_err(QuantError::from)?;
        let micro_rows = self
            .fact_read
            .microstructure_window(tokens.clone(), micro_from, micro_to)
            .await
            .map_err(QuantError::from)?;
        let resolution_rows = self
            .fact_read
            .resolutions_between(markets.clone(), 0, resolution_to)
            .await
            .map_err(QuantError::from)?;
        let market_infos = self
            .market_repo
            .find_by_ids(&markets)
            .await
            .map_err(QuantError::from)?;

        let mut books: HashMap<TokenId, Vec<BookSnapshotRow>> = HashMap::new();
        for row in book_rows {
            books.entry(row.token_id.clone()).or_default().push(row);
        }
        let mut micro: HashMap<TokenId, Vec<BookMicrostructureRow>> = HashMap::new();
        for row in micro_rows {
            micro.entry(row.token_id.clone()).or_default().push(row);
        }
        let mut resolutions: HashMap<MarketId, Vec<MarketResolutionRow>> = HashMap::new();
        for row in resolution_rows {
            resolutions
                .entry(row.market_id.clone())
                .or_default()
                .push(row);
        }
        let markets_by_id: HashMap<MarketId, Arc<MarketInfo>> = market_infos
            .into_iter()
            .map(|info| (info.market_id.clone(), info))
            .collect();

        Ok(Prefetched {
            books,
            micro,
            resolutions,
            markets_by_id,
        })
    }

    /// Materialize one `as_of` cross-section into training examples.
    async fn materialize_group(
        &self,
        builder: &ConfiguredFeatureBuilder,
        engine: &FactorEngine,
        section: CrossSectionBuild<'_>,
        output: &mut CrossSectionOutput<'_>,
    ) -> QuantResult<()> {
        let CrossSectionBuild {
            pit,
            prefetched,
            plan,
            as_of,
            group,
            source_delay,
            lookback,
            max_horizon_secs,
        } = section;
        let (selected, windows) =
            cross_section_inputs(group, prefetched, as_of, source_delay, lookback);
        if selected.is_empty() {
            return Ok(());
        }

        let pit_view = PitView::Historical(pit);
        let resolve_futures = selected
            .iter()
            .zip(windows.iter())
            .map(|(market, window)| builder.resolve_inputs(market, as_of, pit_view, window));
        let resolved = try_join_all(resolve_futures).await?;

        let vectors = builder.build_batch(&resolved, &[], &self.features, &self.data_quality);
        let kept = filter_eligible_vectors(vectors, &resolved, &selected, output.coverage);
        if kept.vectors.is_empty() {
            return Ok(());
        }

        FactorEngine::validate_batch_invariants(&kept.vectors)?;
        let outcomes = engine.compute_all_batch(&kept.vectors, &self.factors)?;
        append_cross_section_examples(
            &CrossSectionAppend {
                service: self,
                kept: &kept,
                outcomes: &outcomes,
                prefetched,
                plan,
                as_of,
                max_horizon_secs,
            },
            output,
        );
        Ok(())
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

/// Batch-prefetched historical facts for a dataset build.
struct Prefetched {
    books: HashMap<TokenId, Vec<BookSnapshotRow>>,
    micro: HashMap<TokenId, Vec<BookMicrostructureRow>>,
    resolutions: HashMap<MarketId, Vec<MarketResolutionRow>>,
    markets_by_id: HashMap<MarketId, Arc<MarketInfo>>,
}

/// Immutable inputs for one `as_of` cross-section build.
struct CrossSectionBuild<'a> {
    pit: &'a dyn PitQueryEngine,
    prefetched: &'a Prefetched,
    plan: &'a DatasetPlan,
    as_of: DateTime<Utc>,
    group: &'a [&'a SamplePlan],
    source_delay: Duration,
    lookback: Duration,
    max_horizon_secs: u64,
}

/// Mutable outputs accumulated while materializing one cross-section.
struct CrossSectionOutput<'a> {
    examples: &'a mut Vec<TrainingExample>,
    coverage: &'a mut DatasetCoverage,
    market_set: &'a mut HashSet<MarketId>,
}

/// Vectors that survived data-quality filtering for one cross-section.
struct KeptCrossSection {
    vectors: Vec<FeatureVector>,
    entry_mids: Vec<Option<Price>>,
    markets: Vec<SelectedMarket>,
}

/// Build selected markets and trailing feature windows for one cross-section.
fn cross_section_inputs(
    group: &[&SamplePlan],
    prefetched: &Prefetched,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    lookback: Duration,
) -> (Vec<SelectedMarket>, Vec<MarketWindowSnapshot>) {
    let mut selected = Vec::with_capacity(group.len());
    let mut windows = Vec::with_capacity(group.len());
    for sample in group {
        let Some(info) = prefetched.markets_by_id.get(&sample.market_id) else {
            continue;
        };
        selected.push(selected_market(info));
        windows.push(feature_window(
            sample.token_id.clone(),
            as_of,
            source_delay,
            lookback,
            prefetched
                .micro
                .get(&sample.token_id)
                .map_or(&[][..], Vec::as_slice),
        ));
    }
    (selected, windows)
}

/// Drop insufficient-quality vectors; keep aligned entry mids and markets.
fn filter_eligible_vectors(
    vectors: Vec<FeatureVector>,
    resolved: &[ResolvedInputs<'_>],
    selected: &[SelectedMarket],
    coverage: &mut DatasetCoverage,
) -> KeptCrossSection {
    let mut kept = KeptCrossSection {
        vectors: Vec::with_capacity(vectors.len()),
        entry_mids: Vec::with_capacity(vectors.len()),
        markets: Vec::with_capacity(vectors.len()),
    };
    for ((vector, input), market) in vectors
        .into_iter()
        .zip(resolved.iter())
        .zip(selected.iter())
    {
        if vector.data_quality == DataQualityStatus::Insufficient {
            coverage.samples_dropped_insufficient += 1;
            continue;
        }
        kept.entry_mids
            .push(input.book.as_ref().and_then(ResolvedBook::mid));
        kept.markets.push(market.clone());
        kept.vectors.push(vector);
    }
    kept
}

/// Inputs for appending scored examples from one cross-section.
struct CrossSectionAppend<'a> {
    service: &'a TrainingDatasetService,
    kept: &'a KeptCrossSection,
    outcomes: &'a [MarketFactorOutcome],
    prefetched: &'a Prefetched,
    plan: &'a DatasetPlan,
    as_of: DateTime<Utc>,
    max_horizon_secs: u64,
}

/// Append training examples for one scored cross-section.
fn append_cross_section_examples(
    section: &CrossSectionAppend<'_>,
    output: &mut CrossSectionOutput<'_>,
) {
    let CrossSectionAppend {
        service,
        kept,
        outcomes,
        prefetched,
        plan,
        as_of,
        max_horizon_secs,
    } = section;
    for (index, vector) in kept.vectors.iter().enumerate() {
        let market = &kept.markets[index];
        let entry_mid = kept.entry_mids[index];
        let outcome = &outcomes[index];
        let factor_values = match &outcome.eligibility {
            FactorEligibility::Eligible => outcome
                .factors
                .iter()
                .map(|scored| scored.value.clone())
                .collect(),
            FactorEligibility::RejectCandidate { .. } => Vec::new(),
        };
        let forward = forward_window(
            *as_of,
            *max_horizon_secs,
            prefetched
                .micro
                .get(&market.primary_token_id)
                .map_or(&[][..], Vec::as_slice),
            prefetched
                .resolutions
                .get(&market.market_id)
                .map_or(&[][..], Vec::as_slice),
        );
        let labels = service.build_labels(
            market,
            *as_of,
            entry_mid,
            &plan.request,
            &forward,
            output.coverage,
        );
        output.market_set.insert(market.market_id.clone());
        output.examples.push(TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: market.market_id.clone(),
            token_id: market.primary_token_id.clone(),
            as_of: *as_of,
            feature_vector: vector.clone(),
            factor_values,
            labels,
            source_refs: vector.source_refs.clone(),
        });
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

/// Build the in-memory PIT engine from the prefetched window.
fn build_materialized_pit(
    prefetched: &Prefetched,
    max_staleness: Duration,
    coverage: &mut DatasetCoverage,
) -> MaterializedPitEngine {
    let placeholder = epoch();
    let mut books: HashMap<TokenId, Vec<BookSnapshotAt>> = HashMap::new();
    for (token, rows) in &prefetched.books {
        let series: Vec<BookSnapshotAt> = rows
            .iter()
            .filter_map(|row| {
                let (snapshot, status) = snapshot_from_row(row.clone(), placeholder);
                if status.counts_as_failure() {
                    coverage.book_decode_failures += 1;
                }
                snapshot
            })
            .collect();
        books.insert(token.clone(), series);
    }

    let mut markets: HashMap<MarketId, Vec<MarketContextAt>> = HashMap::new();
    for (market_id, info) in &prefetched.markets_by_id {
        let mut series = vec![market_context_entry(
            info,
            info.created_at,
            MarketStatus::Active,
        )];
        if let Some(latest) = prefetched.resolutions.get(market_id).and_then(|rows| {
            rows.iter()
                .max_by_key(|row| (row.resolved_at, row.observed_at))
        }) {
            series.push(market_context_entry(
                info,
                ms(latest.resolved_at),
                MarketStatus::Settled,
            ));
        }
        markets.insert(market_id.clone(), series);
    }

    MaterializedPitEngine::new(books, markets, to_chrono(max_staleness))
}

/// One market-context series entry observed at `observed_at` with `status`.
fn market_context_entry(
    info: &MarketInfo,
    observed_at: DateTime<Utc>,
    status: MarketStatus,
) -> MarketContextAt {
    MarketContextAt {
        market_id: info.market_id.clone(),
        as_of: observed_at,
        observed_at,
        status,
        neg_risk: info.neg_risk,
        end_date: info.end_date,
        created_at: info.created_at,
        outcome_count: 2,
    }
}

/// Project a market catalog row into a selection entry (primary = YES token).
fn selected_market(info: &MarketInfo) -> SelectedMarket {
    SelectedMarket {
        market_id: info.market_id.clone(),
        event_id: info.event_id.clone(),
        category: info
            .categories
            .first()
            .copied()
            .unwrap_or(MarketCategory::Other),
        primary_token_id: info.yes_token_id.clone(),
        secondary_token_id: Some(info.no_token_id.clone()),
        liquidity_usd: None,
        volume_24h_usd: None,
        source_refs: Vec::new(),
    }
}

/// Build the trailing PIT feature window for one `(token, as_of)`.
fn feature_window(
    token_id: TokenId,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    lookback: Duration,
    rows: &[BookMicrostructureRow],
) -> MarketWindowSnapshot {
    let cutoff = as_of - to_chrono(source_delay);
    let start = cutoff - to_chrono(lookback);
    let buckets = rows
        .iter()
        .filter_map(|row| {
            let at = ms(row.bucket_time);
            (at >= start && at <= cutoff).then(|| bucket_from_row(row, at))
        })
        .collect();
    MarketWindowSnapshot {
        token_id,
        as_of,
        source_delay,
        buckets,
    }
}

/// Build the strictly-forward label window for one `(token, as_of)`.
fn forward_window(
    as_of: DateTime<Utc>,
    max_horizon_secs: u64,
    rows: &[BookMicrostructureRow],
    resolutions: &[MarketResolutionRow],
) -> ForwardWindow {
    let data_available_until = rows.last().map_or(as_of, |row| ms(row.bucket_time));
    let cap = as_of + ChronoDuration::seconds(i64::try_from(max_horizon_secs).unwrap_or(i64::MAX));
    let samples = rows
        .iter()
        .filter_map(|row| {
            let at = ms(row.bucket_time);
            (at > as_of && at <= cap).then(|| forward_sample(row, at))
        })
        .collect();
    // Settlement is independent of microstructure maturity: any resolution strictly
    // after `as_of` is visible to the settlement labeler.
    let resolution = resolutions
        .iter()
        .filter(|row| ms(row.resolved_at) > as_of)
        .max_by_key(|row| (row.resolved_at, row.observed_at))
        .map(|row| ResearchMarketResolution {
            winning_token_id: row.winning_token_id.clone(),
            resolved_at: ms(row.resolved_at),
            observed_at: ms(row.observed_at),
        });
    ForwardWindow {
        anchor: as_of,
        data_available_until,
        samples,
        resolution,
    }
}

/// Decode a microstructure row into a compute-domain bucket.
fn bucket_from_row(row: &BookMicrostructureRow, at: DateTime<Utc>) -> MicrostructureBucket {
    MicrostructureBucket {
        bucket_time: at,
        mid_close: row.mid_price_close.map(ChPrice::to_price),
        spread_bps_avg: row.spread_bps_avg.map(ChBps::to_bps),
        top1_depth_usd_avg: row.top1_depth_usd_avg.map(ChUsd::to_usd),
        top5_depth_usd_avg: row.top5_depth_usd_avg.map(ChUsd::to_usd),
        imbalance_avg: row.imbalance_avg.map(ChDecimal64::to_decimal),
        update_count: row.update_count,
        snapshot_count: row.snapshot_count,
        delta_count: row.delta_count,
        crossed_count: row.crossed_count,
        gap_count: row.gap_count,
        max_book_age_ms: row.max_book_age_ms,
    }
}

/// Decode a microstructure row into a forward label observation.
fn forward_sample(row: &BookMicrostructureRow, at: DateTime<Utc>) -> ForwardSample {
    ForwardSample {
        at,
        mid_close: row.mid_price_close.map(ChPrice::to_price),
        best_bid_high: row.best_bid_high.map(ChPrice::to_price),
        best_bid_low: row.best_bid_low.map(ChPrice::to_price),
        top1_depth_usd: row.top1_depth_usd_avg.map(ChUsd::to_usd),
    }
}

/// Maximum trailing window any enabled time-series / microstructure feature needs.
fn max_feature_lookback(config: &FeaturesConfig) -> Duration {
    let max_secs = config
        .bar_windows_secs
        .iter()
        .chain(config.momentum_windows_secs.iter())
        .chain(config.volatility_windows_secs.iter())
        .copied()
        .max()
        .unwrap_or(0);
    Duration::from_secs(max_secs)
}

/// Convert a `std::time::Duration` into a saturating `chrono::Duration`.
fn to_chrono(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

/// Convert epoch milliseconds to a UTC instant (epoch fallback on overflow).
fn ms(timestamp_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(epoch)
}

/// The Unix epoch instant, used as an overflow/placeholder fallback.
fn epoch() -> DateTime<Utc> {
    DateTime::from_timestamp(0, 0).unwrap_or_else(Utc::now)
}
