//! Feature-plane orchestration: selection → window prefetch → resolve → build →
//! partition → persist → emit.
//!
//! Wires the research [`ConfiguredFeatureBuilder`] with the online
//! [`FeatureWindowProvider`], Postgres persistence, and the `ClickHouse` feature
//! event writer. PIT inputs are resolved per market (the only async step), then
//! vectors are built in parallel from those frozen inputs. Vectors whose data
//! quality is [`DataQualityStatus::Insufficient`] are **partitioned out**: they
//! are never persisted, never emitted as facts, and never offered downstream —
//! a bad vector cannot reach the factor / model plane. The Phase 4 report
//! scheduler is deferred; this service is the callable unit schedulers invoke.

use crate::{
    observability::feature_fact_writer::FeatureEventWriter,
    pipeline::feature_window_provider::FeatureWindowProvider,
};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::{FeatureVectorInfo, NewFeatureVector, quant::NewReportDataQualitySnapshot},
    enums::quant::DataQualityStatus,
    runtime_config::{DataQualityConfig, FeaturesConfig},
    types::{MarketId, RuntimeConfigVersionId, TokenId, Usd},
};
use quant_pivot_repository::traits::FeatureRepository;
use quant_pivot_research::{
    features::{
        ConfiguredFeatureBuilder, FeatureName, FeatureSchema, FeatureVector, MarketDecisionCapture,
        MarketWindowSnapshot, NullReason, PitView, RejectedMarketDraft, ResolvedMarketBundle,
        draft_data_quality_snapshot, feature_events, merged_required_features,
    },
    selection::{ModelFeatureRequirements, SelectedMarket},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

/// Frozen inputs for one feature-plane round.
pub struct FeaturePipelineRequest<'a> {
    /// Markets selected for this round.
    pub included: &'a [SelectedMarket],
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Frozen feature config.
    pub features: &'a FeaturesConfig,
    /// Frozen data-quality config.
    pub data_quality: &'a DataQualityConfig,
    /// Model-required features (drives critical-missing rejection).
    pub model_requirements: &'a ModelFeatureRequirements,
    /// Source visibility delay, in seconds.
    pub source_delay_secs: u64,
    /// Point-in-time data view (live or historical).
    pub pit: PitView<'a>,
    /// Config version governing this round (DQ snapshot header).
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Liquidity cap used to normalize capture liquidity scores.
    pub liquidity_cap_usd: Usd,
}

/// A market whose feature vector failed the data-quality bar and was excluded.
///
/// Rejected markets are observable (so operators can see *why* a market dropped
/// out) but carry no persisted vector: they never reach persistence, facts, or
/// the downstream factor / model plane.
pub struct RejectedMarket {
    /// The excluded market.
    pub market_id: MarketId,
    /// The primary outcome token, when scoped.
    pub token_id: Option<TokenId>,
    /// Required / critical features that were missing, with their reasons.
    pub missing_required: Vec<(FeatureName, NullReason)>,
}

/// Outcome of one feature-plane round.
pub struct FeaturePipelineResult {
    /// Vectors that passed the data-quality bar (persisted + emitted).
    pub accepted: Vec<FeatureVector>,
    /// Markets excluded for insufficient data quality (not persisted).
    pub rejected: Vec<RejectedMarket>,
    /// Postgres persistence rows, aligned with `accepted`.
    pub persisted: Vec<FeatureVectorInfo>,
    /// Decision captures keyed by market id (accepted + rejected).
    pub captures: HashMap<MarketId, MarketDecisionCapture>,
    /// Draft report-level DQ snapshot for the transaction composer.
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
}

/// Orchestrates the online feature build loop for a selection snapshot.
///
/// Holds only process-lifetime dependencies (window read port, persistence,
/// fact writer). Each [`Self::run`] builds a [`ConfiguredFeatureBuilder`] from
/// the request's frozen [`FeaturesConfig`], so runtime-config activations never
/// require rebootstrap.
pub struct FeaturePipelineService {
    window_provider: FeatureWindowProvider,
    feature_repo: Arc<dyn FeatureRepository>,
    event_writer: Arc<FeatureEventWriter>,
}

impl FeaturePipelineService {
    /// Wire the service from boot-time dependencies.
    #[must_use]
    pub fn new(
        window_provider: FeatureWindowProvider,
        feature_repo: Arc<dyn FeatureRepository>,
        event_writer: Arc<FeatureEventWriter>,
    ) -> Self {
        Self {
            window_provider,
            feature_repo,
            event_writer,
        }
    }

    /// Run one feature round: prefetch windows, resolve PIT inputs, build vectors
    /// in parallel, partition by data quality, persist + emit only the accepted.
    ///
    /// # Errors
    ///
    /// Propagates window read, PIT resolution, persistence, or mapping failures.
    pub async fn run(
        &self,
        request: FeaturePipelineRequest<'_>,
    ) -> QuantResult<FeaturePipelineResult> {
        let builder = ConfiguredFeatureBuilder::new(request.features);
        let source_delay = Duration::from_secs(request.source_delay_secs);
        let windows = self.load_windows(&builder, &request, source_delay).await?;

        let max_concurrent = usize::try_from(request.features.max_concurrent_market_resolves)
            .unwrap_or(usize::MAX)
            .max(1);
        let resolve_jobs = request
            .included
            .iter()
            .enumerate()
            .map(|(index, market)| {
                let window = windows.get(&market.primary_token_id).ok_or_else(|| {
                    QuantError::from(ReportError::InvariantViolation {
                        stage: "feature_pipeline",
                        detail: format!(
                            "missing prefetched window for token {}",
                            market.primary_token_id.as_str()
                        ),
                    })
                })?;
                Ok((index, market, window))
            })
            .collect::<QuantResult<Vec<_>>>()?;

        let mut bundles = Vec::with_capacity(resolve_jobs.len());
        for chunk in resolve_jobs.chunks(max_concurrent) {
            let chunk_results = join_all(chunk.iter().map(|(_, market, window)| {
                builder.resolve_inputs(
                    market,
                    request.as_of,
                    request.pit,
                    window,
                    request.liquidity_cap_usd,
                )
            }))
            .await;
            for ((index, _, _), bundle) in chunk.iter().zip(chunk_results) {
                bundles.push((*index, bundle?));
            }
        }
        bundles.sort_by_key(|(index, _)| *index);
        let bundles: Vec<ResolvedMarketBundle<'_>> =
            bundles.into_iter().map(|(_, bundle)| bundle).collect();

        let required = &request.model_requirements.required_features;
        let vectors =
            builder.build_batch(&bundles, required, request.features, request.data_quality);

        let required_names = merged_required_features(required, request.features);
        let schema = builder.schema();
        let mut accepted = Vec::with_capacity(vectors.len());
        let mut rejected = Vec::new();
        let mut rejected_drafts = Vec::new();
        for (_bundle, vector) in bundles.iter().zip(&vectors) {
            if vector.data_quality == DataQualityStatus::Insufficient {
                let rejected_market = reject_market(vector, &required_names, schema);
                rejected_drafts.push(RejectedMarketDraft {
                    market_id: rejected_market.market_id.clone(),
                    missing_required: rejected_market.missing_required.clone(),
                });
                rejected.push(rejected_market);
            } else {
                accepted.push(vector.clone());
            }
        }

        let captures = finalize_captures(&bundles, &vectors);
        let data_quality_snapshot = draft_data_quality_snapshot(
            request.as_of,
            request.runtime_config_version_id.clone(),
            &bundles,
            &vectors,
            &rejected_drafts,
        );

        let rows = accepted
            .iter()
            .map(FeatureVector::try_to_new)
            .collect::<QuantResult<Vec<NewFeatureVector>>>()?;
        let persisted = self
            .feature_repo
            .create_batch(rows)
            .await
            .map_err(QuantError::from)?;

        let ingestion_time = Utc::now().timestamp_millis();
        let ch_rows = accepted
            .iter()
            .flat_map(|vector| feature_events(vector, schema, ingestion_time))
            .collect::<Vec<_>>();
        self.event_writer.write_batch(ch_rows);

        Ok(FeaturePipelineResult {
            accepted,
            rejected,
            persisted,
            captures,
            data_quality_snapshot,
        })
    }

    /// Prefetch the microstructure windows, skipping the `ClickHouse` read entirely
    /// when no enabled feature consumes a window (book / metadata-only schemas).
    async fn load_windows(
        &self,
        builder: &ConfiguredFeatureBuilder,
        request: &FeaturePipelineRequest<'_>,
        source_delay: Duration,
    ) -> QuantResult<HashMap<TokenId, MarketWindowSnapshot>> {
        if !builder.schema().needs_window() {
            return Ok(empty_windows(request.included, request.as_of, source_delay));
        }
        let lookback = max_feature_lookback(request.features);
        self.window_provider
            .load_windows(request.included, request.as_of, lookback, source_delay)
            .await
    }
}

/// Merge post-build data quality into frozen captures for every market.
fn finalize_captures(
    bundles: &[ResolvedMarketBundle<'_>],
    vectors: &[FeatureVector],
) -> HashMap<MarketId, MarketDecisionCapture> {
    bundles
        .iter()
        .zip(vectors)
        .map(|(bundle, vector)| {
            let mut capture = bundle.capture.clone();
            capture.data_quality = vector.data_quality;
            (capture.market_id.clone(), capture)
        })
        .collect()
}

/// Summarize why a market was rejected: the required / critical features that
/// were missing, with their reasons.
fn reject_market(
    vector: &FeatureVector,
    required_names: &HashSet<FeatureName>,
    schema: &FeatureSchema,
) -> RejectedMarket {
    let missing_required = vector
        .values
        .iter()
        .filter_map(|(name, value)| {
            let reason = value.null_reason()?;
            let spec = schema.by_name(name)?;
            let is_required = spec.critical || required_names.contains(name);
            is_required.then_some((name.clone(), reason))
        })
        .collect();
    RejectedMarket {
        market_id: vector.market_id.clone(),
        token_id: vector.token_id.clone(),
        missing_required,
    }
}

/// Empty (PIT-correct) windows for every market, used when the active schema
/// needs no windowed feature — avoids an unnecessary `ClickHouse` round-trip.
fn empty_windows(
    markets: &[SelectedMarket],
    as_of: DateTime<Utc>,
    source_delay: Duration,
) -> HashMap<TokenId, MarketWindowSnapshot> {
    markets
        .iter()
        .map(|market| {
            let token = market.primary_token_id.clone();
            (
                token.clone(),
                MarketWindowSnapshot::empty(token, as_of, source_delay),
            )
        })
        .collect()
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
