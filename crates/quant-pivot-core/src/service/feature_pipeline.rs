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
use futures_util::future::try_join_all;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{FeatureVectorInfo, NewFeatureVector},
    enums::quant::DataQualityStatus,
    runtime_config::{DataQualityConfig, FeaturesConfig},
    types::{MarketId, TokenId},
};
use quant_pivot_repository::traits::FeatureRepository;
use quant_pivot_research::{
    features::{
        ConfiguredFeatureBuilder, FeatureName, FeatureSchema, FeatureVector, MarketWindowSnapshot,
        NullReason, PitView, feature_events,
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
    pub missing_required: Vec<(String, NullReason)>,
}

/// Outcome of one feature-plane round.
pub struct FeaturePipelineResult {
    /// Vectors that passed the data-quality bar (persisted + emitted).
    pub accepted: Vec<FeatureVector>,
    /// Markets excluded for insufficient data quality (not persisted).
    pub rejected: Vec<RejectedMarket>,
    /// Postgres persistence rows, aligned with `accepted`.
    pub persisted: Vec<FeatureVectorInfo>,
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

        // Resolve PIT inputs for every market concurrently (the only async step),
        // then build every vector in parallel from those frozen inputs.
        // `try_join_all` preserves input order so vectors align with selection.
        let resolve_futures = request
            .included
            .iter()
            .map(|market| {
                let window = windows.get(&market.primary_token_id).ok_or_else(|| {
                    QuantError::Internal(format!(
                        "missing prefetched window for token {}",
                        market.primary_token_id.as_str()
                    ))
                })?;
                Ok(builder.resolve_inputs(market, request.as_of, request.pit, window))
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let resolved = try_join_all(resolve_futures).await?;

        let required = &request.model_requirements.required_features;
        let vectors =
            builder.build_batch(&resolved, required, request.features, request.data_quality);

        // Partition: a vector whose quality is `Insufficient` is excluded — never
        // persisted, never emitted, never offered downstream.
        let required_names = required_name_set(required, request.features);
        let mut accepted = Vec::with_capacity(vectors.len());
        let mut rejected = Vec::new();
        let schema = builder.schema();
        for vector in vectors {
            if vector.data_quality == DataQualityStatus::Insufficient {
                rejected.push(reject_market(&vector, &required_names, schema));
            } else {
                accepted.push(vector);
            }
        }

        // Persist the accepted vectors in one transactional batch (atomic per
        // round), then emit their present-only long-format facts to ClickHouse.
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
        for vector in &accepted {
            self.event_writer
                .write_batch(feature_events(vector, schema, ingestion_time));
        }

        Ok(FeaturePipelineResult {
            accepted,
            rejected,
            persisted,
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

/// Summarize why a market was rejected: the required / critical features that
/// were missing, with their reasons.
fn reject_market(
    vector: &FeatureVector,
    required_names: &HashSet<String>,
    schema: &FeatureSchema,
) -> RejectedMarket {
    let missing_required = vector
        .values
        .iter()
        .filter_map(|(name, value)| {
            let reason = value.null_reason()?;
            let spec = schema.by_name(name)?;
            let is_required = spec.critical || required_names.contains(name.as_str());
            is_required.then(|| (name.as_str().to_owned(), reason))
        })
        .collect();
    RejectedMarket {
        market_id: vector.market_id.clone(),
        token_id: vector.token_id.clone(),
        missing_required,
    }
}

/// The merged required-feature name set: model requirements plus config
/// `required_features` (mirrors the builder's own rejection criterion).
fn required_name_set(required: &[FeatureName], config: &FeaturesConfig) -> HashSet<String> {
    let mut set: HashSet<String> = required
        .iter()
        .map(|name| name.as_str().to_owned())
        .collect();
    set.extend(config.required_features.iter().cloned());
    set
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
