//! Shared point-in-time cross-section materialization.
//!
//! This is the **single** family-aware feature materialization path the offline
//! closure runs. Factor-native families execute the governed factor engine;
//! classical families select [`ReplayFactorMode::FeatureOnly`] and structurally
//! cannot compute factors. Training, rematerialization, and backtest callers
//! therefore share the same point-in-time feature path without fabricating a
//! factor contract for classical estimators.
//!
//! The function is pure orchestration over already-prefetched facts (served by
//! `HistoricalWindow`); it never touches a live `BookStore`.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::data_plane::DecisionBoundary,
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, OutcomeSide},
    },
    runtime_config::{DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig},
    types::{
        MarketId, OutcomeTokenBinding, Price, TokenId, TradeTapeSourceEvidence, Usd,
        stable_name::FeatureName,
    },
};
use quant_pivot_research::{
    domain::{DomainFactWindows, build_domain_slice_inputs},
    factors::{FactorEngine, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, DomainSliceInputs, FeatureSourceWindows, FeatureVector,
        MarketDecisionCapture, MarketWindowSnapshot, ResolvedBook, ResolvedMarketBundle,
        TradeTapeWindowSnapshot,
    },
    model::FavoriteLongshotBiasTable,
    pit::PointInTimeSnapshotSource,
    selection::SelectedMarket,
};

use crate::{
    ingest::trade_tape_health::runtime_market_tape_available,
    prefetch::historical_window::{
        Prefetched, ReplaySample, feature_window, replay_boundary, selected_market,
        trade_tape_window,
    },
};

/// Provenance used to decide whether PIT trade-tape facts were complete enough
/// to consume for one replay.
#[derive(Clone, Copy)]
pub enum ReplayTradeTapeSource<'a> {
    /// A sealed historical source slice owns fact completeness.
    Materialized { available_by: DateTime<Utc> },
    /// Runtime parity recomputes availability from serving's raw source state.
    FrozenRuntime(&'a HashMap<MarketId, TradeTapeSourceEvidence>),
}

/// Frozen feature/factor/data-quality config governing a replay.
#[derive(Clone)]
pub struct ReplayConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// External-vertical domain plane configuration.
    pub domain: DomainConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
    /// Governed single-recommendation exposure cap used by online serving to
    /// normalize decision-capture liquidity. Replay must use the same frozen
    /// denominator; catalog liquidity is an observed market attribute, not a
    /// transform parameter.
    pub liquidity_cap_usd: Usd,
    /// Factor-native bias calibration. Classical feature-only replay carries
    /// `None` and never resolves or consumes this dependency.
    pub bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

/// Per-call inputs for materializing one cross-section from a prefetched window.
pub struct CrossSectionRequest<'a> {
    /// Point-in-time engine resolving books/markets visible at `decision_at`.
    pub pit: &'a dyn PointInTimeSnapshotSource,
    /// Prefetched facts backing the trailing feature windows.
    pub prefetched: &'a Prefetched,
    /// Explicit trade-tape source provenance; never inferred from row presence.
    pub trade_tape_source: ReplayTradeTapeSource<'a>,
    /// Frozen decision time for the replayed cross-section.
    pub decision_at: DateTime<Utc>,
    /// `(market, token)` samples in this decision-time group.
    pub group: &'a [ReplaySample],
    /// Exact frozen model-input contract used by online feature materialization.
    pub required_features: &'a [FeatureName],
    /// Exact `ResearchProfile` category. `None` is the explicit pooled plane.
    pub category_scope: Option<MarketCategory>,
    /// Source visibility delay applied to features.
    pub knowledge_lag: Duration,
}

/// One PIT-resolved cross-section: aligned feature vectors, selection snapshots,
/// entry mids, and factor outcomes for a single `as_of`.
///
/// All four vectors are index-aligned (the `i`-th vector, market, entry mid, and
/// outcome describe the same market).
pub struct ReplayCrossSection {
    /// Complete decision and source-visibility boundary used by every row.
    pub boundary: DecisionBoundary,
    /// Resolved feature vectors that passed the data-quality bar.
    pub vectors: Vec<FeatureVector>,
    /// Resolved vectors rejected by the data-quality/required-input bar. They
    /// remain first-class replay evidence but never enter factors or models.
    pub rejected_vectors: Vec<FeatureVector>,
    /// Full decision captures for every resolved vector, including DQ rejects.
    pub captures: HashMap<ReplayCaptureKey, MarketDecisionCapture>,
    /// Validated catalog/feature-token orientations aligned with `vectors`.
    pub outcome_bindings: Vec<OutcomeTokenBinding>,
    /// Selection snapshots aligned with `vectors`.
    pub markets: Vec<SelectedMarket>,
    /// Entry mids (resolved book mid) aligned with `vectors`.
    pub entry_mids: Vec<Option<Price>>,
    /// Family-aware factor materialization result.
    pub factor_output: ReplayFactorOutput,
    /// Vectors dropped for insufficient data quality (coverage accounting).
    pub dropped_insufficient: u64,
}

/// Exact identity of one replay decision capture.
///
/// A market may contribute both YES- and NO-oriented rows at the same decision
/// time, so `MarketId` alone is not a valid key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplayCaptureKey {
    pub market_id: MarketId,
    pub token_id: TokenId,
}

impl ReplayCaptureKey {
    /// Build the capture key for one market/token orientation.
    #[must_use]
    pub fn new(market_id: &MarketId, token_id: &TokenId) -> Self {
        Self {
            market_id: market_id.clone(),
            token_id: token_id.clone(),
        }
    }
}

/// Explicit factor execution mode for one replay.
///
/// Classical estimators use `FeatureOnly`; the factor engine is structurally
/// absent rather than constructed and ignored.
#[derive(Clone, Copy)]
pub enum ReplayFactorMode<'a> {
    FactorNative { engine: &'a FactorEngine },
    FeatureOnly,
}

/// Family-aware output from one replayed cross-section.
pub enum ReplayFactorOutput {
    FactorNative { outcomes: Vec<MarketFactorOutcome> },
    FeatureOnly,
}

/// Materialize one `as_of` cross-section from the prefetched window.
///
/// Resolves each sample's PIT feature window, builds feature vectors with the
/// configured builder and drops insufficient-quality vectors. Factor-native
/// mode validates cross-section invariants and computes every frozen factor;
/// feature-only mode returns no factor output. Returns `None` when no sample
/// resolves to a market in the catalog.
///
/// # Errors
///
/// Propagates feature-resolution and factor-computation errors.
pub async fn materialize_cross_section(
    builder: &ConfiguredFeatureBuilder,
    factor_mode: ReplayFactorMode<'_>,
    config: &ReplayConfig,
    request: &CrossSectionRequest<'_>,
) -> QuantResult<Option<ReplayCrossSection>> {
    let decision_at = request.decision_at;
    if request.knowledge_lag.subsec_nanos() != 0 {
        return Err(QuantError::config(
            "historical knowledge lag must be expressed in whole seconds",
        ));
    }
    let boundary = replay_boundary(
        decision_at,
        request.knowledge_lag.as_secs(),
        config.domain.crypto.availability_lag_secs,
        config.domain.weather.availability_lag_secs,
    )?;
    let ReplayInputs {
        selected,
        outcome_bindings,
        windows,
        trade_windows,
    } = resolve_replay_inputs(builder, config, request, &boundary).await?;
    if selected.is_empty() {
        return Ok(None);
    }
    // Domain-slice inputs (offline): the SAME pure assembly the online plane
    // runs, over the prefetched linkage ledger + observation series — the
    // domain slice is byte-identical across planes by construction.
    let domain_inputs: Vec<Option<DomainSliceInputs>> = selected
        .iter()
        .map(|market| {
            build_domain_slice_inputs(
                market.category,
                request
                    .prefetched
                    .linkages
                    .get(&market.market_id)
                    .map_or(&[][..], Vec::as_slice),
                &boundary,
                &config.domain,
                DomainFactWindows {
                    observations: &request.prefetched.domain_observations,
                    crypto_reports: &request.prefetched.crypto_reports,
                    weather_observations: &request.prefetched.weather_observations,
                    weather_forecasts: &request.prefetched.weather_forecasts,
                    weather_calibrations: &request.prefetched.weather_calibrations,
                },
            )
        })
        .collect::<QuantResult<Vec<_>>>()?;

    let resolve_futures = selected
        .iter()
        .zip(windows.iter())
        .zip(trade_windows.iter())
        .zip(domain_inputs.iter())
        .map(|(((market, snapshot), trade_snapshot), domain)| {
            let boundary = boundary.clone();
            async move {
                builder
                    .resolve_inputs(
                        market,
                        &boundary,
                        request.pit,
                        FeatureSourceWindows {
                            microstructure: snapshot,
                            trade_tape: trade_snapshot,
                            domain: domain.as_ref(),
                        },
                        config.liquidity_cap_usd,
                    )
                    .await
            }
        });
    let bundles = try_join_all(resolve_futures).await?;

    let vectors = builder.build_batch(
        &bundles,
        request.required_features,
        &config.features,
        &config.data_quality,
    )?;

    let partitioned = partition_feature_vectors(vectors, &bundles, &selected, &outcome_bindings)?;
    if partitioned.kept_vectors.is_empty() {
        return Ok(Some(ReplayCrossSection {
            boundary,
            vectors: Vec::new(),
            rejected_vectors: partitioned.rejected_vectors,
            captures: partitioned.captures,
            outcome_bindings: Vec::new(),
            markets: Vec::new(),
            entry_mids: Vec::new(),
            factor_output: match factor_mode {
                ReplayFactorMode::FactorNative { .. } => ReplayFactorOutput::FactorNative {
                    outcomes: Vec::new(),
                },
                ReplayFactorMode::FeatureOnly => ReplayFactorOutput::FeatureOnly,
            },
            dropped_insufficient: partitioned.dropped_insufficient,
        }));
    }

    let factor_output = match factor_mode {
        ReplayFactorMode::FactorNative { engine } => {
            FactorEngine::validate_batch_invariants(&partitioned.kept_vectors)?;
            ReplayFactorOutput::FactorNative {
                outcomes: engine.compute_all_batch(&partitioned.kept_vectors, &config.factors)?,
            }
        }
        ReplayFactorMode::FeatureOnly => ReplayFactorOutput::FeatureOnly,
    };

    Ok(Some(ReplayCrossSection {
        boundary,
        vectors: partitioned.kept_vectors,
        rejected_vectors: partitioned.rejected_vectors,
        captures: partitioned.captures,
        outcome_bindings: partitioned.kept_bindings,
        markets: partitioned.kept_markets,
        entry_mids: partitioned.kept_entry_mids,
        factor_output,
        dropped_insufficient: partitioned.dropped_insufficient,
    }))
}

struct PartitionedFeatureVectors {
    kept_vectors: Vec<FeatureVector>,
    rejected_vectors: Vec<FeatureVector>,
    captures: HashMap<ReplayCaptureKey, MarketDecisionCapture>,
    kept_bindings: Vec<OutcomeTokenBinding>,
    kept_markets: Vec<SelectedMarket>,
    kept_entry_mids: Vec<Option<Price>>,
    dropped_insufficient: u64,
}

fn partition_feature_vectors(
    vectors: Vec<FeatureVector>,
    bundles: &[ResolvedMarketBundle<'_>],
    selected: &[SelectedMarket],
    outcome_bindings: &[OutcomeTokenBinding],
) -> QuantResult<PartitionedFeatureVectors> {
    let mut output = PartitionedFeatureVectors {
        kept_vectors: Vec::with_capacity(vectors.len()),
        rejected_vectors: Vec::new(),
        captures: HashMap::with_capacity(vectors.len()),
        kept_bindings: Vec::with_capacity(vectors.len()),
        kept_markets: Vec::with_capacity(vectors.len()),
        kept_entry_mids: Vec::with_capacity(vectors.len()),
        dropped_insufficient: 0,
    };
    for (((vector, bundle), market), binding) in vectors
        .into_iter()
        .zip(bundles)
        .zip(selected)
        .zip(outcome_bindings)
    {
        if vector.token_id.as_ref() != Some(&market.primary_token_id) {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "replay feature vector token {:?} does not match oriented primary token {} for market {}",
                    vector.token_id, market.primary_token_id, vector.market_id
                ),
            }
            .into());
        }
        if binding.market_id() != &vector.market_id
            || binding.feature_token_id() != &market.primary_token_id
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "replay outcome binding does not match feature vector {}/{}",
                    vector.market_id, market.primary_token_id
                ),
            }
            .into());
        }
        let mut capture = bundle.capture.clone();
        capture.data_quality = vector.data_quality;
        let capture_key = ReplayCaptureKey::new(&vector.market_id, &market.primary_token_id);
        if output.captures.insert(capture_key, capture).is_some() {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "replay cross-section contains duplicate market/token {}/{}",
                    vector.market_id, market.primary_token_id
                ),
            }
            .into());
        }
        if vector.data_quality == DataQualityStatus::Insufficient {
            output.dropped_insufficient += 1;
            output.rejected_vectors.push(vector);
            continue;
        }
        output
            .kept_entry_mids
            .push(bundle.inputs.book.as_ref().and_then(ResolvedBook::mid));
        output.kept_markets.push(market.clone());
        output.kept_bindings.push(binding.clone());
        output.kept_vectors.push(vector);
    }
    Ok(output)
}

struct ReplayInputs {
    selected: Vec<SelectedMarket>,
    outcome_bindings: Vec<OutcomeTokenBinding>,
    windows: Vec<MarketWindowSnapshot>,
    trade_windows: Vec<TradeTapeWindowSnapshot>,
}

async fn resolve_replay_inputs(
    builder: &ConfiguredFeatureBuilder,
    config: &ReplayConfig,
    request: &CrossSectionRequest<'_>,
    boundary: &DecisionBoundary,
) -> QuantResult<ReplayInputs> {
    // `WindowSpec.lookback` owns the widest prefetch horizon across source
    // families. Materialization must instead apply the exact per-source
    // horizon used online; otherwise a long trade-tape window silently widens
    // the microstructure window and creates training/serving skew.
    let microstructure_lookback =
        Duration::from_secs(config.features.max_microstructure_lookback_secs());
    let mut selected = Vec::with_capacity(request.group.len());
    let mut outcome_bindings = Vec::with_capacity(request.group.len());
    let mut windows = Vec::with_capacity(request.group.len());
    let mut trade_windows = Vec::with_capacity(request.group.len());
    for sample in request.group {
        let Some(snapshot) = request
            .pit
            .market_snapshot_at(&sample.market_id, boundary)
            .await?
        else {
            continue;
        };
        if request
            .category_scope
            .is_some_and(|category| snapshot.market.primary_category() != category)
        {
            continue;
        }
        let feature_side = if sample.token_id == snapshot.market.token_yes {
            OutcomeSide::Yes
        } else if sample.token_id == snapshot.market.token_no {
            OutcomeSide::No
        } else {
            return Err(ResearchError::PitResolution {
                detail: format!(
                    "replay token {} matches neither catalog token {} nor {} for market {}",
                    sample.token_id,
                    snapshot.market.token_yes,
                    snapshot.market.token_no,
                    sample.market_id
                ),
            }
            .into());
        };
        let outcome_binding = OutcomeTokenBinding::try_new(
            sample.market_id.clone(),
            snapshot.market.token_yes.clone(),
            snapshot.market.token_no.clone(),
            sample.token_id.clone(),
            feature_side,
        )
        .map_err(|error| ResearchError::PitResolution {
            detail: error.to_string(),
        })?;
        selected.push(selected_market(snapshot.market.as_ref(), &sample.token_id)?);
        outcome_bindings.push(outcome_binding);
        windows.push(feature_window(
            sample.token_id.clone(),
            boundary,
            microstructure_lookback,
            request
                .prefetched
                .micro
                .get(&sample.token_id)
                .map_or(&[][..], Vec::as_slice),
        )?);
        let (trade_tape_source, trade_tape_available) = replay_trade_tape_source(
            builder.needs_trade_tape(),
            &request.trade_tape_source,
            &sample.market_id,
            snapshot.market.neg_risk,
        )?;
        trade_windows.push(
            trade_tape_window(
                sample.market_id.clone(),
                boundary,
                Duration::from_secs(config.features.structural.trade_tape_window_secs),
                request
                    .prefetched
                    .trade_tape
                    .get(&sample.market_id)
                    .map_or(&[][..], Vec::as_slice),
            )?
            .with_source_evidence(trade_tape_source, trade_tape_available),
        );
    }
    Ok(ReplayInputs {
        selected,
        outcome_bindings,
        windows,
        trade_windows,
    })
}

fn replay_trade_tape_source(
    required: bool,
    source: &ReplayTradeTapeSource<'_>,
    market_id: &MarketId,
    neg_risk: bool,
) -> QuantResult<(TradeTapeSourceEvidence, bool)> {
    if !required {
        return Ok((TradeTapeSourceEvidence::not_required(), false));
    }
    match source {
        ReplayTradeTapeSource::Materialized { available_by } => {
            Ok((TradeTapeSourceEvidence::materialized(*available_by), true))
        }
        ReplayTradeTapeSource::FrozenRuntime(by_market) => {
            let evidence = by_market.get(market_id).ok_or_else(|| {
                ResearchError::Determinism {
                    detail: format!(
                        "runtime parity has no frozen trade-tape source evidence for market {market_id}"
                    ),
                }
            })?;
            let available = runtime_market_tape_available(evidence, neg_risk).map_err(|detail| {
                ResearchError::Determinism {
                    detail: format!(
                        "runtime parity trade-tape evidence for market {market_id} is invalid: {detail}"
                    ),
                }
            })?;
            Ok((evidence.clone(), available))
        }
    }
}
