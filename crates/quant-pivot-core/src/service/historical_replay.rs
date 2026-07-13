//! Shared point-in-time cross-section materialization (Phase 3.6).
//!
//! This is the **single** feature→factor computation path the offline closure
//! runs: the training-dataset build, model training rematerialization, and the
//! backtest replay all call [`materialize_cross_section`], so the factors a
//! model is trained on are byte-identical to those a backtest scores and to
//! those the online plane would compute from the same point-in-time facts. Any
//! divergence here would mean a backtest validates a different model than
//! production — money-unsafe — so the path is deliberately shared rather than
//! duplicated.
//!
//! The function is pure orchestration over already-prefetched facts (served by
//! [`HistoricalWindow`]); it never touches a live `BookStore`.

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::DecisionBoundary,
    enums::quant::DataQualityStatus,
    runtime_config::{DataQualityConfig, DomainConfig, FactorsConfig, FeaturesConfig},
    types::{MarketId, Price, Usd},
};
use quant_pivot_research::{
    domain::build_domain_slice_inputs,
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

use crate::prefetch::historical_window::{
    Prefetched, ReplaySample, feature_window, replay_boundary, selected_market, trade_tape_window,
};

/// Frozen feature/factor/data-quality config governing a replay.
#[derive(Clone)]
pub struct ReplayConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// External-vertical domain plane configuration (Phase 11.2.2).
    pub domain: DomainConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
    /// Favorite-longshot bias table pinned by the frozen factor config (content-
    /// hash verified). `None` keeps `struct.favorite_longshot` inert. Bound here
    /// so the offline engine scores byte-identically to the online plane.
    pub bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
}

/// Per-call inputs for materializing one cross-section from a prefetched window.
pub struct CrossSectionRequest<'a> {
    /// Point-in-time engine resolving books/markets visible at `decision_at`.
    pub pit: &'a dyn PointInTimeSnapshotSource,
    /// Prefetched facts backing the trailing feature windows.
    pub prefetched: &'a Prefetched,
    /// Frozen decision time for the replayed cross-section.
    pub decision_at: DateTime<Utc>,
    /// `(market, token)` samples in this decision-time group.
    pub group: &'a [ReplaySample],
    /// Source visibility delay applied to features.
    pub knowledge_lag: Duration,
    /// Maximum trailing feature lookback.
    pub lookback: Duration,
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
    pub captures: HashMap<MarketId, MarketDecisionCapture>,
    /// Selection snapshots aligned with `vectors`.
    pub markets: Vec<SelectedMarket>,
    /// Entry mids (resolved book mid) aligned with `vectors`.
    pub entry_mids: Vec<Option<Price>>,
    /// Factor outcomes aligned with `vectors`.
    pub outcomes: Vec<MarketFactorOutcome>,
    /// Vectors dropped for insufficient data quality (coverage accounting).
    pub dropped_insufficient: u64,
}

/// Materialize one `as_of` cross-section from the prefetched window.
///
/// Resolves each sample's PIT feature window, builds feature vectors with the
/// configured builder, drops insufficient-quality vectors, validates the
/// cross-section invariants, and computes every enabled factor. Returns `None`
/// when no sample resolves to a market in the catalog.
///
/// # Errors
///
/// Propagates feature-resolution and factor-computation errors.
pub async fn materialize_cross_section(
    builder: &ConfiguredFeatureBuilder,
    engine: &FactorEngine,
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
    )?;
    let ReplayInputs {
        selected,
        windows,
        trade_windows,
        liquidity_caps,
    } = resolve_replay_inputs(config, request, &boundary).await?;
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
                &request.prefetched.domain_observations,
            )
        })
        .collect::<QuantResult<Vec<_>>>()?;

    let resolve_futures = selected
        .iter()
        .zip(windows.iter())
        .zip(trade_windows.iter())
        .zip(domain_inputs.iter())
        .zip(liquidity_caps.iter())
        .map(
            |((((market, snapshot), trade_snapshot), domain), liquidity_cap)| {
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
                            *liquidity_cap,
                        )
                        .await
                }
            },
        );
    let bundles = try_join_all(resolve_futures).await?;

    let vectors = builder.build_batch(&bundles, &[], &config.features, &config.data_quality)?;

    let partitioned = partition_feature_vectors(vectors, &bundles, &selected)?;
    if partitioned.kept_vectors.is_empty() {
        return Ok(Some(ReplayCrossSection {
            boundary,
            vectors: Vec::new(),
            rejected_vectors: partitioned.rejected_vectors,
            captures: partitioned.captures,
            markets: Vec::new(),
            entry_mids: Vec::new(),
            outcomes: Vec::new(),
            dropped_insufficient: partitioned.dropped_insufficient,
        }));
    }

    FactorEngine::validate_batch_invariants(&partitioned.kept_vectors)?;
    let outcomes = engine.compute_all_batch(&partitioned.kept_vectors, &config.factors)?;

    Ok(Some(ReplayCrossSection {
        boundary,
        vectors: partitioned.kept_vectors,
        rejected_vectors: partitioned.rejected_vectors,
        captures: partitioned.captures,
        markets: partitioned.kept_markets,
        entry_mids: partitioned.kept_entry_mids,
        outcomes,
        dropped_insufficient: partitioned.dropped_insufficient,
    }))
}

struct PartitionedFeatureVectors {
    kept_vectors: Vec<FeatureVector>,
    rejected_vectors: Vec<FeatureVector>,
    captures: HashMap<MarketId, MarketDecisionCapture>,
    kept_markets: Vec<SelectedMarket>,
    kept_entry_mids: Vec<Option<Price>>,
    dropped_insufficient: u64,
}

fn partition_feature_vectors(
    vectors: Vec<FeatureVector>,
    bundles: &[ResolvedMarketBundle<'_>],
    selected: &[SelectedMarket],
) -> QuantResult<PartitionedFeatureVectors> {
    let mut output = PartitionedFeatureVectors {
        kept_vectors: Vec::with_capacity(vectors.len()),
        rejected_vectors: Vec::new(),
        captures: HashMap::with_capacity(vectors.len()),
        kept_markets: Vec::with_capacity(vectors.len()),
        kept_entry_mids: Vec::with_capacity(vectors.len()),
        dropped_insufficient: 0,
    };
    for ((vector, bundle), market) in vectors.into_iter().zip(bundles).zip(selected) {
        let mut capture = bundle.capture.clone();
        capture.data_quality = vector.data_quality;
        if output
            .captures
            .insert(vector.market_id.clone(), capture)
            .is_some()
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "replay cross-section contains duplicate market {}",
                    vector.market_id
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
        output.kept_vectors.push(vector);
    }
    Ok(output)
}

struct ReplayInputs {
    selected: Vec<SelectedMarket>,
    windows: Vec<MarketWindowSnapshot>,
    trade_windows: Vec<TradeTapeWindowSnapshot>,
    liquidity_caps: Vec<Usd>,
}

async fn resolve_replay_inputs(
    config: &ReplayConfig,
    request: &CrossSectionRequest<'_>,
    boundary: &DecisionBoundary,
) -> QuantResult<ReplayInputs> {
    let mut selected = Vec::with_capacity(request.group.len());
    let mut windows = Vec::with_capacity(request.group.len());
    let mut trade_windows = Vec::with_capacity(request.group.len());
    let mut liquidity_caps = Vec::with_capacity(request.group.len());
    for sample in request.group {
        let Some(snapshot) = request
            .pit
            .market_snapshot_at(&sample.market_id, boundary)
            .await?
        else {
            continue;
        };
        if sample.token_id != snapshot.market.token_yes {
            return Err(ResearchError::PitResolution {
                detail: format!(
                    "replay schedule token {} does not match catalog YES token {} for market {}",
                    sample.token_id, snapshot.market.token_yes, sample.market_id
                ),
            }
            .into());
        }
        let liquidity_cap =
            snapshot
                .market
                .liquidity_usd
                .ok_or_else(|| ResearchError::PitResolution {
                    detail: format!(
                        "market {} has no catalog liquidity at decision {}",
                        sample.market_id,
                        boundary.decision_at()
                    ),
                })?;
        selected.push(selected_market(snapshot.market.as_ref()));
        windows.push(feature_window(
            sample.token_id.clone(),
            boundary,
            request.lookback,
            request
                .prefetched
                .micro
                .get(&sample.token_id)
                .map_or(&[][..], Vec::as_slice),
        )?);
        trade_windows.push(trade_tape_window(
            sample.market_id.clone(),
            boundary,
            Duration::from_secs(config.features.structural.trade_tape_window_secs),
            request
                .prefetched
                .trade_tape
                .get(&sample.market_id)
                .map_or(&[][..], Vec::as_slice),
        )?);
        liquidity_caps.push(liquidity_cap);
    }
    Ok(ReplayInputs {
        selected,
        windows,
        trade_windows,
        liquidity_caps,
    })
}
