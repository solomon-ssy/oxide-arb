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

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::DataQualityStatus,
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig},
    types::Price,
};
use quant_pivot_research::{
    factors::{FactorEngine, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, FeatureVector, MarketWindowSnapshot, PitView, ResolvedBook,
    },
    pit::PitQueryEngine,
    selection::SelectedMarket,
};

use crate::pipeline::historical_window::{
    Prefetched, ReplaySample, feature_window, selected_market,
};

/// Frozen feature/factor/data-quality config governing a replay.
pub struct ReplayConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
}

/// Per-call inputs for materializing one cross-section from a prefetched window.
pub struct CrossSectionRequest<'a> {
    /// Point-in-time engine resolving books/markets visible at `as_of`.
    pub pit: &'a dyn PitQueryEngine,
    /// Prefetched facts backing the trailing feature windows.
    pub prefetched: &'a Prefetched,
    /// Decision time to resolve the cross-section as of.
    pub as_of: DateTime<Utc>,
    /// `(market, token)` samples in this `as_of` group.
    pub group: &'a [ReplaySample],
    /// Source visibility delay applied to features.
    pub source_delay: Duration,
    /// Maximum trailing feature lookback.
    pub lookback: Duration,
}

/// One PIT-resolved cross-section: aligned feature vectors, selection snapshots,
/// entry mids, and factor outcomes for a single `as_of`.
///
/// All four vectors are index-aligned (the `i`-th vector, market, entry mid, and
/// outcome describe the same market).
pub struct ReplayCrossSection {
    /// Decision time the cross-section was resolved as of.
    pub as_of: DateTime<Utc>,
    /// Resolved feature vectors that passed the data-quality bar.
    pub vectors: Vec<FeatureVector>,
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
    let as_of = request.as_of;
    let (selected, windows) = cross_section_inputs(
        request.group,
        request.prefetched,
        as_of,
        request.source_delay,
        request.lookback,
    );
    if selected.is_empty() {
        return Ok(None);
    }

    let pit_view = PitView::Historical(request.pit);
    let resolve_futures = selected
        .iter()
        .zip(windows.iter())
        .map(|(market, snapshot)| builder.resolve_inputs(market, as_of, pit_view, snapshot));
    let resolved = try_join_all(resolve_futures).await?;

    let vectors = builder.build_batch(&resolved, &[], &config.features, &config.data_quality);

    let mut kept_vectors = Vec::with_capacity(vectors.len());
    let mut kept_markets = Vec::with_capacity(vectors.len());
    let mut kept_entry_mids = Vec::with_capacity(vectors.len());
    let mut dropped_insufficient = 0_u64;
    for ((vector, input), market) in vectors
        .into_iter()
        .zip(resolved.iter())
        .zip(selected.iter())
    {
        if vector.data_quality == DataQualityStatus::Insufficient {
            dropped_insufficient += 1;
            continue;
        }
        kept_entry_mids.push(input.book.as_ref().and_then(ResolvedBook::mid));
        kept_markets.push(market.clone());
        kept_vectors.push(vector);
    }
    if kept_vectors.is_empty() {
        return Ok(Some(ReplayCrossSection {
            as_of,
            vectors: Vec::new(),
            markets: Vec::new(),
            entry_mids: Vec::new(),
            outcomes: Vec::new(),
            dropped_insufficient,
        }));
    }

    FactorEngine::validate_batch_invariants(&kept_vectors)?;
    let outcomes = engine.compute_all_batch(&kept_vectors, &config.factors)?;

    Ok(Some(ReplayCrossSection {
        as_of,
        vectors: kept_vectors,
        markets: kept_markets,
        entry_mids: kept_entry_mids,
        outcomes,
        dropped_insufficient,
    }))
}

/// Build selected markets and trailing feature windows for one cross-section.
fn cross_section_inputs(
    group: &[ReplaySample],
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
