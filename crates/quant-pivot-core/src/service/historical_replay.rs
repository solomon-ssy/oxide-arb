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

use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures_util::future::try_join_all;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::market::registry::NegRiskLegSet,
    enums::quant::DataQualityStatus,
    runtime_config::{DataQualityConfig, FactorsConfig, FeaturesConfig, SmallCrossSectionPolicy},
    types::{Price, Usd},
};
use quant_pivot_research::{
    factors::{FactorEngine, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, FeatureSourceWindows, FeatureVector, MarketWindowSnapshot,
        PitView, ResolvedBook, TradeTapeWindowSnapshot,
    },
    model::FavoriteLongshotBiasTable,
    pit::PitQueryEngine,
    selection::SelectedMarket,
};

use crate::pipeline::historical_window::{
    Prefetched, ReplaySample, feature_window, selected_market, trade_tape_window,
};

/// Frozen feature/factor/data-quality config governing a replay.
#[derive(Clone)]
pub struct ReplayConfig {
    /// Feature builder configuration.
    pub features: FeaturesConfig,
    /// Factor engine configuration.
    pub factors: FactorsConfig,
    /// Data-quality gates applied during feature build.
    pub data_quality: DataQualityConfig,
    /// Favorite-longshot bias table pinned by the frozen factor config (content-
    /// hash verified). `None` keeps `struct.favorite_longshot` inert. Bound here
    /// so the offline engine scores byte-identically to the online plane.
    pub bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
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
    // Offline replay (training / backtest) shares the online engine entrypoint,
    // but the `HistoricalQuantile` fallback needs a PIT-correct rolling factor
    // history — which the online plane prefetches from persisted factor values.
    // Reconstructing that offline without look-ahead is 11.6's job. Until then we
    // fail closed rather than silently normalize offline with an empty history
    // (which would produce indeterminate factors offline while the online plane
    // scores them — a hidden train-serve skew). The default policy is
    // `Indeterminate`, under which offline and online are byte-identical.
    if config.factors.cross_section.small_cross_section_policy
        == SmallCrossSectionPolicy::HistoricalQuantile
    {
        return Err(QuantError::config(
            "offline replay does not support the HistoricalQuantile small-cross-section policy \
             (PIT-correct offline factor history lands in 11.6); train and backtest under the \
             Indeterminate policy to preserve training-serving parity",
        ));
    }

    let as_of = request.as_of;
    let (selected, windows, trade_windows) = cross_section_inputs(
        request.group,
        request.prefetched,
        as_of,
        request.source_delay,
        request.lookback,
        Duration::from_secs(config.features.structural.trade_tape_window_secs),
    );
    if selected.is_empty() {
        return Ok(None);
    }

    let pit_view = PitView::Historical(request.pit);
    let resolve_futures = selected
        .iter()
        .zip(windows.iter())
        .zip(trade_windows.iter())
        .map(|((market, snapshot), trade_snapshot)| {
            // Neg-risk full-leg PIT (offline): enumerate the event's YES legs from
            // the prefetched window and resolve each book at the same `as_of`
            // through the same `resolve_book` the online plane uses — byte-identical.
            let sibling = sibling_leg_set(market, request.prefetched);
            async move {
                builder
                    .resolve_inputs(
                        market,
                        as_of,
                        pit_view,
                        FeatureSourceWindows {
                            microstructure: snapshot,
                            trade_tape: trade_snapshot,
                        },
                        &sibling,
                        Usd::ZERO,
                    )
                    .await
            }
        });
    let bundles = try_join_all(resolve_futures).await?;

    let vectors = builder.build_batch(&bundles, &[], &config.features, &config.data_quality);

    let mut kept_vectors = Vec::with_capacity(vectors.len());
    let mut kept_markets = Vec::with_capacity(vectors.len());
    let mut kept_entry_mids = Vec::with_capacity(vectors.len());
    let mut dropped_insufficient = 0_u64;
    for ((vector, bundle), market) in vectors.into_iter().zip(bundles.iter()).zip(selected.iter()) {
        if vector.data_quality == DataQualityStatus::Insufficient {
            dropped_insufficient += 1;
            continue;
        }
        kept_entry_mids.push(bundle.inputs.book.as_ref().and_then(ResolvedBook::mid));
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

/// The neg-risk YES-leg set for a market's event from the prefetched window.
///
/// Mirrors `MarketRegistry::neg_risk_leg_set` (online): empty for binary /
/// non-neg-risk markets; otherwise the neg-risk leg count with resolvable legs
/// populated (`expected_legs` excludes non-neg-risk event members).
fn sibling_leg_set(market: &SelectedMarket, prefetched: &Prefetched) -> NegRiskLegSet {
    let Some(info) = prefetched.markets_by_id.get(&market.market_id) else {
        return NegRiskLegSet::empty();
    };
    if !info.neg_risk {
        return NegRiskLegSet::empty();
    }
    prefetched
        .neg_risk_leg_sets
        .get(&info.event_id)
        .cloned()
        .unwrap_or(NegRiskLegSet::empty())
}

/// Build selected markets and trailing feature windows for one cross-section.
fn cross_section_inputs(
    group: &[ReplaySample],
    prefetched: &Prefetched,
    as_of: DateTime<Utc>,
    source_delay: Duration,
    lookback: Duration,
    trade_lookback: Duration,
) -> (
    Vec<SelectedMarket>,
    Vec<MarketWindowSnapshot>,
    Vec<TradeTapeWindowSnapshot>,
) {
    let mut selected = Vec::with_capacity(group.len());
    let mut windows = Vec::with_capacity(group.len());
    let mut trade_windows = Vec::with_capacity(group.len());
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
        trade_windows.push(trade_tape_window(
            sample.market_id.clone(),
            as_of,
            source_delay,
            trade_lookback,
            prefetched
                .trade_tape
                .get(&sample.market_id)
                .map_or(&[][..], Vec::as_slice),
        ));
    }
    (selected, windows, trade_windows)
}
