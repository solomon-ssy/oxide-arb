//! Canonical `selector_hash` over a selection's inputs and result.
//!
//! The hash is the determinism contract of the selection plane: identical
//! strategy intent (config version + thresholds) over the same resulting selected
//! set must yield the same `blake3:` digest, regardless of candidate or config
//! insertion order. Every set-like field is sorted before it enters
//! [`SelectorHashInput`], which is then hashed verbatim by
//! [`ResearchHasher::canonical`].

use quant_pivot_models::types::{MarketId, RuntimeConfigVersionId};
use serde::Serialize;

use crate::selection::MarketSelectionBuildRequest;

/// The canonical, order-normalized shape hashed into a `selector_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SelectorHashInput {
    /// Decision time bucketed to whole seconds.
    pub as_of_bucket: i64,
    /// Governing runtime-config version.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Sorted enabled category slugs.
    pub enabled_categories: Vec<String>,
    /// Minimum liquidity threshold, as the verbatim config string.
    pub min_liquidity_usd: String,
    /// Minimum 24h volume threshold, as the verbatim config string.
    pub min_volume_24h_usd: String,
    /// Maximum spread threshold in basis points.
    pub max_spread_bps: u32,
    /// Whether near-resolution markets are admitted.
    pub allow_near_resolution: bool,
    /// Minimum seconds-to-resolution threshold.
    pub min_time_to_resolution_secs: u64,
    /// Maximum seconds-to-resolution threshold.
    pub max_time_to_resolution_secs: u64,
    /// Selection size cap.
    pub max_selection_size: u32,
    /// Maximum allowed published-book age, in milliseconds.
    pub max_book_age_ms: u64,
    /// Maximum allowed worst-case fact-write lag, in milliseconds.
    pub max_fact_lag_ms: u64,
    /// Reject crossed books.
    pub reject_crossed_books: bool,
    /// Reject empty (one-sided) books.
    pub reject_empty_books: bool,
    /// Sorted ids of the markets actually selected.
    pub selected_market_ids: Vec<String>,
}

impl SelectorHashInput {
    /// Build the canonical input from a request and its resulting selected set.
    #[must_use]
    pub fn new(request: &MarketSelectionBuildRequest, included: &[MarketId]) -> Self {
        let selection = &request.selection;
        let data_quality = &request.data_quality;

        let mut enabled_categories = selection
            .enabled_categories
            .iter()
            .map(|category| category.as_str().to_owned())
            .collect::<Vec<_>>();
        enabled_categories.sort();

        let mut selected_market_ids = included
            .iter()
            .map(|market_id| market_id.as_str().to_owned())
            .collect::<Vec<_>>();
        selected_market_ids.sort();

        Self {
            as_of_bucket: request.as_of.timestamp(),
            runtime_config_version_id: request.runtime_config_version_id.clone(),
            enabled_categories,
            min_liquidity_usd: selection.min_liquidity_usd.value.clone(),
            min_volume_24h_usd: selection.min_volume_24h_usd.value.clone(),
            max_spread_bps: selection.max_spread_bps,
            allow_near_resolution: selection.allow_near_resolution,
            min_time_to_resolution_secs: selection.min_time_to_resolution_secs,
            max_time_to_resolution_secs: selection.max_time_to_resolution_secs,
            max_selection_size: selection.max_selection_size,
            max_book_age_ms: data_quality.max_book_age_ms,
            max_fact_lag_ms: data_quality.max_fact_lag_secs.saturating_mul(1_000),
            reject_crossed_books: data_quality.reject_crossed_books,
            reject_empty_books: data_quality.reject_empty_books,
            selected_market_ids,
        }
    }
}
