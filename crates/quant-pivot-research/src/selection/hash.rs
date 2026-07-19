//! Canonical `selector_hash` over a selection's inputs and result.
//!
//! The hash is the determinism contract of the selection plane: identical
//! strategy intent (config version + thresholds) over the same resulting selected
//! set must yield the same `blake3:` digest, regardless of candidate or config
//! insertion order. Every set-like field is sorted before it enters
//! [`SelectorHashInput`], which is then hashed verbatim by
//! [`ResearchHasher::canonical`].

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::MarketCandidate,
    types::{ContentHash, DecisionPolicySnapshotId, SelectionExclusionSummary},
};
use serde::Serialize;

use crate::{
    features::FeatureSchema,
    hashing::ResearchHasher,
    selection::{ExcludedMarket, MarketSelectionBuildRequest, SelectedMarket},
};

/// The canonical, order-normalized shape hashed into a `selector_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SelectorHashInput {
    /// Exact decision time in epoch milliseconds.
    pub decision_at: i64,
    /// Governing runtime-config version.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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
    /// Maximum allowed published-book age, in milliseconds.
    pub max_book_age_ms: u64,
    /// Maximum allowed worst-case ingest pipeline lag (enqueue→flush), in ms.
    pub max_ingest_lag_ms: u64,
    /// Reject crossed books.
    pub reject_crossed_books: bool,
    /// Reject empty (one-sided) books.
    pub reject_empty_books: bool,
    /// Global knowledge lag frozen for the round.
    pub knowledge_lag_secs: u64,
    /// Governed feature schema used by model-eligibility filtering.
    pub feature_schema_hash: ContentHash,
    /// Ordered, category-aware model requiredness contract.
    pub model_requirements_hash: ContentHash,
    /// Complete candidate world, sorted by market id.
    pub candidates: Vec<MarketCandidate>,
    /// Complete selected projection, sorted by market id.
    pub included: Vec<SelectedMarket>,
    /// Complete exclusion result, sorted by market id.
    pub excluded: Vec<ExcludedMarket>,
    /// Aggregate exclusion counts persisted with the snapshot.
    pub exclusion_summary: SelectionExclusionSummary,
}

impl SelectorHashInput {
    /// Build the canonical input from the complete request, candidate world and
    /// included/excluded result. Membership alone is insufficient: a changed
    /// source fact or rejection reason must also perturb the selector digest.
    pub fn new(
        request: &MarketSelectionBuildRequest,
        candidates: &[MarketCandidate],
        included: &[SelectedMarket],
        excluded: &[ExcludedMarket],
        exclusion_summary: SelectionExclusionSummary,
    ) -> QuantResult<Self> {
        let selection = &request.selection;
        let data_quality = &request.data_quality;

        let mut enabled_categories = selection
            .enabled_categories
            .iter()
            .map(|category| category.as_str().to_owned())
            .collect::<Vec<_>>();
        enabled_categories.sort();

        let mut candidates = candidates.to_vec();
        candidates.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let mut included = included.to_vec();
        included.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let mut excluded = excluded.to_vec();
        excluded.sort_by(|left, right| left.market_id.cmp(&right.market_id));

        Ok(Self {
            decision_at: request.decision_at.timestamp_millis(),
            decision_policy_snapshot_id: request.decision_policy_snapshot_id.clone(),
            enabled_categories,
            min_liquidity_usd: selection.min_liquidity_usd.value.to_string(),
            min_volume_24h_usd: selection.min_volume_24h_usd.value.to_string(),
            max_spread_bps: selection.max_spread_bps,
            allow_near_resolution: selection.allow_near_resolution,
            min_time_to_resolution_secs: selection.min_time_to_resolution_secs,
            max_time_to_resolution_secs: selection.max_time_to_resolution_secs,
            max_book_age_ms: data_quality.max_book_age_ms,
            max_ingest_lag_ms: data_quality.max_ingest_lag_ms,
            reject_crossed_books: data_quality.reject_crossed_books,
            reject_empty_books: data_quality.reject_empty_books,
            knowledge_lag_secs: request.knowledge_lag_secs,
            feature_schema_hash: ResearchHasher::feature_schema(&FeatureSchema::build(
                &request.features,
            )?)?,
            model_requirements_hash: ResearchHasher::model_feature_requirements(
                &request.model_requirements,
            )?,
            candidates,
            included,
            excluded,
            exclusion_summary,
        })
    }
}
