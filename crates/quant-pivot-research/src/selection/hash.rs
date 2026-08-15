//! Canonical `selector_hash` over a selection's inputs and result.
//!
//! The hash is the determinism contract of the selection plane: identical
//! strategy intent (config version + thresholds) over the same resulting selected
//! set must yield the same `blake3:` digest, regardless of candidate or config
//! insertion order. Every set-like field is sorted before it enters
//! [`SelectorHashInput`], which is then hashed verbatim by
//! [`ResearchHasher::canonical`].

use std::string::ToString;

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::{DomainAvailability, MarketCandidate, MarketDataHealth},
    enums::{common::MarketCategory, market::MarketStatus},
    types::{
        ContentHash, DecisionPolicySnapshotId, EventId, MarketId, Price, SelectionExclusionSummary,
        SelectorHashEvidence, TokenId, Usd,
    },
};
use serde::Serialize;

use crate::{
    features::AuthoringFeatureCatalog,
    hashing::ResearchHasher,
    selection::{ExcludedMarket, MarketSelectionBuildRequest, SelectedMarket},
};

#[derive(Serialize)]
struct SelectorBoundaryHashInput {
    decision_at: i64,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    knowledge_lag_secs: u64,
}

#[derive(Serialize)]
struct SelectionPolicyHashInput<'a> {
    enabled_categories: &'a [String],
    min_liquidity_usd: &'a str,
    min_volume_24h_usd: &'a str,
    max_spread_bps: u32,
    allow_near_resolution: bool,
    min_time_to_resolution_secs: u64,
    max_time_to_resolution_secs: u64,
}

#[derive(Serialize)]
struct DataQualityPolicyHashInput {
    max_book_age_ms: u64,
    max_ingest_lag_ms: u64,
    reject_crossed_books: bool,
    reject_empty_books: bool,
}

#[derive(Serialize)]
struct CandidateCatalogHashInput<'a> {
    market_id: &'a MarketId,
    event_id: &'a EventId,
    category: MarketCategory,
    status: MarketStatus,
    primary_token_id: &'a TokenId,
    secondary_token_id: &'a Option<TokenId>,
    end_date: Option<DateTime<Utc>>,
    liquidity_usd: Option<Usd>,
    volume_24h_usd: Option<Usd>,
}

#[derive(Serialize)]
struct CandidateBookHashInput<'a> {
    market_id: &'a MarketId,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
    depth_usd: Option<Usd>,
    book_age_ms: Option<u64>,
    crossed: Option<bool>,
    empty: Option<bool>,
    market_data_health: MarketDataHealth,
    ingest_lag_ms: Option<u64>,
}

#[derive(Serialize)]
struct CandidateDomainHashInput<'a> {
    market_id: &'a MarketId,
    domain_availability: DomainAvailability,
}

#[derive(Serialize)]
struct CandidateDecisionHashInput<'a> {
    market_id: &'a MarketId,
    decision_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct SelectorContractHashInput<'a> {
    decision_at: i64,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    enabled_categories: &'a [String],
    min_liquidity_usd: &'a str,
    min_volume_24h_usd: &'a str,
    max_spread_bps: u32,
    allow_near_resolution: bool,
    min_time_to_resolution_secs: u64,
    max_time_to_resolution_secs: u64,
    max_book_age_ms: u64,
    max_ingest_lag_ms: u64,
    reject_crossed_books: bool,
    reject_empty_books: bool,
    knowledge_lag_secs: u64,
    feature_schema_hash: ContentHash,
    model_requirements_hash: ContentHash,
}

/// The canonical, order-normalized shape hashed into a `selector_hash`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SelectorHashInput {
    /// Exact decision time in epoch milliseconds.
    pub decision_at: i64,
    /// Governing runtime-config version.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Sorted enabled category slugs.
    pub enabled_categories: Vec<String>,
    /// Minimum liquidity threshold, as a scale-independent decimal string.
    pub min_liquidity_usd: String,
    /// Minimum 24h volume threshold, as a scale-independent decimal string.
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
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        enabled_categories.sort();

        let mut candidates = candidates.to_vec();
        for candidate in &mut candidates {
            candidate.liquidity_usd = candidate.liquidity_usd.map(Usd::normalized);
            candidate.volume_24h_usd = candidate.volume_24h_usd.map(Usd::normalized);
            candidate.best_bid = candidate.best_bid.map(Price::normalized);
            candidate.best_ask = candidate.best_ask.map(Price::normalized);
            candidate.depth_usd = candidate.depth_usd.map(Usd::normalized);
        }
        candidates.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let mut included = included.to_vec();
        for market in &mut included {
            market.liquidity_usd = market.liquidity_usd.map(Usd::normalized);
            market.volume_24h_usd = market.volume_24h_usd.map(Usd::normalized);
        }
        included.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let mut excluded = excluded.to_vec();
        excluded.sort_by(|left, right| left.market_id.cmp(&right.market_id));

        Ok(Self {
            decision_at: request.decision_at.timestamp_millis(),
            decision_policy_snapshot_id: request.decision_policy_snapshot_id,
            enabled_categories,
            min_liquidity_usd: selection.min_liquidity_usd.value.normalize().to_string(),
            min_volume_24h_usd: selection.min_volume_24h_usd.value.normalize().to_string(),
            max_spread_bps: selection.max_spread_bps,
            allow_near_resolution: selection.allow_near_resolution,
            min_time_to_resolution_secs: selection.min_time_to_resolution_secs,
            max_time_to_resolution_secs: selection.max_time_to_resolution_secs,
            max_book_age_ms: data_quality.max_book_age_ms,
            max_ingest_lag_ms: data_quality.max_ingest_lag_ms,
            reject_crossed_books: data_quality.reject_crossed_books,
            reject_empty_books: data_quality.reject_empty_books,
            knowledge_lag_secs: request.knowledge_lag_secs,
            feature_schema_hash: ResearchHasher::authoring_catalog(
                &AuthoringFeatureCatalog::build(&request.features)?,
            )?,
            model_requirements_hash: ResearchHasher::model_feature_requirements(
                &request.model_requirements,
            )?,
            candidates,
            included,
            excluded,
            exclusion_summary,
        })
    }

    /// Hash the canonical root and each major preimage component.
    pub fn evidence(&self) -> QuantResult<SelectorHashEvidence> {
        let contract = SelectorContractHashInput {
            decision_at: self.decision_at,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            enabled_categories: &self.enabled_categories,
            min_liquidity_usd: &self.min_liquidity_usd,
            min_volume_24h_usd: &self.min_volume_24h_usd,
            max_spread_bps: self.max_spread_bps,
            allow_near_resolution: self.allow_near_resolution,
            min_time_to_resolution_secs: self.min_time_to_resolution_secs,
            max_time_to_resolution_secs: self.max_time_to_resolution_secs,
            max_book_age_ms: self.max_book_age_ms,
            max_ingest_lag_ms: self.max_ingest_lag_ms,
            reject_crossed_books: self.reject_crossed_books,
            reject_empty_books: self.reject_empty_books,
            knowledge_lag_secs: self.knowledge_lag_secs,
            feature_schema_hash: self.feature_schema_hash,
            model_requirements_hash: self.model_requirements_hash,
        };
        let boundary = SelectorBoundaryHashInput {
            decision_at: self.decision_at,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            knowledge_lag_secs: self.knowledge_lag_secs,
        };
        let selection_policy = SelectionPolicyHashInput {
            enabled_categories: &self.enabled_categories,
            min_liquidity_usd: &self.min_liquidity_usd,
            min_volume_24h_usd: &self.min_volume_24h_usd,
            max_spread_bps: self.max_spread_bps,
            allow_near_resolution: self.allow_near_resolution,
            min_time_to_resolution_secs: self.min_time_to_resolution_secs,
            max_time_to_resolution_secs: self.max_time_to_resolution_secs,
        };
        let data_quality_policy = DataQualityPolicyHashInput {
            max_book_age_ms: self.max_book_age_ms,
            max_ingest_lag_ms: self.max_ingest_lag_ms,
            reject_crossed_books: self.reject_crossed_books,
            reject_empty_books: self.reject_empty_books,
        };
        let candidate_catalog = self
            .candidates
            .iter()
            .map(|candidate| CandidateCatalogHashInput {
                market_id: &candidate.market_id,
                event_id: &candidate.event_id,
                category: candidate.category,
                status: candidate.status,
                primary_token_id: &candidate.primary_token_id,
                secondary_token_id: &candidate.secondary_token_id,
                end_date: candidate.end_date,
                liquidity_usd: candidate.liquidity_usd,
                volume_24h_usd: candidate.volume_24h_usd,
            })
            .collect::<Vec<_>>();
        let candidate_books = self
            .candidates
            .iter()
            .map(|candidate| CandidateBookHashInput {
                market_id: &candidate.market_id,
                best_bid: candidate.best_bid,
                best_ask: candidate.best_ask,
                depth_usd: candidate.depth_usd,
                book_age_ms: candidate.book_age_ms,
                crossed: candidate.crossed,
                empty: candidate.empty,
                market_data_health: candidate.market_data_health,
                ingest_lag_ms: candidate.ingest_lag_ms,
            })
            .collect::<Vec<_>>();
        let candidate_domains = self
            .candidates
            .iter()
            .map(|candidate| CandidateDomainHashInput {
                market_id: &candidate.market_id,
                domain_availability: candidate.domain_availability,
            })
            .collect::<Vec<_>>();
        let candidate_decisions = self
            .candidates
            .iter()
            .map(|candidate| CandidateDecisionHashInput {
                market_id: &candidate.market_id,
                decision_at: candidate.decision_at,
            })
            .collect::<Vec<_>>();
        Ok(SelectorHashEvidence {
            selector_hash: ResearchHasher::canonical(self)?,
            contract_hash: ResearchHasher::canonical(&contract)?,
            boundary_hash: ResearchHasher::canonical(&boundary)?,
            selection_policy_hash: ResearchHasher::canonical(&selection_policy)?,
            data_quality_policy_hash: ResearchHasher::canonical(&data_quality_policy)?,
            feature_schema_hash: self.feature_schema_hash,
            model_requirements_hash: self.model_requirements_hash,
            candidates_hash: ResearchHasher::canonical(&self.candidates)?,
            candidate_catalog_hash: ResearchHasher::canonical(&candidate_catalog)?,
            candidate_book_hash: ResearchHasher::canonical(&candidate_books)?,
            candidate_domain_hash: ResearchHasher::canonical(&candidate_domains)?,
            candidate_decision_hash: ResearchHasher::canonical(&candidate_decisions)?,
            included_hash: ResearchHasher::canonical(&self.included)?,
            excluded_hash: ResearchHasher::canonical(&self.excluded)?,
            exclusion_summary_hash: ResearchHasher::canonical(&self.exclusion_summary)?,
        })
    }
}
