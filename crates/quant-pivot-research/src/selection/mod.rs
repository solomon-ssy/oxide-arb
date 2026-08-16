//! Market selection plane: the [`MarketSelector`] contract and its
//! compute-domain snapshot types.
//!
//! Online closure entry: a deterministic, replayable, hashable
//! [`MarketSelectionSnapshot`] of which markets enter a research/report round.
//! The seven-filter pipeline, pure selector, and canonical hash live here; the
//! Postgres repository lives in `quant-pivot-repository`.
//!
//! The selector is a **pure function** of two frozen inputs: a
//! [`MarketSelectionBuildRequest`] (strategy intent: config + model
//! requirements) and a `Vec<MarketCandidate>` (the decision-time freeze of every
//! market fact, owned by `quant-pivot-models`). Given the same two inputs it
//! always produces the same [`MarketSelectionSnapshot::selector_hash`].

mod filters;
mod hash;
mod selector;

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
pub use filters::{
    CategoryFilter, DataQualityFilter, FilterChain, FilterOutcome, LiquidityFilter,
    MarketCandidateCtx, MarketStatusFilter, ModelEligibilityFilter, ResolutionAmbiguityFilter,
    SelectionFilter, SelectionThresholds, accumulate_exclusion,
};
pub use hash::SelectorHashInput;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::MarketCandidate,
    enums::common::MarketCategory,
    runtime_config::{BuyModelRoute, DataQualityConfig, FeaturesConfig, SelectionConfig},
    types::{
        ContentHash, DecisionPolicySnapshotId, EventId, MarketId, MarketSelectionId,
        ModelInputContract, ModelInputRequiredness, SelectionExclusionSummary,
        SelectionMemberEvidence, SelectorHashEvidence, TokenId, Usd,
    },
};
pub use selector::ConfiguredMarketSelector;
use serde::{Deserialize, Serialize};

use crate::features::{EvidenceSourceRef, FeatureName};

/// Builds a deterministic market selection snapshot from frozen config and a
/// frozen slice of market-candidate facts.
#[async_trait]
pub trait MarketSelector: Send + Sync {
    /// Build the snapshot for one research/report round.
    ///
    /// `request` carries strategy intent (config + model requirements);
    /// `candidates` carries the frozen world facts. The result is a pure
    /// function of both — no I/O, no clock, no mutable runtime state.
    async fn build_snapshot(
        &self,
        request: MarketSelectionBuildRequest,
        candidates: Vec<MarketCandidate>,
    ) -> QuantResult<MarketSelectionSnapshot>;
}

/// Frozen inputs to a selection build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSelectionBuildRequest {
    /// Decision time.
    pub decision_at: DateTime<Utc>,
    /// Config version governing this selection.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Frozen selection-policy snapshot.
    pub selection: SelectionConfig,
    /// Frozen data-quality snapshot.
    pub data_quality: DataQualityConfig,
    /// Frozen feature snapshot (drives the availability oracle's schema).
    pub features: FeaturesConfig,
    /// Feature availability the active model requires.
    pub model_requirements: ModelFeatureRequirements,
    /// Source visibility delay, in seconds.
    pub knowledge_lag_secs: u64,
    /// Report-only route availability pinned from one immutable serving generation.
    /// Offline PIT selection leaves this absent and relies on its cohort contract.
    pub route_availability: Option<RouteAvailabilityContract>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAvailabilityContract {
    pub primary_route: BuyModelRoute,
    pub active_routes: Vec<BuyModelRoute>,
    pub universe_plan_hash: ContentHash,
}

impl RouteAvailabilityContract {
    #[must_use]
    pub fn accepts(&self, category: MarketCategory) -> bool {
        self.active_routes.contains(&BuyModelRoute::from(category))
    }
}

/// Feature availability requirements imposed by the routed model(s) on selection.
///
/// A market's actual eligibility bar depends on the exact serving route that
/// scores it. A category-specific route must be checked against that route's
/// own required features, while its domain requirements must never gate a
/// different route. Use
/// [`Self::for_category`] to resolve the actual bar for one candidate.
///
/// Hash via [`crate::hashing::ResearchHasher::model_feature_requirements`] so
/// insertion order (of either `generic` or `by_category`'s per-category
/// vectors) never affects the selector digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ModelFeatureRequirements {
    /// Baseline requirements checked against every candidate.
    #[serde(default)]
    pub generic: Vec<FeatureName>,
    /// Required by the category-specific model actually routed for each
    /// category (only present for a route in the verified serving generation),
    /// additive to `generic` and scoped to candidates of exactly that category.
    #[serde(default)]
    pub by_category: BTreeMap<MarketCategory, Vec<FeatureName>>,
}

impl ModelFeatureRequirements {
    /// One baseline requirement set with no category-specific additions.
    #[must_use]
    pub const fn generic_only(required_features: Vec<FeatureName>) -> Self {
        Self {
            generic: required_features,
            by_category: BTreeMap::new(),
        }
    }

    /// Project one model spec's typed input contract into selection gating.
    /// Optional inputs remain visible to the transform but never reject a
    /// candidate before model-input materialization.
    #[must_use]
    pub fn from_input_contract(contract: &ModelInputContract) -> Self {
        Self::generic_only(
            contract
                .inputs
                .iter()
                .filter(|input| input.requiredness == ModelInputRequiredness::Required)
                .map(|input| FeatureName::new(input.feature_name.clone()))
                .collect(),
        )
    }

    /// The full, deduplicated requirement set for a candidate of `category`:
    /// `generic` ∪ that category's specific requirements.
    #[must_use]
    pub fn for_category(&self, category: MarketCategory) -> Vec<FeatureName> {
        let mut set: BTreeSet<FeatureName> = self.generic.iter().cloned().collect();
        if let Some(extra) = self.by_category.get(&category) {
            set.extend(extra.iter().cloned());
        }
        set.into_iter().collect()
    }

    /// Every feature required by any route (`generic` ∪ every category's
    /// specific set), for consumers that need one flat, category-agnostic
    /// superset (e.g. the required-input feature gate, which is not
    /// selection's per-category eligibility check).
    #[must_use]
    pub fn union_all(&self) -> Vec<FeatureName> {
        let mut set: BTreeSet<FeatureName> = self.generic.iter().cloned().collect();
        for extra in self.by_category.values() {
            set.extend(extra.iter().cloned());
        }
        set.into_iter().collect()
    }

    /// Merge exact per-category requirements from another represented Route.
    pub fn merge(&mut self, other: Self) {
        let mut generic = self.generic.iter().cloned().collect::<BTreeSet<_>>();
        generic.extend(other.generic);
        self.generic = generic.into_iter().collect();
        for (category, required) in other.by_category {
            let entry = self.by_category.entry(category).or_default();
            let mut merged = entry.iter().cloned().collect::<BTreeSet<_>>();
            merged.extend(required);
            *entry = merged.into_iter().collect();
        }
    }
}

/// The included / excluded partition produced by the filter chain + cap, before
/// a snapshot id or canonical hash is attached.
///
/// This is the shared selection core: the online snapshot builder wraps it with
/// an id + hash, while the offline point-in-time dataset selector consumes it
/// per `as_of` cross-section (no persisted snapshot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionResult {
    /// Markets that passed every filter (already capped + stably ordered).
    pub included: Vec<SelectedMarket>,
    /// Excluded markets with their deciding reason.
    pub excluded: Vec<ExcludedMarket>,
    /// Aggregate exclusion summary.
    pub exclusion_summary: SelectionExclusionSummary,
}

/// A deterministic, hashable snapshot of selected and excluded markets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSelectionSnapshot {
    /// Snapshot id.
    pub market_selection_id: MarketSelectionId,
    /// Decision time.
    pub decision_at: DateTime<Utc>,
    /// Governing config version.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Canonical selector hash (same inputs → same hash).
    pub selector_hash: ContentHash,
    /// Component-level commitments for diagnosing replay drift without exposing facts.
    pub selector_evidence: SelectorHashEvidence,
    /// Included markets.
    pub included: Vec<SelectedMarket>,
    /// Excluded markets with reasons.
    pub excluded: Vec<ExcludedMarket>,
    /// Aggregate exclusion summary.
    pub exclusion_summary: SelectionExclusionSummary,
}

/// A market that passed every filter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Owning event (always present for Polymarket catalog-backed selections).
    pub event_id: EventId,
    /// Market category.
    pub category: MarketCategory,
    /// Primary outcome token.
    pub primary_token_id: TokenId,
    /// Secondary outcome token (binary markets).
    pub secondary_token_id: Option<TokenId>,
    /// Reported liquidity, when known.
    pub liquidity_usd: Option<Usd>,
    /// Reported 24h volume, when known.
    pub volume_24h_usd: Option<Usd>,
    /// Provenance of the selection evidence.
    pub source_refs: Vec<EvidenceSourceRef>,
}

impl From<&MarketCandidate> for SelectedMarket {
    fn from(candidate: &MarketCandidate) -> Self {
        Self {
            market_id: candidate.market_id.clone(),
            event_id: candidate.event_id.clone(),
            category: candidate.category,
            primary_token_id: candidate.primary_token_id.clone(),
            secondary_token_id: candidate.secondary_token_id.clone(),
            liquidity_usd: candidate.liquidity_usd,
            volume_24h_usd: candidate.volume_24h_usd,
            source_refs: Vec::new(),
        }
    }
}

impl From<&SelectedMarket> for SelectionMemberEvidence {
    fn from(market: &SelectedMarket) -> Self {
        Self {
            market_id: market.market_id.clone(),
            event_id: market.event_id.clone(),
            category: market.category,
            primary_token_id: market.primary_token_id.clone(),
            secondary_token_id: market.secondary_token_id.clone(),
            liquidity_usd: market.liquidity_usd,
            volume_24h_usd: market.volume_24h_usd,
            source_refs: market.source_refs.clone(),
        }
    }
}

impl From<&SelectionMemberEvidence> for SelectedMarket {
    fn from(market: &SelectionMemberEvidence) -> Self {
        Self {
            market_id: market.market_id.clone(),
            event_id: market.event_id.clone(),
            category: market.category,
            primary_token_id: market.primary_token_id.clone(),
            secondary_token_id: market.secondary_token_id.clone(),
            liquidity_usd: market.liquidity_usd,
            volume_24h_usd: market.volume_24h_usd,
            source_refs: market.source_refs.clone(),
        }
    }
}

/// A market that was filtered out, with the deciding reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Owning event frozen with the catalog-visible candidate.
    pub event_id: EventId,
    /// Primary token frozen with the catalog-visible candidate.
    pub primary_token_id: TokenId,
    /// Why it was excluded.
    pub reason: ExclusionReason,
}

/// Why a market was excluded from the selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    /// Market is not open/active.
    NotOpen,
    /// Category disabled by config.
    CategoryDisabled,
    /// Below the liquidity / volume thresholds.
    InsufficientLiquidity,
    /// Spread wider than allowed.
    SpreadTooWide,
    /// Book staler than allowed.
    StaleBook,
    /// Ingest pipeline lag (enqueue→flush) exceeded the threshold.
    IngestLagExceeded,
    /// Resolution is near/ambiguous.
    ResolutionAmbiguous,
    /// Manually blocked by an operator.
    ManuallyBlocked,
    /// The category-owned route is absent from the pinned serving generation.
    RouteNotActivated,
    /// The model requires features unavailable for this market.
    ModelFeatureUnavailable {
        /// The missing required features.
        missing: Vec<FeatureName>,
    },
}
