//! Market-selection shared value types used by persistence and the research plane.

use std::{
    iter::Sum,
    ops::{Add, AddAssign},
};

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use super::ContentHash;

/// Non-sensitive component digests for diagnosing selector replay drift.
///
/// The canonical selector root remains authoritative. These hashes expose
/// which part of its preimage changed without persisting candidate facts or
/// policy values a second time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SelectorHashEvidence {
    /// Canonical hash of the complete selector preimage.
    pub selector_hash: ContentHash,
    /// Hash of decision time, policy identity, thresholds, and model contract.
    pub contract_hash: ContentHash,
    /// Hash of the decision boundary identity and knowledge lag.
    pub boundary_hash: ContentHash,
    /// Hash of the normalized selection thresholds.
    pub selection_policy_hash: ContentHash,
    /// Hash of the data-quality selection policy.
    pub data_quality_policy_hash: ContentHash,
    /// Governed feature schema hash embedded in the selector contract.
    pub feature_schema_hash: ContentHash,
    /// Routed model-requiredness hash embedded in the selector contract.
    pub model_requirements_hash: ContentHash,
    /// Hash of the complete normalized candidate world.
    pub candidates_hash: ContentHash,
    /// Hash of candidate catalog identity and metadata fields.
    pub candidate_catalog_hash: ContentHash,
    /// Hash of candidate book and runtime data-quality fields.
    pub candidate_book_hash: ContentHash,
    /// Hash of candidate domain-availability fields.
    pub candidate_domain_hash: ContentHash,
    /// Hash of each candidate's frozen decision time.
    pub candidate_decision_hash: ContentHash,
    /// Hash of the normalized included projection.
    pub included_hash: ContentHash,
    /// Hash of the ordered exclusion decisions.
    pub excluded_hash: ContentHash,
    /// Hash of the aggregate exclusion counts.
    pub exclusion_summary_hash: ContentHash,
}

/// Aggregate counts of exclusion reasons for a market selection snapshot.
///
/// Shared by the `quant_market_selection.exclusion_summary` JSONB column,
/// persistence DTOs (`MarketSelectionInfo`), and the research compute snapshot
/// (`MarketSelectionSnapshot`).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct SelectionExclusionSummary {
    /// Count excluded for a stale book.
    pub stale_book_count: u32,
    /// Count excluded for insufficient liquidity/volume.
    pub insufficient_liquidity_count: u32,
    /// Count excluded by an operator block.
    pub excluded_by_operator_count: u32,
    /// Count excluded because the pinned serving generation has no active route.
    pub route_not_activated_count: u32,
    /// Count excluded for any other reason.
    pub other_count: u32,
}

impl Add for SelectionExclusionSummary {
    type Output = Self;

    #[inline]
    fn add(mut self, rhs: Self) -> Self {
        self += rhs;
        self
    }
}

impl AddAssign for SelectionExclusionSummary {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.stale_book_count += rhs.stale_book_count;
        self.insufficient_liquidity_count += rhs.insufficient_liquidity_count;
        self.excluded_by_operator_count += rhs.excluded_by_operator_count;
        self.route_not_activated_count += rhs.route_not_activated_count;
        self.other_count += rhs.other_count;
    }
}

impl Sum for SelectionExclusionSummary {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}
