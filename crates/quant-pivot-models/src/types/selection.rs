//! Market-selection shared value types used by persistence and the research plane.

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::jsonb_active;

/// Aggregate counts of exclusion reasons for a market selection snapshot.
///
/// Shared by the `quant_market_selection.exclusion_summary` JSONB column,
/// persistence DTOs (`MarketSelectionInfo`), and the research compute snapshot
/// (`MarketSelectionSnapshot`).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult,
)]
pub struct SelectionExclusionSummary {
    /// Count excluded for a stale book.
    pub stale_book_count: u32,
    /// Count excluded for insufficient liquidity/volume.
    pub insufficient_liquidity_count: u32,
    /// Count excluded by an operator block.
    pub excluded_by_operator_count: u32,
    /// Count excluded for any other reason.
    pub other_count: u32,
}

jsonb_active!(SelectionExclusionSummary);
