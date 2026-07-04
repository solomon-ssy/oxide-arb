//! Market-selection shared value types used by persistence and the research plane.

use std::{
    iter::Sum,
    ops::{Add, AddAssign},
};

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
        self.other_count += rhs.other_count;
    }
}

impl Sum for SelectionExclusionSummary {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}

jsonb_active!(SelectionExclusionSummary);
