//! Strong-typed portfolio-plan JSONB column content types.
//!
//! Content contract for `quant_portfolio_plan.risk_budget_json` /
//! `constraints_json` / `rejected_summary`. Produced by the governed planner
//! (04.1); defined here so the entity uses them directly as JSONB columns.

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    jsonb_active,
    types::{RejectionReasonCount, Usd},
};

/// Resolved capital budget for one portfolio plan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct PortfolioRiskBudget {
    /// Total deployable budget (governance cap).
    pub total_budget_usd: Usd,
    /// Capital base used for sizing (`equity = min(net liquidation, budget)`).
    pub capital_base_usd: Usd,
    /// Capital reserved by pending intents at decision time.
    pub reserved_usd: Usd,
    /// USD allocated across the plan.
    pub allocated_usd: Usd,
    /// USD remaining after allocation.
    pub remaining_usd: Usd,
}

/// Resolved exposure constraints applied by one portfolio plan.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct PortfolioConstraintsSnapshot {
    /// Max USD exposure per market.
    pub max_market_exposure_usd: Usd,
    /// Max USD exposure per event.
    pub max_event_exposure_usd: Usd,
    /// Max USD exposure per category.
    pub max_category_exposure_usd: Usd,
    /// Max USD correlated exposure.
    pub max_correlated_exposure_usd: Usd,
    /// Max USD allocated to a single recommendation.
    pub max_single_recommendation_usd: Usd,
    /// Minimum useful recommendation size.
    pub min_recommendation_usd: Usd,
    /// Fraction of visible liquidity an allocation may consume.
    pub liquidity_usage_cap_pct: Decimal,
}

/// Summary of candidates rejected during portfolio allocation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct PortfolioRejectedSummary {
    /// Total rejected candidate count.
    pub rejected_count: u32,
    /// Rejection reasons with counts.
    pub reasons: Vec<RejectionReasonCount>,
}

jsonb_active!(
    PortfolioRiskBudget,
    PortfolioConstraintsSnapshot,
    PortfolioRejectedSummary,
);
