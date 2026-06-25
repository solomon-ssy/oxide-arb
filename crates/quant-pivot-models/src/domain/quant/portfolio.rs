//! Portfolio plan persistence DTOs.
//!
//! The JSONB columns are strong-typed (`types::portfolio_plan`) — never a bare
//! `serde_json::Value`.

use crate::types::{
    MarketSelectionId, ModelRunId, PortfolioConstraintsSnapshot, PortfolioPlanId,
    PortfolioRejectedSummary, PortfolioRiskBudget, Usd,
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted portfolio pruning and allocation result.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_portfolio_plan::Entity")]
pub struct PortfolioPlanInfo {
    pub portfolio_plan_id: PortfolioPlanId,
    pub model_run_id: ModelRunId,
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub budget_usd: Usd,
    pub allocated_usd: Usd,
    pub risk_budget_json: PortfolioRiskBudget,
    pub constraints_json: PortfolioConstraintsSnapshot,
    pub rejected_summary: PortfolioRejectedSummary,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PortfolioPlanInfo, crate::entities::quant_portfolio_plan::Model, {
    portfolio_plan_id, model_run_id, market_selection_id, as_of, budget_usd,
    allocated_usd, risk_budget_json, constraints_json, rejected_summary, created_at,
});

/// Insert payload for `quant_portfolio_plan`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_portfolio_plan::ActiveModel")]
pub struct NewPortfolioPlan {
    pub portfolio_plan_id: PortfolioPlanId,
    pub model_run_id: ModelRunId,
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub budget_usd: Usd,
    pub allocated_usd: Usd,
    pub risk_budget_json: PortfolioRiskBudget,
    pub constraints_json: PortfolioConstraintsSnapshot,
    pub rejected_summary: PortfolioRejectedSummary,
}
