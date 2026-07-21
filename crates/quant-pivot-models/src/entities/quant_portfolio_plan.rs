//! `quant_portfolio_plan` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_market_selection, quant_model_run};
use crate::types::{
    MarketSelectionId, ModelRunId, PortfolioConstraintsSnapshot, PortfolioOptimizerMeta,
    PortfolioPlanId, PortfolioRejectedSummary, PortfolioRiskBudget, Usd,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_portfolio_plan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub portfolio_plan_id: PortfolioPlanId,
    pub model_run_id: Option<ModelRunId>,
    pub market_selection_id: MarketSelectionId,
    pub decision_at: DateTime<Utc>,
    pub budget_usd: Usd,
    pub allocated_usd: Usd,
    #[sea_orm(column_type = "JsonBinary")]
    pub risk_budget_json: PortfolioRiskBudget,
    #[sea_orm(column_type = "JsonBinary")]
    pub constraints_json: PortfolioConstraintsSnapshot,
    #[sea_orm(column_type = "JsonBinary")]
    pub rejected_summary: PortfolioRejectedSummary,
    #[sea_orm(column_type = "JsonBinary")]
    pub optimizer_meta_json: PortfolioOptimizerMeta,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<Option<quant_model_run::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<quant_market_selection::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
