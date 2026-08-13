//! `quant_recommendation` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    event, market, quant_execution_rollup_reconciliation_task, quant_order_intent,
    quant_portfolio_plan, quant_recommendation_execution_rollup, quant_recommendation_report,
    quant_recommendation_resolution_outcome, quant_report_route_run,
    quant_resolution_outcome_reconciliation_task,
};
use crate::{
    domain::quant::{ExecutableEconomicTier, RecommendationEconomics},
    enums::quant::{OutcomeSide, RecommendationStatus},
    runtime_config::BuyModelRoute,
    types::{
        EconomicTierId, EventId, EvidenceRefs, ExecutionEligibility, MarketContext, MarketId,
        PortfolioPlanId, RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ReportRouteRunId, TokenId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub economic_tier_id: EconomicTierId,
    pub rank: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    #[sea_orm(column_type = "JsonBinary")]
    pub economics_json: RecommendationEconomics,
    #[sea_orm(column_type = "JsonBinary")]
    pub economic_tier_json: ExecutableEconomicTier,
    #[sea_orm(column_type = "JsonBinary")]
    pub identity: RecommendationIdentity,
    #[sea_orm(column_type = "JsonBinary")]
    pub market_context: MarketContext,
    #[sea_orm(column_type = "JsonBinary")]
    pub trade_plan: RecommendationTradePlan,
    #[sea_orm(column_type = "JsonBinary")]
    pub factor_breakdown: RecommendationFactorBreakdown,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence_refs: EvidenceRefs,
    #[sea_orm(column_type = "JsonBinary")]
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    pub status_changed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RecommendationReport",
        from = "recommendation_report_id",
        to = "recommendation_report_id"
    )]
    pub recommendation_report: BelongsTo<quant_recommendation_report::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ReportRouteRun",
        from = "report_route_run_id",
        to = "report_route_run_id"
    )]
    pub report_route_run: BelongsTo<quant_report_route_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "PortfolioPlan",
        from = "portfolio_plan_id",
        to = "portfolio_plan_id"
    )]
    pub portfolio_plan: BelongsTo<quant_portfolio_plan::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<event::Entity>,
    #[sea_orm(has_many, relation_enum = "OrderIntent")]
    pub order_intent: HasMany<quant_order_intent::Entity>,
    #[sea_orm(has_one, relation_enum = "ResolutionOutcome")]
    pub resolution_outcome: HasOne<quant_recommendation_resolution_outcome::Entity>,
    #[sea_orm(has_one, relation_enum = "ExecutionRollup")]
    pub execution_rollup: HasOne<quant_recommendation_execution_rollup::Entity>,
    #[sea_orm(has_one, relation_enum = "ExecutionRollupReconciliationTask")]
    pub execution_rollup_reconciliation_task:
        HasOne<quant_execution_rollup_reconciliation_task::Entity>,
    #[sea_orm(has_one, relation_enum = "ResolutionOutcomeReconciliationTask")]
    pub resolution_outcome_reconciliation_task:
        HasOne<quant_resolution_outcome_reconciliation_task::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
