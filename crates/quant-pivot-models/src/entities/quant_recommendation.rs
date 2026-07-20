//! `quant_recommendation` table entity.

use crate::{
    enums::quant::{OutcomeSide, RecommendationStatus},
    types::{
        Bps, EventId, EvidenceRefs, ExecutionEligibility, MarketContext, MarketId, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationIdentity,
        RecommendationReportId, RecommendationTradePlan, ResearchProfileArtifactId, TokenId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_recommendation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recommendation_id: RecommendationId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub recommendation_report_id: RecommendationReportId,
    pub rank: i32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub expected_return_bps: Bps,
    pub downside_bps: Bps,
    #[sea_orm(column_type = "JsonBinary")]
    pub identity: RecommendationIdentity,
    #[sea_orm(column_type = "JsonBinary")]
    pub market_context: MarketContext,
    pub rank_before_portfolio: i32,
    pub liquidity_score: Probability,
    pub data_quality_score: Probability,
    pub model_score_percentile: Probability,
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
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<super::research_profile_artifact::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RecommendationReport",
        from = "recommendation_report_id",
        to = "recommendation_report_id"
    )]
    pub recommendation_report: BelongsTo<super::quant_recommendation_report::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<super::event::Entity>,
    #[sea_orm(has_many, relation_enum = "OrderIntent")]
    pub order_intent: HasMany<super::quant_order_intent::Entity>,
    #[sea_orm(has_one, relation_enum = "Attribution")]
    pub attribution: HasOne<super::quant_recommendation_attribution::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
