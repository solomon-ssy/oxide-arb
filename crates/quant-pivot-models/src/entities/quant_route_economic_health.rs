//! Immutable Route executable economic-health assessment.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::research_profile_artifact;
use crate::{
    domain::quant::RouteEconomicHealthEvidenceDocument,
    enums::quant::RouteEconomicHealthState,
    runtime_config::BuyModelRoute,
    types::{Bps, ContentHash, ResearchProfileArtifactId, RouteEconomicHealthId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_route_economic_health")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub route_economic_health_id: RouteEconomicHealthId,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub route_identity_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub feedback_policy_hash: ContentHash,
    pub state: RouteEconomicHealthState,
    pub window_start: Option<DateTime<Utc>>,
    pub assessed_through: DateTime<Utc>,
    pub due_observation_count: i64,
    pub usable_observation_count: i64,
    pub coverage: Decimal,
    pub effective_sample_size: Option<Decimal>,
    pub weighted_mean_return_bps: Option<Bps>,
    pub lower_confidence_return_bps: Option<Bps>,
    pub comparison_minimum_observations: i64,
    pub minimum_coverage: Decimal,
    pub minimum_effect_bps: Bps,
    pub confidence: Decimal,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence_json: RouteEconomicHealthEvidenceDocument,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
