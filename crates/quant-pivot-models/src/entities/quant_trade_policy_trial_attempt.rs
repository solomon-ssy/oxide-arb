//! Append-only trade-policy fit trial ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{TradePolicyTrialScope, TradePolicyTrialStatus},
    types::{
        ArtifactUri, ContentHash, ResearchJobId, TradePolicyTrialAttemptId, TradePolicyTrialMetrics,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_policy_trial_attempt")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub trial_attempt_id: TradePolicyTrialAttemptId,
    pub fit_job_id: ResearchJobId,
    pub attempt_ordinal: i64,
    pub experiment_family_hash: ContentHash,
    pub research_program_hash: ContentHash,
    pub candidate_id: String,
    pub candidate_hash: ContentHash,
    pub scope: TradePolicyTrialScope,
    pub fold_index: Option<i32>,
    pub path_index: Option<i32>,
    pub status: TradePolicyTrialStatus,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub metrics_json: Option<TradePolicyTrialMetrics>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub evidence_row_count: Option<i64>,
    pub failure_detail: Option<String>,
    pub row_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_research_job::Entity",
        from = "Column::FitJobId",
        to = "super::quant_research_job::Column::JobId"
    )]
    ResearchJob,
}

impl Related<super::quant_research_job::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ResearchJob.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
