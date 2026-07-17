//! `quant_trade_policy_governance_audit` append-only entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus},
    types::{ContentHash, TradePolicyArtifactId, TradePolicyGovernanceAuditId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_trade_policy_governance_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub audit_id: TradePolicyGovernanceAuditId,
    pub artifact_id: TradePolicyArtifactId,
    pub action: TradePolicyGovernanceAction,
    pub from_status: TradePolicyStatus,
    pub to_status: TradePolicyStatus,
    pub content_hash: ContentHash,
    pub actor_id: Uuid,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
