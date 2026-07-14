//! Append-only entry-condition state/audit ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{EntryConditionAuditAction, EntryConditionState},
    types::{ConditionTruth, ContentHash, EntryConditionAuditId, EntryConditionInstanceId},
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_entry_condition_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub audit_id: EntryConditionAuditId,
    pub condition_instance_id: EntryConditionInstanceId,
    pub revision: i64,
    pub action: EntryConditionAuditAction,
    pub from_state: Option<EntryConditionState>,
    pub to_state: EntryConditionState,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub truth_json: Option<ConditionTruth>,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub lease_epoch: i64,
    #[sea_orm(column_type = "Text", nullable)]
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_entry_condition_instance::Entity",
        from = "Column::ConditionInstanceId",
        to = "super::quant_entry_condition_instance::Column::ConditionInstanceId"
    )]
    Instance,
}

impl Related<super::quant_entry_condition_instance::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Instance.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
