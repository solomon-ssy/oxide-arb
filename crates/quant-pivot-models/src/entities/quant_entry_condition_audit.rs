//! Append-only entry-condition state/audit ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_entry_condition_instance;
use crate::{
    enums::quant::{EntryConditionAuditAction, EntryConditionState},
    types::{ConditionTruth, ContentHash, EntryConditionAuditId, EntryConditionInstanceId},
};

#[sea_orm::model]
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

    #[sea_orm(
        belongs_to,
        relation_enum = "Instance",
        from = "condition_instance_id",
        to = "condition_instance_id"
    )]
    pub instance: BelongsTo<quant_entry_condition_instance::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
