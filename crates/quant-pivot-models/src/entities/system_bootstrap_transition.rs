//! Append-only bootstrap lifecycle transition audit.

use crate::{
    enums::system::BootstrapPhase,
    types::{BootstrapTransitionId, RuntimeConfigApprovalId, RuntimeConfigVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "system_bootstrap_transition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bootstrap_transition_id: BootstrapTransitionId,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
    pub from_phase: BootstrapPhase,
    pub to_phase: BootstrapPhase,
    pub runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub runtime_config_approval_id: Option<RuntimeConfigApprovalId>,
    #[sea_orm(column_type = "Text")]
    pub actor: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub acting_role: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub report_only_forced_ack: bool,
    pub occurred_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RuntimeConfigVersion",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub runtime_config_version: BelongsTo<Option<super::runtime_config_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RuntimeConfigApproval",
        from = "runtime_config_approval_id",
        to = "runtime_config_approval_id"
    )]
    pub runtime_config_approval: BelongsTo<Option<super::runtime_config_approval::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
