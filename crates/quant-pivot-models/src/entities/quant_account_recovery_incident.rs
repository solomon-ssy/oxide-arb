//! Unified account recovery incident entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_account_chain_execution, quant_account_clean_funder_blocker,
    quant_account_pause_operation, quant_account_recovery_manifest, quant_execution_account,
};
use crate::{
    enums::execution::{AccountRecoveryIncidentKind, AccountRecoveryIncidentStatus},
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, ContentHash, ExecutionAccountId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_recovery_incident")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub account_recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: AccountRecoveryIncidentKind,
    pub status: AccountRecoveryIncidentStatus,
    pub trigger_chain_execution_id: Option<AccountChainExecutionId>,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub opened_at: DateTime<Utc>,
    pub seal_hash: Option<ContentHash>,
    pub sealed_by: Option<UserId>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TriggerChainExecution",
        from = "trigger_chain_execution_id",
        to = "account_chain_execution_id"
    )]
    pub trigger_chain_execution: BelongsTo<Option<quant_account_chain_execution::Entity>>,
    #[sea_orm(has_many, relation_enum = "PauseOperation")]
    pub pause_operation: HasMany<quant_account_pause_operation::Entity>,
    #[sea_orm(has_many, relation_enum = "RecoveryManifest")]
    pub recovery_manifest: HasMany<quant_account_recovery_manifest::Entity>,
    #[sea_orm(has_one, relation_enum = "CleanFunderBlocker")]
    pub clean_funder_blocker: HasOne<quant_account_clean_funder_blocker::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
