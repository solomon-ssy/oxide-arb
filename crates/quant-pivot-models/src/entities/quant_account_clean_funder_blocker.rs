//! Append-only clean-funder blocker for an unrecoverable external resting order.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_account_chain_execution, quant_account_recovery_incident};
use crate::{
    enums::execution::AccountChainExecutionRole,
    types::{AccountChainExecutionId, AccountRecoveryIncidentId, ContentHash},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_clean_funder_blocker")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub account_chain_execution_id: AccountChainExecutionId,
    pub role: AccountChainExecutionRole,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RecoveryIncident",
        from = "recovery_incident_id",
        to = "account_recovery_incident_id"
    )]
    pub recovery_incident: BelongsTo<quant_account_recovery_incident::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ChainExecution",
        from = "account_chain_execution_id",
        to = "account_chain_execution_id"
    )]
    pub chain_execution: BelongsTo<quant_account_chain_execution::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
