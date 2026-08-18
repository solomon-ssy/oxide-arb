//! Append-only ownership association for account chain executions.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_account_chain_execution, quant_account_recovery_incident, quant_execution_order,
};
use crate::{
    enums::execution::AccountExecutionAssociationKind,
    types::{AccountChainExecutionId, AccountRecoveryIncidentId, ContentHash, ExecutionOrderId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_execution_association")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub account_chain_execution_id: AccountChainExecutionId,
    pub kind: AccountExecutionAssociationKind,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub evidence_hash: ContentHash,
    pub associated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ChainExecution",
        from = "account_chain_execution_id",
        to = "account_chain_execution_id"
    )]
    pub chain_execution: BelongsTo<quant_account_chain_execution::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionOrder",
        from = "execution_order_id",
        to = "execution_order_id"
    )]
    pub execution_order: BelongsTo<Option<quant_execution_order::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RecoveryIncident",
        from = "recovery_incident_id",
        to = "account_recovery_incident_id"
    )]
    pub recovery_incident: BelongsTo<Option<quant_account_recovery_incident::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
