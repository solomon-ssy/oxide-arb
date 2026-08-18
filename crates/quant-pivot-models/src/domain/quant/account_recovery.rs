//! Account recovery incident and chain-execution association contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::{quant_account_execution_association, quant_account_recovery_incident},
    enums::execution::{
        AccountExecutionAssociationKind, AccountRecoveryIncidentKind, AccountRecoveryIncidentStatus,
    },
    types::{
        AccountChainExecutionId, AccountRecoveryIncidentId, ContentHash, ExecutionAccountId,
        ExecutionOrderId, UserId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_recovery_incident::Entity")]
pub struct AccountRecoveryIncidentInfo {
    pub account_recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: AccountRecoveryIncidentKind,
    pub status: AccountRecoveryIncidentStatus,
    pub trigger_chain_execution_id: Option<AccountChainExecutionId>,
    pub reason: String,
    pub opened_at: DateTime<Utc>,
    pub seal_hash: Option<ContentHash>,
    pub sealed_by: Option<UserId>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    AccountRecoveryIncidentInfo,
    quant_account_recovery_incident::Model,
    {
        account_recovery_incident_id,
        execution_account_id,
        kind,
        status,
        trigger_chain_execution_id,
        reason,
        opened_at,
        seal_hash,
        sealed_by,
        sealed_at,
        revision,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_recovery_incident::ActiveModel")]
pub struct NewAccountRecoveryIncident {
    pub account_recovery_incident_id: AccountRecoveryIncidentId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: AccountRecoveryIncidentKind,
    pub status: AccountRecoveryIncidentStatus,
    pub trigger_chain_execution_id: Option<AccountChainExecutionId>,
    pub reason: String,
    pub opened_at: DateTime<Utc>,
    pub seal_hash: Option<ContentHash>,
    pub sealed_by: Option<UserId>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "quant_account_execution_association::Entity")]
pub struct AccountExecutionAssociationInfo {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub kind: AccountExecutionAssociationKind,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub evidence_hash: ContentHash,
    pub associated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountExecutionAssociationInfo,
    quant_account_execution_association::Model,
    {
        account_chain_execution_id,
        kind,
        execution_order_id,
        recovery_incident_id,
        evidence_hash,
        associated_at,
        created_at,
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_account_execution_association::ActiveModel")]
pub struct NewAccountExecutionAssociation {
    pub account_chain_execution_id: AccountChainExecutionId,
    pub kind: AccountExecutionAssociationKind,
    pub execution_order_id: Option<ExecutionOrderId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub evidence_hash: ContentHash,
    pub associated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountExecutionAssociationOutcome {
    pub association: AccountExecutionAssociationInfo,
    pub incident: Option<AccountRecoveryIncidentInfo>,
    pub incident_created: bool,
}
