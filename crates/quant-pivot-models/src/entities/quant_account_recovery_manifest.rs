//! Immutable account-recovery assessment manifest.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_account_recovery_incident;
use crate::{
    domain::quant::{
        AccountRecoveryAssessment, AccountRecoveryAssessmentInput, AccountRecoveryCreatedLots,
    },
    types::{AccountRecoveryIncidentId, AccountRecoveryManifestId, ContentHash, EvmBlockHash},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_account_recovery_manifest")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub account_recovery_manifest_id: AccountRecoveryManifestId,
    pub recovery_incident_id: AccountRecoveryIncidentId,
    pub attempt_no: i32,
    pub observed_at: DateTime<Utc>,
    pub finalized_block_number: i64,
    pub finalized_block_hash: EvmBlockHash,
    pub converged: bool,
    #[sea_orm(column_type = "JsonBinary")]
    pub input_json: AccountRecoveryAssessmentInput,
    #[sea_orm(column_type = "JsonBinary")]
    pub assessment_json: AccountRecoveryAssessment,
    #[sea_orm(column_type = "JsonBinary")]
    pub created_lots_json: AccountRecoveryCreatedLots,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RecoveryIncident",
        from = "recovery_incident_id",
        to = "account_recovery_incident_id"
    )]
    pub recovery_incident: BelongsTo<quant_account_recovery_incident::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
