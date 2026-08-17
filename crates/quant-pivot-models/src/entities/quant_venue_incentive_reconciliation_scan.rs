//! `quant_venue_incentive_reconciliation_scan` append-only scan manifest entity.

use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;

use super::quant_execution_account;
use crate::{
    enums::fee::{VenueIncentiveKind, VenueIncentiveReconciliationScanStatus, VenueIncentiveStage},
    types::{ContentHash, ExecutionAccountId, ids::VenueIncentiveReconciliationScanId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_venue_incentive_reconciliation_scan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub venue_incentive_reconciliation_scan_id: VenueIncentiveReconciliationScanId,
    pub execution_account_id: ExecutionAccountId,
    pub kind: VenueIncentiveKind,
    pub stage: VenueIncentiveStage,
    pub program_date: NaiveDate,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub status: VenueIncentiveReconciliationScanStatus,
    pub response_digest: Option<ContentHash>,
    pub response_count: i32,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
