//! Durable derived state for one configured report schedule.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, RuntimeConfigVersionId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_schedule_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub schedule_id: String,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub last_materialized_for: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "RuntimeConfigVersion",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub runtime_config_version: BelongsTo<super::runtime_config_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
