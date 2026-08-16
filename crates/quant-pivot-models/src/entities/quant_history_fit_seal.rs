//! Immutable finalized-history fit-window seal.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_exchange_history_plan;
use crate::types::{ContentHash, HistoryFitSealId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_history_fit_seal")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub fit_seal_id: HistoryFitSealId,
    #[sea_orm(unique)]
    pub seal_hash: ContentHash,
    pub plan_id: Uuid,
    pub window_from_block: i64,
    pub window_to_block: i64,
    pub policy_hash: ContentHash,
    pub profile_hash: ContentHash,
    pub cohort_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(belongs_to, from = "plan_id", to = "plan_id")]
    pub plan: BelongsTo<quant_exchange_history_plan::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
