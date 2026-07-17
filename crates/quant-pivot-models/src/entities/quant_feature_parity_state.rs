//! Append-only `quant_feature_parity_state` latch ledger entity.

use crate::{
    enums::quant::{FeatureParityLatchState, FeatureParityStateTransition},
    types::{FeatureParityRunId, FeatureParityStateId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_parity_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub state_id: FeatureParityStateId,
    pub state: FeatureParityLatchState,
    pub transition: FeatureParityStateTransition,
    pub cause_run_id: Option<FeatureParityRunId>,
    pub recovery_run_id: Option<FeatureParityRunId>,
    pub previous_state_id: Option<FeatureParityStateId>,
    pub actor: Option<String>,
    pub acting_role: Option<String>,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
