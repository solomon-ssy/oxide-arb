//! `quant_portfolio_plan` table entity.

use crate::types::{ModelRunId, PortfolioPlanId, UniverseSnapshotId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_portfolio_plan")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub portfolio_plan_id: PortfolioPlanId,
    pub model_run_id: ModelRunId,
    pub universe_snapshot_id: UniverseSnapshotId,
    pub as_of: DateTime<Utc>,
    pub budget_usd: Usd,
    pub allocated_usd: Usd,
    #[sea_orm(column_type = "JsonBinary")]
    pub risk_budget_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub constraints_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub rejected_summary: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_run::Entity",
        from = "Column::ModelRunId",
        to = "super::quant_model_run::Column::ModelRunId"
    )]
    ModelRun,
    #[sea_orm(
        belongs_to = "super::quant_universe_snapshot::Entity",
        from = "Column::UniverseSnapshotId",
        to = "super::quant_universe_snapshot::Column::UniverseSnapshotId"
    )]
    UniverseSnapshot,
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
    }
}

impl Related<super::quant_universe_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UniverseSnapshot.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
