//! Append-only calibration publication timeline.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::{enums::quant::CalibrationKind, types::CalibrationArtifactId};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_calibration_artifact_publication")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub publication_id: Uuid,
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub published_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_calibration_artifact::Entity",
        from = "Column::ArtifactId",
        to = "super::quant_calibration_artifact::Column::ArtifactId",
        on_delete = "Restrict",
        on_update = "Restrict"
    )]
    Artifact,
}

impl Related<super::quant_calibration_artifact::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Artifact.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
