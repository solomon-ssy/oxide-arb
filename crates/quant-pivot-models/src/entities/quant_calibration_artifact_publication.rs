//! Append-only calibration publication timeline.

use crate::{
    enums::quant::CalibrationKind,
    types::{CalibrationArtifactId, CalibrationArtifactPublicationId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_calibration_artifact_publication")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub publication_id: CalibrationArtifactPublicationId,
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub published_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Artifact",
        from = "artifact_id",
        to = "artifact_id"
    )]
    pub artifact: BelongsTo<super::quant_calibration_artifact::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
