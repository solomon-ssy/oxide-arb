//! `quant_calibration_artifact` table entity.

use crate::{
    enums::quant::CalibrationKind,
    types::{CalibrationArtifactId, ContentHash},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_calibration_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub artifact_id: CalibrationArtifactId,
    pub kind: CalibrationKind,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload_json: Json,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
