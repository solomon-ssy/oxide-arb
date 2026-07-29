//! `quant_drift_report` append-only typed drift header.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use super::quant_feedback_cycle;
use crate::{
    enums::quant::{FeedbackDriftAssessment, FeedbackDriftKind, FeedbackDriftMetric},
    types::{ArtifactUri, ContentHash, DriftReportId, FeedbackCycleId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_drift_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub drift_report_id: DriftReportId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub kind: FeedbackDriftKind,
    pub metric: FeedbackDriftMetric,
    pub assessment: FeedbackDriftAssessment,
    pub baseline_window_start: DateTime<Utc>,
    pub baseline_window_end: DateTime<Utc>,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    #[sea_orm(column_type = "Decimal(Some((28, 12)))", nullable)]
    pub observed_value: Option<Decimal>,
    #[sea_orm(column_type = "Decimal(Some((28, 12)))")]
    pub threshold: Decimal,
    pub sample_count: i64,
    pub detail_uri: ArtifactUri,
    pub detail_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
