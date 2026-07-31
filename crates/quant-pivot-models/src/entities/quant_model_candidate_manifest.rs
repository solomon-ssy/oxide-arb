//! `quant_model_candidate_manifest` WORM table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_feedback_cycle, quant_model_version};
use crate::{
    domain::quant::ModelCandidateManifestDocument,
    types::{ContentHash, FeedbackCycleId, ModelCandidateManifestId, ModelVersionId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_candidate_manifest")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: ModelCandidateManifestId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub promotion_gate_hash: ContentHash,
    pub manifest_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub document: ModelCandidateManifestDocument,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
