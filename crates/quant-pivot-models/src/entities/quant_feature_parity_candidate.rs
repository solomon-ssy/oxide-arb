//! Frozen market membership for one parity subject.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_feature_parity_subject;
use crate::types::{ContentHash, FeatureParityCandidateId, FeatureParitySubjectId, MarketId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_parity_candidate")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub parity_candidate_id: FeatureParityCandidateId,
    pub parity_subject_id: FeatureParitySubjectId,
    pub market_id: MarketId,
    pub ordinal: i32,
    pub membership_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Subject",
        from = "parity_subject_id",
        to = "parity_subject_id"
    )]
    pub subject: BelongsTo<quant_feature_parity_subject::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
