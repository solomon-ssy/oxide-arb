//! `quant_model_spec` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_model_version;
use crate::{
    enums::model::ModelFamily,
    types::{
        ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract, RoleCode,
        SchemaVersion, UserId, model_spec::ModelSpecThesis,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_spec")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_spec_id: ModelSpecId,
    #[sea_orm(column_type = "Text", unique)]
    pub name: String,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    /// Fixed human-authored research thesis. Executable contracts remain in
    /// dedicated typed fields below.
    #[sea_orm(column_type = "JsonBinary")]
    pub thesis: ModelSpecThesis,
    /// Ordered raw features consumed by this model. Encoded columns are derived
    /// exclusively by the fitted input transform and cannot be persisted here.
    #[sea_orm(column_type = "JsonBinary")]
    pub input_contract: ModelInputContract,
    /// Frozen target and validation policy; train requests cannot override it.
    #[sea_orm(column_type = "JsonBinary")]
    pub training_contract: ModelTrainingContract,
    /// Domain-separated digest of every semantic field on this immutable row.
    pub definition_hash: ContentHash,
    /// Authenticated author identity; `NULL` is reserved for system bootstrapping.
    pub created_by_user_id: Option<UserId>,
    /// Human-readable actor snapshot retained even if the account is later renamed.
    pub created_by_label: String,
    pub created_by_role: Option<RoleCode>,
    /// Mandatory authoring rationale frozen with this WORM specification.
    pub reason: String,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "ModelVersion")]
    pub model_version: HasMany<quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
