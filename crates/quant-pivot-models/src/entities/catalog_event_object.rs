//! Content-addressed normalized Gamma event objects.

use crate::types::{CatalogEventObjectId, ContentHash};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_event_object")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_object_id: CatalogEventObjectId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    pub created_at: DateTime<Utc>,
    #[sea_orm(has_many, relation_enum = "EventChange")]
    pub event_changes: HasMany<super::catalog_event_change::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
