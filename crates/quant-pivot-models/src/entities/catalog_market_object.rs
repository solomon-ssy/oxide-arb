//! Content-addressed normalized Gamma market objects.

use crate::types::{CatalogMarketObjectId, ContentHash};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_market_object")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_object_id: CatalogMarketObjectId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "MarketChange")]
    pub market_change: HasMany<super::catalog_market_change::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
