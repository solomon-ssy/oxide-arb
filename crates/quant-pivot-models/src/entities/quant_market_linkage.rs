//! `quant_market_linkage` table entity.

use crate::{
    enums::domain::{DomainFamily, LinkageStatus, ResolverTier},
    types::{ContentHash, MarketId, MarketLinkageId, Probability, ResolverVersion},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_linkage")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub linkage_id: MarketLinkageId,
    pub market_id: MarketId,
    pub domain_family: DomainFamily,
    pub status: LinkageStatus,
    pub resolver_tier: ResolverTier,
    pub resolver_version: ResolverVersion,
    pub confidence: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub outcome: Json,
    pub metadata_hash: ContentHash,
    pub content_hash: ContentHash,
    pub derived_at: DateTime<Utc>,
    pub override_reason: Option<String>,
    pub override_actor: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
