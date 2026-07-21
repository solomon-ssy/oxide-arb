//! `quant_domain_source_expectation` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    domain::data_plane::{AffectedMarketIds, AffectedProfileIds},
    enums::domain::{DomainFamily, DomainSourceExpectationStatus},
    types::{ContentHash, DomainInstrumentKey, DomainSourceExpectationId, DomainSourceId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_domain_source_expectation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub expectation_id: DomainSourceExpectationId,
    pub family: DomainFamily,
    #[sea_orm(unique_key = "source_instrument")]
    pub source_id: DomainSourceId,
    #[sea_orm(unique_key = "source_instrument")]
    pub instrument_key: DomainInstrumentKey,
    pub capability_registry_hash: ContentHash,
    pub binding_hash: ContentHash,
    pub required: bool,
    pub credential_required: bool,
    pub freshness_secs: i64,
    pub affected_market_ids: AffectedMarketIds,
    pub affected_profile_ids: AffectedProfileIds,
    pub status: DomainSourceExpectationStatus,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
