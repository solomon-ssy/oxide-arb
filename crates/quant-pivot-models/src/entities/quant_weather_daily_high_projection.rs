//! Current corrected airport-local-day high-temperature projection.

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, DomainEventId, DomainInstrumentKey, DomainSourceId};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_weather_daily_high_projection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: DomainSourceId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub instrument_key: DomainInstrumentKey,
    #[sea_orm(primary_key, auto_increment = false)]
    pub local_date: NaiveDate,
    pub station: String,
    pub timezone: String,
    pub current_high_celsius: Decimal,
    pub previous_high_celsius: Option<Decimal>,
    pub last_observation_time: DateTime<Utc>,
    pub last_report_hash: ContentHash,
    pub last_event_id: Option<DomainEventId>,
    pub revision: i64,
    pub day_closed: bool,
    pub gap_generation: i64,
    pub source_healthy: bool,
    pub available_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
